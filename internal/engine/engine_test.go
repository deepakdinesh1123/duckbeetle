package engine

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"
)

func TestUseAndCreateLakeTable(t *testing.T) {
	tmpDir := t.TempDir()
	catalogFile := filepath.Join(tmpDir, "catalog.ducklake")
	dataPath := filepath.Join(tmpDir, "data") + "/"
	require.NoError(t, os.MkdirAll(dataPath, 0755))

	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()

	// 1. Attach DuckLake with DATA_INLINING_ROW_LIMIT 0 to ensure Parquet files are always created
	_, err = db.ExecContext(ctx, "ATTACH 'ducklake:"+catalogFile+"' AS lake (DATA_PATH '"+dataPath+"', DATA_INLINING_ROW_LIMIT 0)")
	require.NoError(t, err)

	// 2. Attach Source DB
	_, err = db.ExecContext(ctx, "ATTACH ':memory:' AS my_source")
	require.NoError(t, err)

	// Populate Source DB
	_, err = db.ExecContext(ctx, "CREATE TABLE my_source.orders (id INT, customer VARCHAR, total DOUBLE)")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "INSERT INTO my_source.orders VALUES (1, 'Alice', 99.5), (2, 'Bob', 140.0)")
	require.NoError(t, err)

	// 3. Switch context to Source DB
	_, err = db.ExecContext(ctx, "USE my_source")
	require.NoError(t, err)

	// 4. Execute CTAS into lake with prepared statement parameter!
	_, err = db.ExecContext(ctx, "CREATE TABLE lake.results_job123 AS SELECT * FROM orders WHERE total > ?", 100.0)
	require.NoError(t, err)

	// 5. Verify the data in DuckLake
	var customer string
	var total float64
	err = db.QueryRowContext(ctx, "SELECT customer, total FROM lake.results_job123").Scan(&customer, &total)
	require.NoError(t, err)
	require.Equal(t, "Bob", customer)
	require.Equal(t, 140.0, total)

	// 6. Verify ducklake_list_files
	var file string
	var size int64
	err = db.QueryRowContext(ctx, "SELECT data_file, data_file_size_bytes FROM ducklake_list_files('lake', 'results_job123')").Scan(&file, &size)
	require.NoError(t, err)
	require.NotEmpty(t, file)
	require.Greater(t, size, int64(0))

	t.Logf("Success! Parquet file generated: %s (%d bytes)", file, size)
}

func TestDuckDBEngineExecuteJob(t *testing.T) {
	tmpDir := t.TempDir()
	catalogFile := filepath.Join(tmpDir, "catalog.ducklake")
	dataPath := filepath.Join(tmpDir, "data") + "/"
	sourceFile := filepath.Join(tmpDir, "source.duckdb")
	require.NoError(t, os.MkdirAll(dataPath, 0755))

	// Pre-create the source database file with some tables
	sdb, err := sql.Open("duckdb", sourceFile)
	require.NoError(t, err)
	_, err = sdb.Exec("CREATE TABLE entries (id INT, amount DOUBLE, user_id VARCHAR);")
	require.NoError(t, err)
	_, err = sdb.Exec("INSERT INTO entries VALUES (1, 150.0, 'USR1'), (2, 250.0, 'USR2'), (3, 50.0, 'USR1');")
	require.NoError(t, err)
	sdb.Close()

	// Initialize DuckDBEngine
	cfg := Config{
		DuckLake: DuckLakeConfig{
			Catalog:  "ducklake:" + catalogFile,
			DataPath: dataPath,
		},
		Sources: map[string]SourceConfig{
			"orders_db": {
				Type: "duckdb",
				DSN:  sourceFile,
			},
		},
	}

	eng, err := New(cfg, nil)
	require.NoError(t, err)
	defer eng.Close()
	require.Equal(t, "ducklake:"+catalogFile, eng.GetCatalog())

	ctx := context.Background()
	for i := 1; i <= 6; i++ {
		jobID := "job99" + string(rune('0'+i))
		res, err := eng.ExecuteJob(ctx, jobID, "orders_db", "SELECT * FROM entries WHERE user_id = ?", "USR1")
		require.NoError(t, err, "failed on iteration %d (%s)", i, jobID)
		require.Equal(t, int64(2), res.Count)
		require.NotEmpty(t, res.Files)
		require.Greater(t, res.SizeBytes, int64(0))
		t.Logf("Slot iteration %d verified! Results: %d rows, File: %v (%d bytes)", i, res.Count, res.Files, res.SizeBytes)
	}

	// Verify idempotent retry with duplicate jobID
	retryRes, err := eng.ExecuteJob(ctx, "job991", "orders_db", "SELECT * FROM entries WHERE user_id = ?", "USR2")
	require.NoError(t, err)
	require.Equal(t, int64(1), retryRes.Count)

	// Verify slot self-healing after a failed query
	_, err = eng.ExecuteJob(ctx, "job_bad", "orders_db", "SELECT * FROM non_existent_table")
	require.Error(t, err)

	// Verify subsequent jobs on healed slot still succeed
	emptyRes, err := eng.ExecuteJob(ctx, "job_empty", "orders_db", "SELECT * FROM entries WHERE user_id = ?", "NON_EXISTENT")
	require.NoError(t, err)
	require.Equal(t, int64(0), emptyRes.Count)
}

func TestServerOnlyGuard(t *testing.T) {
	cfg := Config{
		ServerOnly: true,
		DuckLake: DuckLakeConfig{
			Catalog:  "ducklake:test.db",
			DataPath: "/tmp/data/",
		},
		Sources: map[string]SourceConfig{
			"db1": {Type: "postgres", DSN: "host=localhost"},
		},
	}
	eng, err := New(cfg, nil)
	require.NoError(t, err)
	defer eng.Close()

	_, err = eng.ExecuteJob(context.Background(), "job1", "db1", "SELECT 1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "server-only mode")
}

func TestDuckLakeS3ConfigValidation(t *testing.T) {
	cfg := Config{
		DuckLake: DuckLakeConfig{
			Catalog:  "ducklake:test.db",
			DataPath: "s3://my-bucket/data/",
		},
		Sources: map[string]SourceConfig{
			"db1": {Type: "postgres", DSN: "host=localhost"},
		},
	}
	_, err := New(cfg, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ducklake.s3_region is required")

	cfg.DuckLake.S3Region = "us-east-1"
	_, err = New(cfg, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ducklake.s3_endpoint is required")

	cfg.DuckLake.S3Endpoint = "minio:9000"
	cfg.DuckLake.S3AccessKey = "key_only"
	_, err = New(cfg, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "must be configured together")

	// Verify valid S3 config initializes CREATE SECRET + enable_http_metadata_cache cleanly
	tempDir := t.TempDir()
	cfg.DuckLake.Catalog = "ducklake:" + filepath.Join(tempDir, "s3_catalog.db")
	cfg.DuckLake.S3SecretKey = "secret_key"
	cfg.Sources = nil
	eng, err := New(cfg, nil)
	require.NoError(t, err)
	eng.Close()
}

func TestEngineCloseIdempotentAndRejectsJobs(t *testing.T) {
	tempDir := t.TempDir()
	sourceFile := filepath.Join(tempDir, "source.duckdb")
	catalogFile := filepath.Join(tempDir, "catalog.duckdb")
	dataPath := filepath.Join(tempDir, "data")
	require.NoError(t, os.MkdirAll(dataPath, 0755))

	sdb, err := sql.Open("duckdb", sourceFile)
	require.NoError(t, err)
	_, err = sdb.Exec("CREATE TABLE t1 (id INT); INSERT INTO t1 VALUES (1);")
	require.NoError(t, err)
	sdb.Close()

	cfg := Config{
		DuckLake: DuckLakeConfig{
			Catalog:  "ducklake:" + catalogFile,
			DataPath: dataPath,
		},
		Sources: map[string]SourceConfig{
			"db1": {Type: "duckdb", DSN: sourceFile},
		},
	}
	eng, err := New(cfg, nil)
	require.NoError(t, err)

	// Close engine
	eng.Close()
	// Idempotent second close must not panic
	eng.Close()

	// Subsequent job must fail immediately
	_, err = eng.ExecuteJob(context.Background(), "job_after_close", "db1", "SELECT * FROM t1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "engine is closed")
}
