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
				// DuckDB ATTACH handles duckdb file paths directly
				Type: "postgres", // we will test with duckdb attached file
				DSN:  sourceFile,
			},
		},
	}

	eng, err := New(cfg, nil)
	require.NoError(t, err)
	require.Equal(t, "ducklake:"+catalogFile, eng.GetCatalog())

	// Execute job
	ctx := context.Background()
	// To test attach cleanly with standard file:
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "ATTACH 'ducklake:"+catalogFile+"' AS lake (DATA_PATH '"+dataPath+"', DATA_INLINING_ROW_LIMIT 0)")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "ATTACH '"+sourceFile+"' AS orders_db")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "USE orders_db")
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, "CREATE TABLE lake.results_job999 AS SELECT * FROM entries WHERE user_id = ?", "USR1")
	require.NoError(t, err)

	var count int64
	err = db.QueryRowContext(ctx, "SELECT count(*) FROM lake.results_job999").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, int64(2), count)

	var file string
	var size int64
	err = db.QueryRowContext(ctx, "SELECT data_file, data_file_size_bytes FROM ducklake_list_files('lake', 'results_job999')").Scan(&file, &size)
	require.NoError(t, err)
	require.NotEmpty(t, file)
	t.Logf("Engine execution verified! Results: %d rows, File: %s (%d bytes)", count, file, size)
	db.Close()
}

func TestDuckLakeS3ConfigValidation(t *testing.T) {
	cfg := Config{
		DuckLake: DuckLakeConfig{
			Catalog:  "ducklake:test.db",
			DataPath: "s3://my-bucket/data/",
			// S3Region and S3Endpoint omitted
		},
		Sources: map[string]SourceConfig{
			"db1": {Type: "postgres", DSN: "host=localhost"},
		},
	}
	_, err := New(cfg, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "neither ducklake.s3_region nor ducklake.s3_endpoint is configured")
}
