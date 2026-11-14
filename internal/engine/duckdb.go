package engine

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net"
	"regexp"
	"strings"
	"sync"

	_ "github.com/duckdb/duckdb-go/v2"
	gomysql "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/zerodha/dungbeetle/v2/models"
)

var reSafeIdentifier = regexp.MustCompile(`^[a-zA-Z0-9_:-]+$`)

const duckLakeAdvisoryLockKey int64 = 84729104

// escapeSQLString escapes single quotes in SQL string literals.
func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// SourceConfig defines connection parameters for an attached source database.
type SourceConfig struct {
	Type string `koanf:"type"` // "postgres" or "mysql"
	DSN  string `koanf:"dsn"`
}

// DuckLakeConfig defines catalog and data storage parameters for DuckLake.
type DuckLakeConfig struct {
	Catalog     string `koanf:"catalog"`
	DataPath    string `koanf:"data_path"`
	S3Region    string `koanf:"s3_region"`
	S3Endpoint  string `koanf:"s3_endpoint"`
	S3AccessKey string `koanf:"s3_access_key"`
	S3SecretKey string `koanf:"s3_secret_key"`
	S3UseSSL    bool   `koanf:"s3_use_ssl"`
	S3URLStyle  string `koanf:"s3_url_style"`
}

// Config defines the DuckDB engine configuration.
type Config struct {
	PoolSize          int                     `koanf:"pool_size"`
	ServerOnly        bool                    `koanf:"-"`
	ThreadsPerJob     int                     `koanf:"threads_per_job"`
	MemoryLimitPerJob string                  `koanf:"memory_limit_per_job"`
	DuckLake          DuckLakeConfig          `koanf:"ducklake"`
	Sources           map[string]SourceConfig `koanf:"sources"`
}

// DuckDBEngine manages executing jobs against source databases with DuckLake outputs.
// It uses a Per-Slot Long-Lived DuckDB Instance pool combined with direct streaming `COPY ... TO` Parquet
// and a PostgreSQL Advisory Lock around `ducklake_add_data_files` to achieve zero temp-table memory bloat
// and zero snapshot concurrency collisions.
type DuckDBEngine struct {
	cfg           Config
	lo            *slog.Logger
	slotPool      chan *sql.DB
	catalogLockDB *sql.DB
	catalogMu     sync.Mutex
}

// Validate checks engine configuration at startup and fails fast if required parameters are missing.
func (cfg *Config) Validate() error {
	if cfg.DuckLake.Catalog == "" {
		return fmt.Errorf("ducklake.catalog is required")
	}
	if cfg.DuckLake.DataPath == "" {
		return fmt.Errorf("ducklake.data_path is required")
	}
	if strings.HasPrefix(cfg.DuckLake.DataPath, "s3://") {
		if cfg.DuckLake.S3Region == "" {
			return fmt.Errorf("ducklake.s3_region is required when using S3 data path ('%s')", cfg.DuckLake.DataPath)
		}
		if cfg.DuckLake.S3Endpoint == "" {
			return fmt.Errorf("ducklake.s3_endpoint is required when using S3 data path ('%s')", cfg.DuckLake.DataPath)
		}
		if (cfg.DuckLake.S3AccessKey != "" && cfg.DuckLake.S3SecretKey == "") ||
			(cfg.DuckLake.S3AccessKey == "" && cfg.DuckLake.S3SecretKey != "") {
			return fmt.Errorf("both ducklake.s3_access_key and ducklake.s3_secret_key must be configured together")
		}
		if cfg.DuckLake.S3URLStyle == "" {
			if !cfg.DuckLake.S3UseSSL || strings.Contains(cfg.DuckLake.S3Endpoint, "127.0.0.1") || strings.Contains(cfg.DuckLake.S3Endpoint, "localhost") {
				cfg.DuckLake.S3URLStyle = "path"
			} else {
				cfg.DuckLake.S3URLStyle = "vhost"
			}
		}
	}
	return nil
}

// extractPostgresCatalogDSN extracts a lib/pq compatible DSN if the DuckLake catalog is backed by PostgreSQL.
func extractPostgresCatalogDSN(catalog string) string {
	if strings.HasPrefix(catalog, "ducklake:postgres://") || strings.HasPrefix(catalog, "ducklake:postgresql://") {
		return strings.TrimPrefix(catalog, "ducklake:")
	}
	if strings.HasPrefix(catalog, "ducklake:postgres:") {
		dsn := strings.TrimPrefix(catalog, "ducklake:postgres:")
		if !strings.Contains(dsn, "sslmode=") {
			dsn += " sslmode=disable"
		}
		return strings.TrimSpace(dsn)
	}
	return ""
}

// New creates a new DuckDBEngine instance with safe defaults and validates configuration.
// It initializes dedicated long-lived DuckDB slot instances and a PostgreSQL advisory lock connection pool.
func New(cfg Config, lo *slog.Logger) (*DuckDBEngine, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.ThreadsPerJob <= 0 {
		cfg.ThreadsPerJob = 2
	}
	if cfg.MemoryLimitPerJob == "" {
		cfg.MemoryLimitPerJob = "2GB"
	}
	if cfg.PoolSize <= 0 {
		cfg.PoolSize = 2
	}
	if lo == nil {
		lo = slog.Default()
	}

	eng := &DuckDBEngine{
		cfg: cfg,
		lo:  lo,
	}

	// In server-only mode (API server), no DuckDB worker slots are needed.
	if cfg.ServerOnly {
		return eng, nil
	}

	// Initialize PostgreSQL catalog advisory lock connection if backed by Postgres.
	if pgDSN := extractPostgresCatalogDSN(cfg.DuckLake.Catalog); pgDSN != "" {
		lockDB, err := sql.Open("postgres", pgDSN)
		if err != nil {
			return nil, fmt.Errorf("failed to open postgres catalog lock connection: %w", err)
		}
		lockDB.SetMaxOpenConns(2)
		lockDB.SetMaxIdleConns(2)
		eng.catalogLockDB = lockDB
	}

	eng.slotPool = make(chan *sql.DB, cfg.PoolSize)
	ctx := context.Background()

	for i := 0; i < cfg.PoolSize; i++ {
		db, err := eng.createInitializedSlotDB(ctx)
		if err != nil {
			eng.Close()
			return nil, fmt.Errorf("failed to initialize duckdb slot %d/%d: %w", i+1, cfg.PoolSize, err)
		}
		eng.slotPool <- db
	}

	lo.Info("initialized dedicated duckdb worker slots", "slots", cfg.PoolSize, "threads_per_slot", cfg.ThreadsPerJob, "memory_per_slot", cfg.MemoryLimitPerJob)
	return eng, nil
}

// createInitializedSlotDB creates a long-lived, isolated DuckDB instance for a single worker concurrency slot.
func (e *DuckDBEngine) createInitializedSlotDB(ctx context.Context) (*sql.DB, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to open duckdb slot instance: %w", err)
	}
	// Pin to 1 connection per slot instance so session state (USE <db>) stays strictly bound.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	// 1. Resource limits
	if _, err := db.ExecContext(ctx, fmt.Sprintf("PRAGMA threads=%d;", e.cfg.ThreadsPerJob)); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to set thread limit: %w", err)
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("PRAGMA memory_limit='%s';", escapeSQLString(e.cfg.MemoryLimitPerJob))); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to set memory limit: %w", err)
	}

	// 2. Configure S3 Secret (only if using S3)
	if strings.HasPrefix(e.cfg.DuckLake.DataPath, "s3://") {
		authClause := "PROVIDER CREDENTIAL_CHAIN"
		if e.cfg.DuckLake.S3AccessKey != "" {
			authClause = fmt.Sprintf("KEY_ID '%s', SECRET '%s'",
				escapeSQLString(e.cfg.DuckLake.S3AccessKey),
				escapeSQLString(e.cfg.DuckLake.S3SecretKey),
			)
		}
		secretSQL := fmt.Sprintf(`CREATE SECRET s3_creds (
			TYPE S3,
			REGION '%s',
			ENDPOINT '%s',
			URL_STYLE '%s',
			USE_SSL %t,
			%s
		);`,
			escapeSQLString(e.cfg.DuckLake.S3Region),
			escapeSQLString(e.cfg.DuckLake.S3Endpoint),
			escapeSQLString(e.cfg.DuckLake.S3URLStyle),
			e.cfg.DuckLake.S3UseSSL,
			authClause,
		)
		if _, err := db.ExecContext(ctx, secretSQL); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("failed to configure s3 secret: %w", err)
		}
	}

	// 3. Attach DuckLake under catalog lock so concurrent containers do not race when bootstrapping a fresh catalog DB.
	attachErr := func() error {
		unlock, err := e.acquireCatalogLock(ctx)
		if err != nil {
			return err
		}
		defer unlock()

		attachLake := fmt.Sprintf(
			"ATTACH '%s' AS lake (DATA_PATH '%s', DATA_INLINING_ROW_LIMIT 0);",
			escapeSQLString(e.cfg.DuckLake.Catalog),
			escapeSQLString(e.cfg.DuckLake.DataPath),
		)
		_, err = db.ExecContext(ctx, attachLake)
		return err
	}()
	if attachErr != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to attach ducklake: %w", attachErr)
	}

	// 4. Attach all configured sources ahead of time and eagerly prime schema metadata
	for name, src := range e.cfg.Sources {
		var attachSrc string
		switch strings.ToLower(src.Type) {
		case "postgres":
			attachSrc = fmt.Sprintf("ATTACH '%s' AS %s (TYPE POSTGRES, READ_ONLY);", escapeSQLString(src.DSN), name)
		case "mysql":
			dsn := src.DSN
			if strings.Contains(dsn, "@tcp(") {
				if parsed, err := gomysql.ParseDSN(dsn); err == nil {
					host, port, _ := net.SplitHostPort(parsed.Addr)
					if host == "" {
						host = parsed.Addr
					}
					if port == "" {
						port = "3306"
					}
					dsn = fmt.Sprintf("host=%s port=%s user=%s password=%s database=%s", host, port, parsed.User, parsed.Passwd, parsed.DBName)
				}
			}
			attachSrc = fmt.Sprintf("ATTACH '%s' AS %s (TYPE MYSQL, READ_ONLY);", escapeSQLString(dsn), name)
		case "duckdb", "":
			attachSrc = fmt.Sprintf("ATTACH '%s' AS %s;", escapeSQLString(src.DSN), name)
		default:
			_ = db.Close()
			return nil, fmt.Errorf("unsupported source type '%s'", src.Type)
		}
		if _, err := db.ExecContext(ctx, attachSrc); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("failed to attach source '%s': %w", name, err)
		}

		// Eagerly prime schema definitions in the slot's catalog cache
		_, _ = db.ExecContext(ctx, fmt.Sprintf("USE %s;", name))
	}

	return db, nil
}

// acquireCatalogLock acquires both the in-process mutex and the cross-container PostgreSQL transaction advisory lock.
func (e *DuckDBEngine) acquireCatalogLock(ctx context.Context) (func(), error) {
	e.catalogMu.Lock()
	if e.catalogLockDB == nil {
		return func() {
			e.catalogMu.Unlock()
		}, nil
	}

	tx, err := e.catalogLockDB.BeginTx(ctx, nil)
	if err != nil {
		e.catalogMu.Unlock()
		return nil, fmt.Errorf("failed to begin catalog lock transaction: %w", err)
	}

	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock($1)", duckLakeAdvisoryLockKey); err != nil {
		_ = tx.Rollback()
		e.catalogMu.Unlock()
		return nil, fmt.Errorf("failed to acquire postgres catalog advisory lock: %w", err)
	}

	return func() {
		_ = tx.Commit()
		e.catalogMu.Unlock()
	}, nil
}

// Close gracefully closes all worker slot DuckDB instances and the catalog lock connection.
func (e *DuckDBEngine) Close() {
	if e.slotPool != nil {
	drainLoop:
		for {
			select {
			case db := <-e.slotPool:
				_ = db.Close()
			default:
				break drainLoop
			}
		}
	}
	if e.catalogLockDB != nil {
		_ = e.catalogLockDB.Close()
	}
}

// GetCatalog returns the DuckLake catalog connection string.
func (e *DuckDBEngine) GetCatalog() string {
	return e.cfg.DuckLake.Catalog
}

// GetSources returns the registered source database names.
func (e *DuckDBEngine) GetSources() map[string]SourceConfig {
	return e.cfg.Sources
}

// ExecuteJob runs a query using Option C (Direct Streaming COPY TO S3 + Locked DuckLake Registration):
// Phase 1 (Parallel, 100% Streaming, Zero TEMP TABLE): `COPY (<query>) TO 's3://.../results_<id>.parquet'`
// streams directly from the source DB to Parquet storage without touching DuckLake or buffering in worker memory/disk.
// Phase 2 (Cluster-Serialized via PostgreSQL Advisory Lock, ~3-5ms): Under `pg_advisory_xact_lock`,
// creates the table schema in `lake` and registers the already-written Parquet file via `ducklake_add_data_files`.
func (e *DuckDBEngine) ExecuteJob(ctx context.Context, jobID, sourceDB, query string, args ...interface{}) (models.JobResultPayload, error) {
	if e.cfg.ServerOnly || e.slotPool == nil {
		return models.JobResultPayload{}, fmt.Errorf("cannot execute job: engine is running in server-only mode")
	}
	if !reSafeIdentifier.MatchString(jobID) {
		return models.JobResultPayload{}, fmt.Errorf("invalid characters in job_id: %s", jobID)
	}
	if sourceDB == "" {
		return models.JobResultPayload{}, fmt.Errorf("source database must be specified")
	}
	if _, ok := e.cfg.Sources[sourceDB]; !ok {
		return models.JobResultPayload{}, fmt.Errorf("unknown source db '%s'", sourceDB)
	}

	// Checkout a dedicated, pre-warmed long-lived slot instance
	var db *sql.DB
	select {
	case db = <-e.slotPool:
	case <-ctx.Done():
		return models.JobResultPayload{}, ctx.Err()
	}

	slotHealthy := true
	defer func() {
		if !slotHealthy {
			_ = db.Close()
			if freshDB, err := e.createInitializedSlotDB(context.Background()); err == nil {
				e.slotPool <- freshDB
				return
			}
			// Fallback to an uninitialized handle if replacement fails so slotPool never deadlocks
			if fallbackDB, err := sql.Open("duckdb", ""); err == nil {
				e.slotPool <- fallbackDB
				return
			}
		}
		e.slotPool <- db
	}()

	// 1. Switch active context to sourceDB so unqualified table names resolve directly
	if _, err := db.ExecContext(ctx, fmt.Sprintf("USE %s;", sourceDB)); err != nil {
		slotHealthy = false
		return models.JobResultPayload{}, fmt.Errorf("failed to switch context to source '%s': %w", sourceDB, err)
	}

	tableName := fmt.Sprintf("results_%s", jobID)
	targetTable := fmt.Sprintf(`lake."%s"`, tableName)
	dataRoot := strings.TrimRight(e.cfg.DuckLake.DataPath, "/")
	parquetFile := fmt.Sprintf("%s/%s.parquet", dataRoot, tableName)
	escapedParquetFile := escapeSQLString(parquetFile)
	cleanedQuery := strings.TrimRight(strings.TrimSpace(query), ";")

	// Phase 1: Stream directly from Source DB to Parquet file on S3/disk (Zero TEMP TABLE, Zero DuckLake lock)
	var count int64
	copySQL := fmt.Sprintf("COPY (%s) TO '%s' (FORMAT PARQUET)", cleanedQuery, escapedParquetFile)
	if err := db.QueryRowContext(ctx, copySQL, args...).Scan(&count); err != nil {
		if _, pingErr := db.ExecContext(context.Background(), "SELECT 1;"); pingErr != nil {
			slotHealthy = false
		}
		return models.JobResultPayload{}, fmt.Errorf("query execution failed: %w", err)
	}

	// Phase 2: Cluster-Serialized DuckLake Catalog Registration (~3-5ms)
	var files []string
	var totalBytes int64

	commitErr := func() error {
		unlock, err := e.acquireCatalogLock(ctx)
		if err != nil {
			return err
		}
		defer unlock()

		// Drop any prior table on retry, create empty table with schema inferred from the Parquet footer,
		// and register the file in a single atomic DuckLake transaction.
		registerSQL := fmt.Sprintf(
			"BEGIN TRANSACTION; DROP TABLE IF EXISTS %s; CREATE TABLE %s AS SELECT * FROM read_parquet('%s') WITH NO DATA; CALL ducklake_add_data_files('lake', '%s', '%s'); COMMIT;",
			targetTable, targetTable, escapedParquetFile, escapeSQLString(tableName), escapedParquetFile,
		)
		if _, err := db.ExecContext(ctx, registerSQL); err != nil {
			if _, rbErr := db.ExecContext(context.Background(), "ROLLBACK;"); rbErr != nil {
				slotHealthy = false
			}
			return fmt.Errorf("ducklake catalog registration failed: %w", err)
		}

		fileRows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT data_file, data_file_size_bytes FROM ducklake_list_files('lake', '%s')", escapeSQLString(tableName)))
		if err != nil {
			return fmt.Errorf("failed to list ducklake files for '%s': %w", tableName, err)
		}
		defer fileRows.Close()

		for fileRows.Next() {
			var f string
			var s int64
			if err := fileRows.Scan(&f, &s); err != nil {
				return fmt.Errorf("failed to scan ducklake file metadata: %w", err)
			}
			files = append(files, f)
			totalBytes += s
		}
		if err := fileRows.Err(); err != nil {
			return fmt.Errorf("error iterating ducklake file metadata: %w", err)
		}
		return nil
	}()
	if commitErr != nil {
		return models.JobResultPayload{}, commitErr
	}

	return models.JobResultPayload{
		Count:     count,
		Table:     tableName,
		Catalog:   e.cfg.DuckLake.Catalog,
		Files:     files,
		SizeBytes: totalBytes,
	}, nil
}
