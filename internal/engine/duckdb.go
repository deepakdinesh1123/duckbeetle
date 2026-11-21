package engine

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net"
	"os"
	"regexp"
	"strings"
	"sync"

	_ "github.com/duckdb/duckdb-go/v2"
	gomysql "github.com/go-sql-driver/mysql"
	"github.com/zerodha/dungbeetle/v2/models"
)

var reSafeIdentifier = regexp.MustCompile(`^[a-zA-Z0-9_:-]+$`)

// escapeSQLString escapes single quotes in SQL string literals.
func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// SourceConfig defines connection parameters for an attached source database.
type SourceConfig struct {
	Type string `koanf:"type"` // "postgres" or "mysql"
	DSN  string `koanf:"dsn"`
}

// DuckLakeConfig defines data storage parameters for Parquet output.
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

// Slot represents a dedicated worker concurrency slot with its own isolated, long-lived DuckDB instance.
// Concurrency is managed via Slot tokens in slotPool, keeping connection health decoupled from semaphore tokens.
type Slot struct {
	id uint32
	db *sql.DB
	mu sync.Mutex
}

// getOrConnect returns the slot's DuckDB instance, lazily reconnecting if currently nil or invalidated.
func (s *Slot) getOrConnect(ctx context.Context, e *DuckDBEngine) (*sql.DB, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.db != nil {
		return s.db, nil
	}

	db, err := e.createInitializedSlotDB(ctx)
	if err != nil {
		return nil, fmt.Errorf("slot %d: failed to initialize duckdb instance: %w", s.id, err)
	}
	s.db = db
	return s.db, nil
}

// invalidate closes and resets the slot's DuckDB instance when a fatal connection error occurs.
func (s *Slot) invalidate() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.db != nil {
		_ = s.db.Close()
		s.db = nil
	}
}

// close cleanly closes the slot's DuckDB instance during engine shutdown.
func (s *Slot) close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.db != nil {
		_ = s.db.Close()
		s.db = nil
	}
}

// DuckDBEngine manages executing jobs against source databases and streaming Parquet outputs directly to S3/disk.
// It uses a Per-Slot Long-Lived DuckDB Instance pool with direct streaming `COPY ... TO` Parquet,
// requiring zero catalog databases, zero locks, and zero temp-table memory bloat.
type DuckDBEngine struct {
	cfg      Config
	lo       *slog.Logger
	slots    []*Slot
	slotsMu  sync.Mutex
	slotPool chan *Slot
	closed   bool
}

// Validate checks engine configuration at startup and fails fast if required parameters are missing.
func (cfg *Config) Validate() error {
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

// New creates a new DuckDBEngine instance with safe defaults and validates configuration.
// It initializes dedicated long-lived DuckDB slot instances.
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

	eng.slots = make([]*Slot, 0, cfg.PoolSize)
	eng.slotPool = make(chan *Slot, cfg.PoolSize)
	ctx := context.Background()

	for i := 0; i < cfg.PoolSize; i++ {
		slot := &Slot{id: uint32(i + 1)}
		if _, err := slot.getOrConnect(ctx, eng); err != nil {
			eng.Close()
			return nil, fmt.Errorf("failed to initialize duckdb slot %d/%d: %w", i+1, cfg.PoolSize, err)
		}
		eng.slots = append(eng.slots, slot)
		eng.slotPool <- slot
	}

	lo.Info("initialized dedicated duckdb worker slots (catalog-free S3 streaming)", "slots", cfg.PoolSize, "threads_per_slot", cfg.ThreadsPerJob, "memory_per_slot", cfg.MemoryLimitPerJob)
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
		if _, err := db.ExecContext(ctx, "SET enable_http_metadata_cache = true;"); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("failed to enable http metadata cache: %w", err)
		}
	}

	// 3. Attach all configured sources ahead of time and eagerly prime schema metadata
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

// Close gracefully closes all worker slot DuckDB instances.
func (e *DuckDBEngine) Close() {
	e.slotsMu.Lock()
	if e.closed {
		e.slotsMu.Unlock()
		return
	}
	e.closed = true

	// Drain any idle slot tokens from the pool channel so no future borrows succeed
	if e.slotPool != nil {
	drainLoop:
		for {
			select {
			case <-e.slotPool:
			default:
				break drainLoop
			}
		}
	}

	// Close all slots directly from the tracked slice
	for _, slot := range e.slots {
		if slot != nil {
			slot.close()
		}
	}
	e.slots = nil
	e.slotsMu.Unlock()
}

// GetCatalog returns the configured catalog string (if any).
func (e *DuckDBEngine) GetCatalog() string {
	return e.cfg.DuckLake.Catalog
}

// GetSources returns the registered source database names.
func (e *DuckDBEngine) GetSources() map[string]SourceConfig {
	return e.cfg.Sources
}

// ExecuteJob streams query results directly from the attached source DB to a Parquet file on S3/disk
// (`COPY (<query>) TO 's3://.../results_<id>.parquet' (FORMAT PARQUET)`) with zero catalog overhead and zero locks.
func (e *DuckDBEngine) ExecuteJob(ctx context.Context, jobID, sourceDB, query string, args ...interface{}) (models.JobResultPayload, error) {
	if e.cfg.ServerOnly || e.slotPool == nil {
		return models.JobResultPayload{}, fmt.Errorf("cannot execute job: engine is running in server-only mode")
	}
	e.slotsMu.Lock()
	if e.closed {
		e.slotsMu.Unlock()
		return models.JobResultPayload{}, fmt.Errorf("cannot execute job: engine is closed")
	}
	e.slotsMu.Unlock()

	if !reSafeIdentifier.MatchString(jobID) {
		return models.JobResultPayload{}, fmt.Errorf("invalid characters in job_id: %s", jobID)
	}
	if sourceDB == "" {
		return models.JobResultPayload{}, fmt.Errorf("source database must be specified")
	}
	if _, ok := e.cfg.Sources[sourceDB]; !ok {
		return models.JobResultPayload{}, fmt.Errorf("unknown source db '%s'", sourceDB)
	}

	// Checkout a dedicated, pre-warmed long-lived slot token
	var slot *Slot
	select {
	case slot = <-e.slotPool:
	case <-ctx.Done():
		return models.JobResultPayload{}, ctx.Err()
	}

	slotHealthy := true
	defer func() {
		if !slotHealthy {
			slot.invalidate()
		}

		e.slotsMu.Lock()
		defer e.slotsMu.Unlock()

		if e.closed {
			slot.close()
			return
		}
		e.slotPool <- slot
	}()

	db, err := slot.getOrConnect(ctx, e)
	if err != nil {
		slotHealthy = false
		return models.JobResultPayload{}, fmt.Errorf("failed to obtain slot connection: %w", err)
	}

	// 1. Switch active context to sourceDB so unqualified table names resolve directly
	if _, err := db.ExecContext(ctx, fmt.Sprintf("USE %s;", sourceDB)); err != nil {
		slotHealthy = false
		return models.JobResultPayload{}, fmt.Errorf("failed to switch context to source '%s': %w", sourceDB, err)
	}

	tableName := fmt.Sprintf("results_%s", jobID)
	dataRoot := strings.TrimRight(e.cfg.DuckLake.DataPath, "/")
	parquetFile := fmt.Sprintf("%s/%s.parquet", dataRoot, tableName)
	escapedParquetFile := escapeSQLString(parquetFile)
	cleanedQuery := strings.TrimRight(strings.TrimSpace(query), ";")

	// 2. Stream directly from Source DB to Parquet file on S3/disk (Zero TEMP TABLE, Zero Catalog Lock)
	var count int64
	copySQL := fmt.Sprintf("COPY (%s) TO '%s' (FORMAT PARQUET)", cleanedQuery, escapedParquetFile)
	if err := db.QueryRowContext(ctx, copySQL, args...).Scan(&count); err != nil {
		if _, pingErr := db.ExecContext(context.Background(), "SELECT 1;"); pingErr != nil {
			slotHealthy = false
		}
		return models.JobResultPayload{}, fmt.Errorf("query execution failed: %w", err)
	}

	var totalBytes int64
	if !strings.HasPrefix(parquetFile, "s3://") {
		if fi, err := os.Stat(parquetFile); err == nil {
			totalBytes = fi.Size()
		}
	}

	return models.JobResultPayload{
		Count:     count,
		Table:     tableName,
		Catalog:   e.cfg.DuckLake.Catalog,
		Files:     []string{parquetFile},
		SizeBytes: totalBytes,
	}, nil
}
