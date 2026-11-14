package engine

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net"
	"regexp"
	"strings"

	_ "github.com/duckdb/duckdb-go/v2"
	gomysql "github.com/go-sql-driver/mysql"
	"github.com/zerodha/dungbeetle/v2/models"
)

var reSafeIdentifier = regexp.MustCompile(`^[a-zA-Z0-9_:-]+$`)

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
	ThreadsPerJob     int                     `koanf:"threads_per_job"`
	MemoryLimitPerJob string                  `koanf:"memory_limit_per_job"`
	DuckLake          DuckLakeConfig          `koanf:"ducklake"`
	Sources           map[string]SourceConfig `koanf:"sources"`
}

// DuckDBEngine manages executing jobs against source databases with DuckLake outputs.
type DuckDBEngine struct {
	cfg Config
	lo  *slog.Logger
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
		if cfg.DuckLake.S3Region == "" && cfg.DuckLake.S3Endpoint == "" {
			return fmt.Errorf("ducklake.data_path is an S3 URI ('%s') but neither ducklake.s3_region nor ducklake.s3_endpoint is configured", cfg.DuckLake.DataPath)
		}
	}
	return nil
}

// New creates a new DuckDBEngine instance with safe defaults and validates configuration.
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
	return &DuckDBEngine{cfg: cfg, lo: lo}, nil
}

// GetCatalog returns the DuckLake catalog connection string.
func (e *DuckDBEngine) GetCatalog() string {
	return e.cfg.DuckLake.Catalog
}

// GetSources returns the registered source database names.
func (e *DuckDBEngine) GetSources() map[string]SourceConfig {
	return e.cfg.Sources
}

// ExecuteJob runs a query within an ephemeral, isolated in-memory DuckDB connection,
// streaming results directly to DuckLake Parquet files and recording catalog metadata.
func (e *DuckDBEngine) ExecuteJob(ctx context.Context, jobID, sourceDB, query string, args ...interface{}) (models.JobResultPayload, error) {
	if !reSafeIdentifier.MatchString(jobID) {
		return models.JobResultPayload{}, fmt.Errorf("invalid characters in job_id: %s", jobID)
	}

	// 1. Open ephemeral in-memory connection
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return models.JobResultPayload{}, fmt.Errorf("failed to open ephemeral duckdb: %w", err)
	}
	defer db.Close()

	// 2. Resource limits (safe fallback defaults if omitted from config)
	threads := e.cfg.ThreadsPerJob
	if threads <= 0 {
		threads = 2
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("PRAGMA threads=%d;", threads)); err != nil {
		return models.JobResultPayload{}, fmt.Errorf("failed to set thread limit: %w", err)
	}

	memLimit := e.cfg.MemoryLimitPerJob
	if memLimit == "" {
		memLimit = "2GB"
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("PRAGMA memory_limit='%s';", memLimit)); err != nil {
		return models.JobResultPayload{}, fmt.Errorf("failed to set memory limit: %w", err)
	}

	// 3. Configure S3 Secret if region/endpoint provided
	if e.cfg.DuckLake.S3Region != "" || e.cfg.DuckLake.S3Endpoint != "" {
		secretSQL := "CREATE SECRET s3_creds (TYPE S3"
		if e.cfg.DuckLake.S3Region != "" {
			secretSQL += fmt.Sprintf(", REGION '%s'", e.cfg.DuckLake.S3Region)
		}
		if e.cfg.DuckLake.S3Endpoint != "" {
			secretSQL += fmt.Sprintf(", ENDPOINT '%s'", e.cfg.DuckLake.S3Endpoint)
		}
		if e.cfg.DuckLake.S3AccessKey != "" && e.cfg.DuckLake.S3SecretKey != "" {
			secretSQL += fmt.Sprintf(", KEY_ID '%s', SECRET '%s'", e.cfg.DuckLake.S3AccessKey, e.cfg.DuckLake.S3SecretKey)
		} else {
			secretSQL += ", PROVIDER CREDENTIAL_CHAIN"
		}
		urlStyle := e.cfg.DuckLake.S3URLStyle
		if urlStyle == "" && (!e.cfg.DuckLake.S3UseSSL || strings.Contains(e.cfg.DuckLake.S3Endpoint, "127.0.0.1") || strings.Contains(e.cfg.DuckLake.S3Endpoint, "localhost")) {
			urlStyle = "path"
		}
		if urlStyle != "" {
			secretSQL += fmt.Sprintf(", URL_STYLE '%s'", urlStyle)
		}
		secretSQL += fmt.Sprintf(", USE_SSL %t);", e.cfg.DuckLake.S3UseSSL)
		if _, err := db.ExecContext(ctx, secretSQL); err != nil {
			return models.JobResultPayload{}, fmt.Errorf("failed to configure s3 secret: %w", err)
		}
	}

	// 4. Attach DuckLake (DATA_INLINING_ROW_LIMIT 0 guarantees Parquet files are always emitted)
	attachLake := fmt.Sprintf("ATTACH '%s' AS lake (DATA_PATH '%s', DATA_INLINING_ROW_LIMIT 0);", e.cfg.DuckLake.Catalog, e.cfg.DuckLake.DataPath)
	if _, err := db.ExecContext(ctx, attachLake); err != nil {
		return models.JobResultPayload{}, fmt.Errorf("failed to attach ducklake: %w", err)
	}

	// 5. Attach Source DB
	if sourceDB == "" {
		return models.JobResultPayload{}, fmt.Errorf("source database must be specified")
	}
	src, ok := e.cfg.Sources[sourceDB]
	if !ok {
		return models.JobResultPayload{}, fmt.Errorf("unknown source db '%s'", sourceDB)
	}
		var attachSrc string
		switch strings.ToLower(src.Type) {
		case "postgres":
			attachSrc = fmt.Sprintf("ATTACH '%s' AS %s (TYPE POSTGRES, READ_ONLY);", src.DSN, sourceDB)
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
			attachSrc = fmt.Sprintf("ATTACH '%s' AS %s (TYPE MYSQL, READ_ONLY);", dsn, sourceDB)
		default:
			return models.JobResultPayload{}, fmt.Errorf("unsupported source type '%s'", src.Type)
		}
		if _, err := db.ExecContext(ctx, attachSrc); err != nil {
			return models.JobResultPayload{}, fmt.Errorf("failed to attach source '%s': %w", sourceDB, err)
		}

	// Switch active catalog context to sourceDB so unqualified table names resolve directly
	if _, err := db.ExecContext(ctx, fmt.Sprintf("USE %s;", sourceDB)); err != nil {
		return models.JobResultPayload{}, fmt.Errorf("failed to switch context to source '%s': %w", sourceDB, err)
	}

	// 6. Execute CTAS directly into DuckLake
	targetTable := fmt.Sprintf(`lake."results_%s"`, jobID)
	cleanedQuery := strings.TrimRight(strings.TrimSpace(query), ";")
	ctasSQL := fmt.Sprintf("CREATE TABLE %s AS %s", targetTable, cleanedQuery)

	if _, err := db.ExecContext(ctx, ctasSQL, args...); err != nil {
		return models.JobResultPayload{}, fmt.Errorf("query execution failed: %w", err)
	}

	// 7. Get row count
	var count int64
	row := db.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM %s", targetTable))
	if err := row.Scan(&count); err != nil {
		return models.JobResultPayload{}, fmt.Errorf("failed to count rows: %w", err)
	}

	// 8. Retrieve generated Parquet files list from DuckLake
	var files []string
	var totalBytes int64
	fileRows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT data_file, data_file_size_bytes FROM ducklake_list_files('lake', 'results_%s')", jobID))
	if err == nil {
		defer fileRows.Close()
		for fileRows.Next() {
			var f string
			var s int64
			if err := fileRows.Scan(&f, &s); err == nil {
				files = append(files, f)
				totalBytes += s
			}
		}
	}

	return models.JobResultPayload{
		Count:     count,
		Table:     fmt.Sprintf("results_%s", jobID),
		Catalog:   e.cfg.DuckLake.Catalog,
		Files:     files,
		SizeBytes: totalBytes,
	}, nil
}
