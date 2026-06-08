package query

import (
	"database/sql"
	"log/slog"
)

// ReadEngineOptions configures OpenReadEngine.
type ReadEngineOptions struct {
	// AnalyticsDir holds the Parquet analytics cache; consulted only for the
	// DuckDB fast path.
	AnalyticsDir string
	// IsPostgres selects the PostgreSQL dialect engine and bypasses the
	// SQLite-only Parquet/DuckDB pipeline entirely.
	IsPostgres bool
	// ForceSQL skips the Parquet/DuckDB fast path and uses the direct engine.
	ForceSQL bool
	// CacheStale marks the Parquet cache as out of date: when set, the direct
	// engine is used even if the cache files are complete. Callers compute it
	// (e.g. via the build-cache staleness check); it is irrelevant on
	// PostgreSQL, which never uses the cache.
	CacheStale bool
	// DisableSQLiteScanner forces DuckDB to read via the CSV fallback instead
	// of the sqlite_scanner extension (Windows / debugging).
	DisableSQLiteScanner bool
	// Log receives selection/fallback messages; defaults to slog.Default().
	Log *slog.Logger
}

// OpenReadEngine selects the read/analytics engine for a local store. It
// consolidates the choice that the serve, mcp, and tui commands each used to
// duplicate (and had begun to drift on):
//
//   - PostgreSQL → the dialect-parameterized SQLite engine (no Parquet cache).
//   - otherwise, when a complete Parquet cache is present and neither stale nor
//     force-disabled → DuckDB over Parquet, falling back to the direct SQLite
//     engine (with a warning) if DuckDB cannot open.
//   - otherwise → the direct SQLite engine.
//
// It does NOT build the cache — callers that want a fresh cache build it first
// (only the TUI does, today). The returned engine's Close() is always safe to
// defer: the SQLite engine's Close is a no-op; only DuckDB owns a handle.
func OpenReadEngine(db *sql.DB, dbPath string, opts ReadEngineOptions) Engine {
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}

	if opts.IsPostgres {
		return NewEngine(db, true)
	}

	useCache := !opts.ForceSQL && !opts.CacheStale && HasCompleteParquetData(opts.AnalyticsDir)
	if useCache {
		duck, err := NewDuckDBEngine(opts.AnalyticsDir, dbPath, db,
			DuckDBOptions{DisableSQLiteScanner: opts.DisableSQLiteScanner})
		if err == nil {
			return duck
		}
		log.Warn("DuckDB/Parquet engine failed; falling back to direct SQLite", "error", err.Error())
	} else if !opts.ForceSQL {
		log.Info("Parquet cache not usable; using direct SQLite engine (run 'msgvault build-cache' for faster aggregates)")
	}

	return NewEngine(db, false)
}
