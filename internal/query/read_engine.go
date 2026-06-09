package query

import (
	"log/slog"

	"go.kenn.io/msgvault/internal/store"
)

// ReadEngineOptions configures OpenReadEngine.
type ReadEngineOptions struct {
	// AnalyticsDir holds the Parquet analytics cache; consulted only for the
	// DuckDB fast path.
	AnalyticsDir string
	// ForceSQL skips the Parquet/DuckDB fast path and uses the direct engine.
	ForceSQL bool
	// CacheStale marks the Parquet cache as out of date: when set, the direct
	// engine is used even if the cache files are complete. Callers compute it
	// (e.g. via the build-cache staleness check); it is irrelevant to backends
	// without the analytics-cache capability, which never use the cache.
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
//   - backends without the analytics-cache capability (PostgreSQL, Dolt) → the
//     dialect-parameterized direct engine (no Parquet cache).
//   - otherwise, when a complete Parquet cache is present and neither stale nor
//     force-disabled → DuckDB over Parquet, falling back to the direct engine
//     (with a warning) if DuckDB cannot open.
//   - otherwise → the direct engine.
//
// Backend selection is delegated entirely to the store's capabilities: the
// presence of AnalyticsCache decides whether the Parquet path is even
// considered, and NewEngineForStore picks the SQL dialect. It does NOT build
// the cache — callers that want a fresh cache build it first (only the TUI
// does, today). The returned engine's Close() is always safe to defer: the
// direct engine's Close is a no-op; only DuckDB owns a handle.
func OpenReadEngine(s *store.Store, opts ReadEngineOptions) Engine {
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}

	cache, ok := s.AnalyticsCache()
	if !ok {
		// No local analytics cache (PostgreSQL, Dolt): query the backend directly.
		return NewEngineForStore(s)
	}

	useCache := !opts.ForceSQL && !opts.CacheStale && HasCompleteParquetData(opts.AnalyticsDir)
	if useCache {
		duck, err := NewDuckDBEngine(opts.AnalyticsDir, cache.SourcePath(), s.DB(),
			DuckDBOptions{DisableSQLiteScanner: opts.DisableSQLiteScanner})
		if err == nil {
			return duck
		}
		log.Warn("DuckDB/Parquet engine failed; falling back to direct SQLite", "error", err.Error())
	} else if !opts.ForceSQL {
		log.Info("Parquet cache not usable; using direct SQLite engine (run 'msgvault build-cache' for faster aggregates)")
	}

	return NewEngineForStore(s)
}
