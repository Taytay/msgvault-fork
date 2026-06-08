package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestOpenReadEngine_Selection covers the non-DuckDB branches of the factory
// (the DuckDB path needs a real Parquet cache and is exercised by the duckdb
// tests). Construction touches no DB, so a nil handle is fine here.
func TestOpenReadEngine_Selection(t *testing.T) {
	t.Run("postgres uses the dialect engine, not DuckDB", func(t *testing.T) {
		e := OpenReadEngine(nil, "postgres://x/y", ReadEngineOptions{IsPostgres: true})
		_, isPG := e.(*pgEngine)
		assert.True(t, isPG, "PostgreSQL should select the pg dialect engine")
	})

	t.Run("force-sql uses the direct SQLite engine", func(t *testing.T) {
		e := OpenReadEngine(nil, "/tmp/x.db", ReadEngineOptions{
			AnalyticsDir: t.TempDir(), ForceSQL: true,
		})
		_, isSQLite := e.(*SQLiteEngine)
		assert.True(t, isSQLite, "force-sql should select the direct SQLite engine")
	})

	t.Run("no cache falls back to the direct SQLite engine", func(t *testing.T) {
		// Empty analytics dir → HasCompleteParquetData is false.
		e := OpenReadEngine(nil, "/tmp/x.db", ReadEngineOptions{AnalyticsDir: t.TempDir()})
		_, isSQLite := e.(*SQLiteEngine)
		assert.True(t, isSQLite, "missing cache should select the direct SQLite engine")
	})

	t.Run("stale cache is not used", func(t *testing.T) {
		e := OpenReadEngine(nil, "/tmp/x.db", ReadEngineOptions{
			AnalyticsDir: t.TempDir(), CacheStale: true,
		})
		_, isSQLite := e.(*SQLiteEngine)
		assert.True(t, isSQLite, "a stale cache should not select DuckDB")
	})
}
