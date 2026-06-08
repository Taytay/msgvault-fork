package query

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/store"
)

// TestOpenReadEngine_Selection covers the non-DuckDB branches of the factory
// (the DuckDB path needs a real Parquet cache and is exercised by the duckdb
// tests). The PostgreSQL branch is covered by pg_compat_test.go, which needs a
// live server; here we drive the SQLite selection logic with a temp store.
func TestOpenReadEngine_Selection(t *testing.T) {
	newStore := func(t *testing.T) *store.Store {
		t.Helper()
		s, err := store.OpenForTest(filepath.Join(t.TempDir(), "msgvault.db"))
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })
		return s
	}

	t.Run("force-sql uses the direct SQLite engine", func(t *testing.T) {
		e := OpenReadEngine(newStore(t), ReadEngineOptions{
			AnalyticsDir: t.TempDir(), ForceSQL: true,
		})
		_, isSQLite := e.(*SQLiteEngine)
		assert.True(t, isSQLite, "force-sql should select the direct SQLite engine")
	})

	t.Run("no cache falls back to the direct SQLite engine", func(t *testing.T) {
		// Empty analytics dir → HasCompleteParquetData is false.
		e := OpenReadEngine(newStore(t), ReadEngineOptions{AnalyticsDir: t.TempDir()})
		_, isSQLite := e.(*SQLiteEngine)
		assert.True(t, isSQLite, "missing cache should select the direct SQLite engine")
	})

	t.Run("stale cache is not used", func(t *testing.T) {
		e := OpenReadEngine(newStore(t), ReadEngineOptions{
			AnalyticsDir: t.TempDir(), CacheStale: true,
		})
		_, isSQLite := e.(*SQLiteEngine)
		assert.True(t, isSQLite, "a stale cache should not select DuckDB")
	})
}
