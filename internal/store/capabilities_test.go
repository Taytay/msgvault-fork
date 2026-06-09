package store

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSQLiteCapabilities verifies a local-file SQLite store advertises the
// local-file capabilities (analytics cache, integrity check, snapshot backup)
// and that they behave.
func TestSQLiteCapabilities(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "msgvault.db")
	s, err := OpenForTest(dbPath)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	require.NoError(t, s.InitSchema())

	assert.Equal(t, BackendSQLite, s.Backend())

	cache, ok := s.AnalyticsCache()
	require.True(t, ok, "SQLite file store should expose the analytics-cache capability")
	assert.Equal(t, dbPath, cache.SourcePath())

	// RequireAnalyticsCache returns the capability without error for SQLite.
	gotCache, err := s.RequireAnalyticsCache()
	require.NoError(t, err)
	assert.Equal(t, dbPath, gotCache.SourcePath())

	checker, ok := s.IntegrityChecker()
	require.True(t, ok, "SQLite should expose the integrity-check capability")
	problems, err := checker.CheckIntegrity(context.Background())
	require.NoError(t, err)
	assert.Empty(t, problems, "a fresh database should be healthy")

	backup, ok := s.SnapshotBackup()
	require.True(t, ok, "SQLite should expose the snapshot-backup capability")
	dst := filepath.Join(t.TempDir(), "backup.db")
	require.NoError(t, backup.BackupTo(context.Background(), dst))
	assert.FileExists(t, dst)
}

// TestInMemorySQLiteHasNoLocalFileCapabilities verifies the local-file
// capabilities are gated on an actual file: an in-memory database exposes none
// of them, so callers fall back to the direct query path.
func TestInMemorySQLiteHasNoLocalFileCapabilities(t *testing.T) {
	s, err := OpenForTest(":memory:")
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	_, ok := s.AnalyticsCache()
	assert.False(t, ok, "in-memory store has no file for the analytics ETL")

	_, ok = s.IntegrityChecker()
	assert.False(t, ok)

	_, ok = s.SnapshotBackup()
	assert.False(t, ok)

	// RequireAnalyticsCache surfaces a typed UnsupportedError naming the backend.
	_, err = s.RequireAnalyticsCache()
	require.Error(t, err)
	var unsupported *UnsupportedError
	require.ErrorAs(t, err, &unsupported)
	assert.Equal(t, "SQLite", unsupported.Backend)
}

func TestBackendOfDSN(t *testing.T) {
	cases := map[string]Backend{
		"/var/lib/msgvault.db":           BackendSQLite,
		":memory:":                       BackendSQLite,
		"postgres://x/y":                 BackendPostgreSQL,
		"postgresql://u:p@host:5432/db":  BackendPostgreSQL,
		"mysql://root@127.0.0.1:3306/db": BackendDolt,
		"dolt://root@host/db":            BackendDolt,
	}
	for dsn, want := range cases {
		assert.Equalf(t, want, BackendOfDSN(dsn), "BackendOfDSN(%q)", dsn)
	}
}
