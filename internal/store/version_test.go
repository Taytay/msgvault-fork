package store_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	assertpkg "github.com/stretchr/testify/assert"
	requirepkg "github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/store"
	"go.kenn.io/msgvault/internal/testutil/storetest"
)

// TestVersionController_SQLiteUnsupported confirms the capability is absent on a
// non-version-controlled backend, so snapshotting is a clean no-op there.
func TestVersionController_SQLiteUnsupported(t *testing.T) {
	p := filepath.Join(t.TempDir(), "x.db")
	st, err := store.OpenForTest(p) // always SQLite (ignores MSGVAULT_TEST_DB)
	requirepkg.NoError(t, err)
	defer func() { _ = st.Close() }()
	requirepkg.NoError(t, st.InitSchema())

	_, ok := st.VersionController()
	assertpkg.False(t, ok, "SQLite has no version-control capability")
}

// TestVersionController_Dolt exercises the Dolt commit capability and the
// CompleteSync boundary hook against a live Dolt server.
func TestVersionController_Dolt(t *testing.T) {
	testDB := os.Getenv("MSGVAULT_TEST_DB")
	if !strings.HasPrefix(testDB, "mysql://") && !strings.HasPrefix(testDB, "dolt://") {
		t.Skip("set MSGVAULT_TEST_DB=mysql://... to run the Dolt version-control test")
	}
	require := requirepkg.New(t)
	assert := assertpkg.New(t)
	ctx := context.Background()

	f := storetest.New(t) // Dolt store + source "gmail:test@example.com" + conversation
	require.True(f.Store.IsMySQL())

	vc, ok := f.Store.VersionController()
	require.True(ok, "Dolt exposes the version-control capability")

	// storetest.New left uncommitted schema/source/conversation changes.
	h1, err := vc.Commit(ctx, "initial snapshot", "")
	require.NoError(err)
	assert.NotEmpty(h1, "first commit records a hash")

	// Nothing changed since → --skip-empty makes this a silent no-op.
	h2, err := vc.Commit(ctx, "noop", "")
	require.NoError(err)
	assert.Empty(h2, "no-op commit returns empty hash, not an error")

	// CompleteSync at a real sync boundary must produce exactly one commit.
	before := doltLogCount(t, f.Store.DB())
	syncID, err := f.Store.StartSync(f.Source.ID, "incremental")
	require.NoError(err)
	f.CreateMessage("vc-msg-1")
	require.NoError(f.Store.CompleteSync(syncID, "hist-1"))

	after := doltLogCount(t, f.Store.DB())
	assert.Equal(before+1, after, "CompleteSync creates one Dolt commit")

	msg := doltLogTopMessage(t, f.Store.DB())
	assert.Contains(msg, "gmail:test@example.com", "commit message names the source")
	assert.Contains(msg, "run", "commit message references the sync run")
}

func doltLogCount(t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	requirepkg.NoError(t, db.QueryRow("SELECT COUNT(*) FROM dolt_log").Scan(&n))
	return n
}

func doltLogTopMessage(t *testing.T, db *sql.DB) string {
	t.Helper()
	var msg string
	requirepkg.NoError(t, db.QueryRow("SELECT message FROM dolt_log ORDER BY date DESC LIMIT 1").Scan(&msg))
	return msg
}
