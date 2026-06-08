package projection_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	assertpkg "github.com/stretchr/testify/assert"
	requirepkg "github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/projection"
	"go.kenn.io/msgvault/internal/search"
	"go.kenn.io/msgvault/internal/store"
	"go.kenn.io/msgvault/internal/testutil/storetest"
)

// TestProject_DoltToSQLiteReplica exercises the full projection against a live
// Dolt source: ingest into Dolt, project into a fresh local SQLite replica, and
// confirm row counts, a working FTS5 body search on the replica, and
// idempotency. Gated on MSGVAULT_TEST_DB=mysql://... and the fts5 build tag.
func TestProject_DoltToSQLiteReplica(t *testing.T) {
	testDB := os.Getenv("MSGVAULT_TEST_DB")
	if !strings.HasPrefix(testDB, "mysql://") && !strings.HasPrefix(testDB, "dolt://") {
		t.Skip("set MSGVAULT_TEST_DB=mysql://... to run the Dolt projection test")
	}
	require := requirepkg.New(t)
	assert := assertpkg.New(t)
	ctx := context.Background()

	// System of record: Dolt-backed store (storetest.New honors MSGVAULT_TEST_DB).
	f := storetest.New(t)
	require.True(f.Store.IsMySQL(), "source must be the Dolt backend")
	mid := f.CreateMessage("proj-msg-1")
	require.NoError(f.Store.UpsertMessageBody(mid,
		sql.NullString{String: "uniquesearchtoken hello world", Valid: true}, sql.NullString{}))
	_, err := f.Store.EnsureLabel(f.Source.ID, "L1", "Inbox", "user")
	require.NoError(err)
	require.NoError(f.Store.UpsertAttachment(mid, "a.pdf", "application/pdf", "/p/a.pdf", "h1", 10))

	// Read replica: fresh local SQLite database.
	replicaPath := filepath.Join(t.TempDir(), "replica.db")
	dst, err := store.OpenForTest(replicaPath)
	require.NoError(err)
	defer func() { _ = dst.Close() }()
	require.NoError(dst.InitSchema())
	require.False(dst.IsMySQL(), "replica must be SQLite")

	rep, err := projection.Project(ctx, f.Store, dst)
	require.NoError(err)
	assert.Equal(int64(1), rep.Rows["messages"], "messages copied")
	assert.Equal(int64(1), rep.Rows["message_bodies"], "bodies copied")
	assert.Equal(int64(1), rep.Rows["attachments"], "attachments copied")
	assert.GreaterOrEqual(rep.Rows["sources"], int64(1), "source copied")
	assert.GreaterOrEqual(rep.FTSIndexed, int64(1), "messages backfilled into FTS5")

	// The replica's FTS5 index resolves a body-only search term — proof the
	// SQLite read path works end-to-end from Dolt-sourced data.
	msgs, total, err := dst.SearchMessagesQuery(
		&search.Query{TextTerms: []string{"uniquesearchtoken"}}, 0, 10)
	require.NoError(err)
	assert.Equal(int64(1), total, "FTS body search finds the projected message")
	require.Len(msgs, 1)

	// Idempotency: re-projecting clears and reloads without duplicating rows.
	rep2, err := projection.Project(ctx, f.Store, dst)
	require.NoError(err)
	assert.Equal(int64(1), rep2.Rows["messages"], "re-projection must not duplicate")
}
