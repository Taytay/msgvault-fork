package store_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/store"
	"go.kenn.io/msgvault/internal/testutil"
)

// TestSubsetExporter_Portable exercises the SubsetExporter capability end to
// end: seed a source, export the 2 most recent messages, and confirm the new
// store contains exactly that subset. Runs against SQLite by default (dest is a
// directory) and against Dolt when MSGVAULT_TEST_DB is set (dest is a new
// database on the server).
func TestSubsetExporter_Portable(t *testing.T) {
	st := testutil.NewTestStore(t)

	src, err := st.GetOrCreateSource("gmail", "subset@example.com")
	require.NoError(t, err)
	conv, err := st.EnsureConversation(src.ID, "thread-1", "Thread")
	require.NoError(t, err)
	alice, err := st.EnsureParticipant("alice@example.com", "Alice", "example.com")
	require.NoError(t, err)

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		mid, err := st.UpsertMessage(&store.Message{
			ConversationID:  conv,
			SourceID:        src.ID,
			SourceMessageID: fmt.Sprintf("m%d", i),
			MessageType:     "email",
			SentAt:          sql.NullTime{Time: base.Add(time.Duration(i) * time.Hour), Valid: true},
			Subject:         sql.NullString{String: fmt.Sprintf("subject %d", i), Valid: true},
			SizeEstimate:    1000,
		})
		require.NoError(t, err, "UpsertMessage %d", i)
		require.NoError(t, st.ReplaceMessageRecipients(mid, "from", []int64{alice}, []string{"Alice"}))
	}

	exp, ok := st.SubsetExporter()
	require.True(t, ok, "SubsetExporter should be available on %s", st.Backend())
	assert.NotEmpty(t, exp.DestinationHint())

	dest, subsetDSN := subsetDestination(t, st)
	res, err := exp.ExportSubset(context.Background(), 2, dest)
	require.NoError(t, err, "ExportSubset")
	assert.Equal(t, int64(2), res.Messages, "subset should hold the 2 requested messages")
	assert.Equal(t, int64(1), res.Sources)

	// Open the new store and verify it contains exactly the subset.
	sub, err := store.Open(subsetDSN)
	require.NoError(t, err, "open subset store")
	defer func() { _ = sub.Close() }()

	var msgs, recips, parts int
	require.NoError(t, sub.DB().QueryRow("SELECT COUNT(*) FROM messages").Scan(&msgs))
	require.NoError(t, sub.DB().QueryRow("SELECT COUNT(*) FROM message_recipients").Scan(&recips))
	require.NoError(t, sub.DB().QueryRow("SELECT COUNT(*) FROM participants").Scan(&parts))
	assert.Equal(t, 2, msgs, "subset messages")
	assert.Equal(t, 2, recips, "each copied message keeps its from-recipient")
	assert.Equal(t, 1, parts, "the shared sender participant is copied once")

	// The two most recent (m3, m4) should be the ones copied.
	var have []string
	rows, err := sub.DB().Query("SELECT source_message_id FROM messages ORDER BY source_message_id")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var id string
		require.NoError(t, rows.Scan(&id))
		have = append(have, id)
	}
	assert.Equal(t, []string{"m3", "m4"}, have, "the most-recent two messages are subsetted")
}

// subsetDestination returns the dest argument for ExportSubset and a DSN to
// open the resulting store, per backend. For Dolt it registers cleanup that
// drops the created database.
func subsetDestination(t *testing.T, st *store.Store) (dest, dsn string) {
	t.Helper()
	if !st.IsMySQL() {
		dir := t.TempDir()
		return dir, filepath.Join(dir, "msgvault.db")
	}
	base := os.Getenv("MSGVAULT_TEST_DB")
	name := fmt.Sprintf("subset_test_%d", time.Now().UnixNano())
	u, err := url.Parse(base)
	require.NoError(t, err)
	host := u.Host
	if !strings.Contains(host, ":") {
		host += ":3306"
	}
	user := ""
	if u.User != nil {
		user = u.User.Username() + "@"
	}
	ctl, err := sql.Open("mysql", fmt.Sprintf("%stcp(%s)/", user, host))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = ctl.Exec("DROP DATABASE IF EXISTS " + name)
		_ = ctl.Close()
	})
	u.Path = "/" + name
	return name, u.String()
}
