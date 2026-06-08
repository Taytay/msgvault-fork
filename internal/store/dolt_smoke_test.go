package store_test

import (
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	assertpkg "github.com/stretchr/testify/assert"
	requirepkg "github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/store"
	"go.kenn.io/msgvault/internal/testutil"
	"go.kenn.io/msgvault/internal/testutil/storetest"
)

// requireDolt skips the calling test unless MSGVAULT_TEST_DB targets Dolt/MySQL.
func requireDolt(t *testing.T) {
	t.Helper()
	testDB := os.Getenv("MSGVAULT_TEST_DB")
	if !strings.HasPrefix(testDB, "mysql://") && !strings.HasPrefix(testDB, "dolt://") {
		t.Skip("set MSGVAULT_TEST_DB=mysql://... to run the Dolt integration test")
	}
}

// TestMySQLDialect_Unit covers the dialect's pure SQL-shaping and error
// classification on any platform (no database required). These are the M0/M1
// behaviors a Dolt backend depends on.
func TestMySQLDialect_Unit(t *testing.T) {
	assert := assertpkg.New(t)
	d := &store.MySQLDialect{}

	assert.Equal("mysql", d.DriverName())
	assert.Equal("SELECT ?", d.Rebind("SELECT ?"), "MySQL placeholders are native; Rebind is a no-op")
	assert.Equal("NOW()", d.Now())
	assert.Equal("c = 1", d.BoolTrueExpr("c"), "BOOLEAN is TINYINT(1) on MySQL")

	// INSERT OR IGNORE (SQLite form) rewrites to INSERT IGNORE; no suffix.
	assert.Equal("INSERT IGNORE INTO t (a) VALUES (?)",
		d.InsertOrIgnore("INSERT OR IGNORE INTO t (a) VALUES (?)"))
	assert.Equal("INSERT IGNORE INTO t (a) VALUES ",
		d.InsertOrIgnorePrefix("INSERT OR IGNORE INTO t (a) VALUES "))
	assert.Equal("", d.InsertOrIgnoreSuffix())

	// FTS is served from the local SQLite replica, so the Dolt dialect does
	// NOT implement the optional FTSIndexer capability.
	var dialect store.Dialect = d
	_, hasFTS := dialect.(store.FTSIndexer)
	assert.False(hasFTS, "MySQL/Dolt must not advertise the FTS capability")

	// Dolt supports RETURNING, so the Exec+SELECT fallback must stay off.
	assert.False(d.IsReturningError(nil))

	// Error classification keys off the driver's numeric codes.
	assert.True(d.IsConflictError(&mysql.MySQLError{Number: 1062}), "1062 = ER_DUP_ENTRY")
	assert.False(d.IsConflictError(&mysql.MySQLError{Number: 1146}))
	assert.True(d.IsNoSuchTableError(&mysql.MySQLError{Number: 1146}), "1146 = ER_NO_SUCH_TABLE")
	assert.True(d.IsDuplicateColumnError(&mysql.MySQLError{Number: 1060}), "1060 = ER_DUP_FIELDNAME")
	assert.True(d.IsBusyError(&mysql.MySQLError{Number: 1205}), "1205 = lock wait timeout")
	assert.True(d.IsBusyError(&mysql.MySQLError{Number: 1213}), "1213 = deadlock")
	assert.False(d.IsConflictError(nil))
}

// TestMySQLDialect_RewriteUpsert covers the canonical-ON-CONFLICT -> MySQL
// translation for the real upsert shapes used across internal/store. No
// database required.
func TestMySQLDialect_RewriteUpsert(t *testing.T) {
	d := &store.MySQLDialect{}

	t.Run("passthrough for non-upserts", func(t *testing.T) {
		in := "SELECT id FROM messages WHERE source_id = ?"
		assertpkg.Equal(t, in, d.RewriteUpsert(in))
	})

	t.Run("DO UPDATE with excluded refs", func(t *testing.T) {
		out := d.RewriteUpsert(`INSERT INTO message_bodies (message_id, body_text, body_html)
			VALUES (?, ?, ?)
			ON CONFLICT(message_id) DO UPDATE SET
				body_text = excluded.body_text,
				body_html = excluded.body_html`)
		assertpkg.NotContains(t, out, "ON CONFLICT")
		assertpkg.NotContains(t, out, "excluded.")
		assertpkg.Contains(t, out, "ON DUPLICATE KEY UPDATE")
		assertpkg.Contains(t, out, "body_text = VALUES(body_text)")
		assertpkg.Contains(t, out, "body_html = VALUES(body_html)")
	})

	t.Run("DO UPDATE with WHERE target, uppercase EXCLUDED, RETURNING", func(t *testing.T) {
		out := d.RewriteUpsert(`INSERT INTO participants (email_address, display_name, domain)
			VALUES (?, ?, ?)
			ON CONFLICT (email_address) WHERE email_address IS NOT NULL
				DO UPDATE SET email_address = EXCLUDED.email_address
			RETURNING id`)
		assertpkg.NotContains(t, out, "ON CONFLICT")
		assertpkg.NotContains(t, out, "WHERE email_address IS NOT NULL", "partial-index predicate must be dropped")
		assertpkg.Contains(t, out, "ON DUPLICATE KEY UPDATE")
		assertpkg.Contains(t, out, "email_address = VALUES(email_address)")
		assertpkg.Contains(t, out, "RETURNING id", "RETURNING must survive (Dolt supports it)")
	})

	t.Run("DO UPDATE preserves table-qualified existing-row refs in CASE", func(t *testing.T) {
		out := d.RewriteUpsert(`INSERT INTO conversations (source_id, source_conversation_id, conversation_type, title)
			VALUES (?, ?, ?, ?)
			ON CONFLICT (source_id, source_conversation_id) DO UPDATE
			SET conversation_type = EXCLUDED.conversation_type,
			    title = CASE WHEN EXCLUDED.title IS NOT NULL AND EXCLUDED.title != ''
			                 THEN EXCLUDED.title ELSE conversations.title END
			RETURNING id`)
		assertpkg.NotContains(t, out, "EXCLUDED.")
		assertpkg.Contains(t, out, "ON DUPLICATE KEY UPDATE")
		assertpkg.Contains(t, out, "VALUES(title)")
		assertpkg.Contains(t, out, "conversations.title", "existing-row ref stays as the current value")
	})

	t.Run("DO NOTHING becomes INSERT IGNORE", func(t *testing.T) {
		out := d.RewriteUpsert(`INSERT INTO attachments (message_id, content_hash)
			VALUES (?, ?)
			ON CONFLICT (message_id, content_hash) WHERE content_hash IS NOT NULL AND content_hash != '' DO NOTHING`)
		assertpkg.NotContains(t, out, "ON CONFLICT")
		assertpkg.NotContains(t, out, "DO NOTHING")
		assertpkg.Contains(t, out, "INSERT IGNORE INTO attachments")
	})
}

// TestStore_URLDetection verifies the backend-selection helpers route mysql://
// and dolt:// URLs to the MySQL/Dolt path.
func TestStore_URLDetection(t *testing.T) {
	assert := assertpkg.New(t)
	assert.True(store.IsMySQLURL("mysql://root@127.0.0.1:3306/db"))
	assert.True(store.IsMySQLURL("dolt://root@host/db"))
	assert.False(store.IsMySQLURL("postgres://x/y"))
	assert.False(store.IsMySQLURL("/var/lib/msgvault.db"))
	assert.True(store.IsServerURL("mysql://x/y"))
	assert.True(store.IsServerURL("postgresql://x/y"))
	assert.False(store.IsServerURL("/tmp/x.db"))
}

// TestDolt_SchemaAndRoundtrip is the M0/M1 integration check: against a live
// Dolt sql-server it opens a store, runs InitSchema (executing the entire
// schema_mysql.sql in one multi-statement Exec), and verifies the connection,
// schema, a RETURNING insert, a DATETIME roundtrip, and DatabaseSize.
//
// Gated on MSGVAULT_TEST_DB=mysql://... (or dolt://...).
func TestDolt_SchemaAndRoundtrip(t *testing.T) {
	requireDolt(t)
	require := requirepkg.New(t)
	assert := assertpkg.New(t)

	st := testutil.NewTestStore(t) // creates an isolated DB + InitSchema
	require.True(st.IsMySQL(), "store should report the MySQL/Dolt backend")

	db := st.DB()

	// Schema landed: the messages table and a representative index column exist.
	var tableCount int
	require.NoError(db.QueryRow(
		`SELECT COUNT(*) FROM information_schema.tables
		   WHERE table_schema = DATABASE() AND table_name = 'messages'`,
	).Scan(&tableCount))
	assert.Equal(1, tableCount, "messages table should exist after InitSchema")

	// RETURNING works on Dolt (the headline correction): insert a source and
	// get its generated id back in one round-trip.
	var srcID int64
	require.NoError(db.QueryRow(
		`INSERT INTO sources (source_type, identifier) VALUES (?, ?) RETURNING id`,
		"gmail", "smoke@example.com",
	).Scan(&srcID), "INSERT ... RETURNING id")
	assert.Positive(srcID)

	// DATETIME roundtrip with parseTime: write NOW(), read back as time.Time.
	_, err := db.Exec(`UPDATE sources SET last_sync_at = NOW() WHERE id = ?`, srcID)
	require.NoError(err)
	var lastSync sql.NullTime
	require.NoError(db.QueryRow(
		`SELECT last_sync_at FROM sources WHERE id = ?`, srcID,
	).Scan(&lastSync))
	assert.True(lastSync.Valid, "last_sync_at should scan into time.Time")
	assert.WithinDuration(time.Now(), lastSync.Time, 5*time.Minute)

	// UNIQUE(source_type, identifier) is enforced -> a duplicate is a conflict.
	_, dupErr := db.Exec(
		`INSERT INTO sources (source_type, identifier) VALUES (?, ?)`,
		"gmail", "smoke@example.com",
	)
	require.Error(dupErr)
	d := &store.MySQLDialect{}
	assert.True(d.IsConflictError(dupErr),
		"duplicate unique key should classify as a conflict")

	// DatabaseSize reports a positive logical size via information_schema.
	size, err := d.DatabaseSize(db, "")
	require.NoError(err)
	assert.Positive(size)
}

// TestDolt_UpsertRoundtrip exercises the real store write APIs against Dolt to
// prove the canonical ON CONFLICT upserts run correctly after translation:
// GetOrCreateSource / EnsureConversation (storetest.New), UpsertMessage,
// UpsertMessageBody, EnsureLabel, and UpsertAttachment. Idempotency (the
// conflict path returning the existing id) is the key assertion.
func TestDolt_UpsertRoundtrip(t *testing.T) {
	requireDolt(t)
	require := requirepkg.New(t)
	assert := assertpkg.New(t)

	// storetest.New runs GetOrCreateSource + EnsureConversation — both are
	// ON CONFLICT ... DO UPDATE ... RETURNING id upserts.
	f := storetest.New(t)
	require.True(f.Store.IsMySQL())
	require.Positive(f.Source.ID)
	require.Positive(f.ConvID)

	// GetOrCreateSource conflict path returns the same source id.
	src2, err := f.Store.GetOrCreateSource("gmail", "test@example.com")
	require.NoError(err)
	assert.Equal(f.Source.ID, src2.ID, "GetOrCreateSource must be idempotent")

	// UpsertMessage: insert, then re-upsert the same source_message_id and
	// confirm the conflict path returns the existing id (ON DUPLICATE KEY
	// UPDATE ... RETURNING id).
	mid := f.CreateMessage("dolt-msg-1")
	require.Positive(mid)
	mid2 := f.CreateMessage("dolt-msg-1")
	assert.Equal(mid, mid2, "UpsertMessage must be idempotent on (source_id, source_message_id)")

	// UpsertMessageBody (DO UPDATE on message_id).
	require.NoError(f.Store.UpsertMessageBody(mid,
		sql.NullString{String: "hello dolt body", Valid: true}, sql.NullString{}))

	// EnsureLabel (DO UPDATE on (source_id, name)).
	lid, err := f.Store.EnsureLabel(f.Source.ID, "L1", "Inbox", "user")
	require.NoError(err)
	assert.Positive(lid)

	// UpsertAttachment (DO NOTHING -> INSERT IGNORE).
	require.NoError(f.Store.UpsertAttachment(mid, "a.pdf", "application/pdf", "/p/a.pdf", "hash123", 1234))

	// Verify the rows landed.
	db := f.Store.DB()
	var msgCount, bodyCount, attCount int
	require.NoError(db.QueryRow(`SELECT COUNT(*) FROM messages WHERE source_id = ?`, f.Source.ID).Scan(&msgCount))
	require.NoError(db.QueryRow(`SELECT COUNT(*) FROM message_bodies WHERE message_id = ?`, mid).Scan(&bodyCount))
	require.NoError(db.QueryRow(`SELECT COUNT(*) FROM attachments WHERE message_id = ?`, mid).Scan(&attCount))
	assert.Equal(1, msgCount, "exactly one message after idempotent upserts")
	assert.Equal(1, bodyCount)
	assert.Equal(1, attCount)

	// Re-upserting the same (message_id, content_hash) must dedup via the
	// generated-column UNIQUE key (DO NOTHING -> INSERT IGNORE), not duplicate.
	require.NoError(f.Store.UpsertAttachment(mid, "a.pdf", "application/pdf", "/p/a.pdf", "hash123", 1234))
	require.NoError(db.QueryRow(`SELECT COUNT(*) FROM attachments WHERE message_id = ?`, mid).Scan(&attCount))
	assert.Equal(1, attCount, "attachment dedup on (message_id, content_hash) must hold on Dolt")
}

// TestDolt_ReadOnly verifies OpenReadOnly fails loudly on the Dolt backend
// rather than returning a store that is "read-only" in name only. Dolt does not
// enforce per-connection read-only, so a silent writable handle would be a
// footgun; read-only consumers use the local SQLite replica instead.
func TestDolt_ReadOnly(t *testing.T) {
	requireDolt(t)
	require := requirepkg.New(t)

	_, err := store.OpenReadOnly(os.Getenv("MSGVAULT_TEST_DB"))
	require.Error(err, "OpenReadOnly must not return a writable Dolt store")
	require.Contains(err.Error(), "read-only mode is not supported on the Dolt backend")
}
