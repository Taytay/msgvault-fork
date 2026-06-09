package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3" // SQLite driver
)

// SubsetExporter copies the most recent rowCount messages (and every row they
// reference) into a new store of the same backend. Each backend produces the
// artifact natural to it — SQLite writes a new msgvault.db file in a directory;
// Dolt creates a new database on the same server — so the meaning of dest is
// backend-specific (see DestinationHint). PostgreSQL does not expose it.
//
// This is the seam that keeps `create-subset` backend-agnostic: the command
// asks the opened store for the capability and forwards the user's --output
// value; all backend knowledge lives in the implementation.
type SubsetExporter interface {
	// ExportSubset copies the rowCount most recent messages into a new store
	// described by dest and returns a summary.
	ExportSubset(ctx context.Context, rowCount int, dest string) (*CopyResult, error)
	// DestinationHint describes what dest means on this backend, for CLI help
	// and error messages.
	DestinationHint() string
}

// SubsetExporter returns the subset-export capability for backends that can
// produce a self-contained subset (SQLite, Dolt) and (nil, false) otherwise.
func (s *Store) SubsetExporter() (SubsetExporter, bool) {
	switch s.Backend() {
	case BackendSQLite:
		// Needs a real file to ATTACH; in-memory stores have nothing to copy.
		if lf, ok := s.localFile(); ok {
			return sqliteSubsetExporter{srcPath: lf.path}, true
		}
		return nil, false
	case BackendDolt:
		return doltSubsetExporter{src: s}, true
	default:
		return nil, false
	}
}

// sqliteSubsetExporter writes the subset to a new msgvault.db file via the
// ATTACH-based CopySubset. dest is the output directory.
type sqliteSubsetExporter struct{ srcPath string }

func (e sqliteSubsetExporter) DestinationHint() string {
	return "an output directory (a new msgvault.db is created inside)"
}

func (e sqliteSubsetExporter) ExportSubset(_ context.Context, rowCount int, dest string) (*CopyResult, error) {
	return CopySubset(e.srcPath, dest, rowCount)
}

// CopyResult holds the summary of a subset copy operation.
type CopyResult struct {
	Messages      int64
	Conversations int64
	Participants  int64
	Labels        int64
	Sources       int64
	DBSize        int64
	Elapsed       time.Duration
}

// CopySubset copies rowCount most recent messages (and all referenced
// data) from srcDBPath into a new database in dstDir. The destination
// schema is initialized using the embedded store schema.
//
// Security: validates srcDBPath for control characters and canonicalizes
// it before use in SQL. Callers must validate path containment.
func CopySubset(
	srcDBPath, dstDir string, rowCount int,
) (*CopyResult, error) {
	if rowCount <= 0 {
		return nil, fmt.Errorf("rowCount must be positive, got %d", rowCount)
	}
	// CopySubset is implemented with SQLite ATTACH DATABASE against local
	// files; client/server backends cannot be a source. The check lives here,
	// with the ATTACH logic it guards, rather than in the calling command.
	if b := BackendOfDSN(srcDBPath); b != BackendSQLite {
		return nil, fmt.Errorf("create-subset requires a local SQLite source database (it uses ATTACH DATABASE); %q is a %s backend", srcDBPath, b)
	}

	// Validate the source up front — before creating the destination — so a
	// missing or malformed source fails fast and ATTACH never creates an empty
	// file for a bad path. Missing-source is reported via ErrDatabaseNotFound
	// so callers can add a setup hint without inspecting the backend.
	srcDBPath, err := filepath.Abs(filepath.Clean(srcDBPath))
	if err != nil {
		return nil, fmt.Errorf("canonicalize source path: %w", err)
	}
	for _, r := range srcDBPath {
		if r < 0x20 || r == 0x7F {
			return nil, fmt.Errorf(
				"source database path contains control character (0x%02X)", r,
			)
		}
	}
	if _, err := os.Stat(srcDBPath); err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("source database not found: %s: %w", srcDBPath, ErrDatabaseNotFound)
		}
		return nil, fmt.Errorf("source database not found: %w", err)
	}

	start := time.Now()

	dstDBPath := filepath.Join(dstDir, "msgvault.db")
	if _, err := os.Stat(dstDBPath); err == nil {
		return nil, fmt.Errorf(
			"destination database already exists: %s", dstDBPath,
		)
	}

	// Track whether we created the dir so cleanup only removes
	// what we made.
	createdDir := false
	if _, err := os.Stat(dstDir); os.IsNotExist(err) {
		createdDir = true
	}

	if err := os.MkdirAll(dstDir, 0700); err != nil {
		return nil, fmt.Errorf("create destination directory: %w", err)
	}

	cleanup := func() {
		if createdDir {
			_ = os.RemoveAll(dstDir)
		} else {
			_ = os.Remove(dstDBPath)
			_ = os.Remove(dstDBPath + "-wal")
			_ = os.Remove(dstDBPath + "-shm")
		}
	}

	// Phase 1: create destination DB with schema
	st, err := Open(dstDBPath)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("create destination database: %w", err)
	}
	if err := st.InitSchema(); err != nil {
		_ = st.Close()
		cleanup()
		return nil, fmt.Errorf("initialize schema: %w", err)
	}
	if err := st.Close(); err != nil {
		cleanup()
		return nil, fmt.Errorf("close schema database: %w", err)
	}

	// Phase 2: re-open with foreign keys OFF for bulk copy
	dsn := dstDBPath +
		"?_journal_mode=WAL&_busy_timeout=5000&_foreign_keys=OFF"
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("reopen database: %w", err)
	}

	// closeAndCleanup closes db before cleanup to ensure WAL/SHM
	// files are released before removal.
	closeAndCleanup := func() {
		_ = db.Close()
		cleanup()
	}

	escapedSrcPath := strings.ReplaceAll(srcDBPath, "'", "''")
	attachSQL := fmt.Sprintf(
		"ATTACH DATABASE '%s' AS src", escapedSrcPath,
	)
	if _, err := db.Exec(attachSQL); err != nil {
		closeAndCleanup()
		return nil, fmt.Errorf("attach source database: %w", err)
	}

	tx, err := db.Begin()
	if err != nil {
		closeAndCleanup()
		return nil, fmt.Errorf("begin transaction: %w", err)
	}

	result, err := copyData(tx, "src.", "", "", rowCount)
	if err != nil {
		_ = tx.Rollback()
		_, _ = db.Exec("DETACH DATABASE src")
		closeAndCleanup()
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		_, _ = db.Exec("DETACH DATABASE src")
		closeAndCleanup()
		return nil, fmt.Errorf("commit transaction: %w", err)
	}

	// Detach source before post-copy operations so PRAGMA
	// foreign_key_check only scans the destination database.
	if _, err := db.Exec("DETACH DATABASE src"); err != nil {
		closeAndCleanup()
		return nil, fmt.Errorf("detach source database: %w", err)
	}

	if err := verifyForeignKeys(db); err != nil {
		closeAndCleanup()
		return nil, err
	}

	if err := updateConversationCounts(db); err != nil {
		closeAndCleanup()
		return nil, fmt.Errorf("update conversation counts: %w", err)
	}

	if ftsErr := populateFTS(db); ftsErr != nil {
		errMsg := ftsErr.Error()
		ftsUnavailable :=
			strings.HasSuffix(errMsg, "no such table: messages_fts") ||
				strings.HasSuffix(errMsg, "no such module: fts5")
		if !ftsUnavailable {
			fmt.Fprintf(
				os.Stderr,
				"warning: FTS index population failed: %v\n",
				ftsErr,
			)
		}
	}

	_ = db.Close()

	if info, err := os.Stat(dstDBPath); err == nil {
		result.DBSize = info.Size()
	}

	result.Elapsed = time.Since(start)
	return result, nil
}

// verifyForeignKeys runs PRAGMA foreign_key_check and returns an error
// if any violations are found.
func verifyForeignKeys(db *sql.DB) error {
	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		return fmt.Errorf("enable foreign keys: %w", err)
	}

	rows, err := db.Query("PRAGMA foreign_key_check")
	if err != nil {
		return fmt.Errorf("foreign key check: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var violations []string
	for rows.Next() {
		var table, rowid, parent, fkid string
		if err := rows.Scan(&table, &rowid, &parent, &fkid); err != nil {
			violations = append(violations,
				fmt.Sprintf("scan error: %v", err))
		} else {
			violations = append(violations,
				fmt.Sprintf("%s(rowid=%s) -> %s", table, rowid, parent))
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate foreign key check: %w", err)
	}

	if len(violations) > 0 {
		return fmt.Errorf(
			"foreign key violations: %s",
			strings.Join(violations, "; "),
		)
	}
	return nil
}

// copyData executes INSERT INTO ... SELECT in dependency order.
// copyData runs the dependency-ordered subset copy. srcQ/dstQ are the
// table-name qualifiers for the source and destination: SQLite passes
// ("src.", "") — source is ATTACH'd as "src", destination is the connection's
// main schema — while Dolt passes ("<srcdb>.", "<destdb>.") to copy between two
// databases on one server. selected_messages is a session TEMPORARY table
// (referenced unqualified on both backends). The caller runs this with foreign
// keys disabled (SQLite: _foreign_keys=OFF DSN; Dolt: SET FOREIGN_KEY_CHECKS=0)
// so insertion order need not be a strict topological sort.
// attachmentCols, when non-empty, is an explicit comma-separated column list
// for the attachments copy (INSERT … (cols) SELECT cols …). It excludes
// generated columns: schema_mysql.sql defines attachments.dedup_content_hash
// as STORED GENERATED, and Dolt/MySQL rejects writing a value into a generated
// column, so SELECT * cannot be used there. SQLite has no generated columns and
// passes "" to keep SELECT *.
func copyData(tx *sql.Tx, srcQ, dstQ, attachmentCols string, rowCount int) (*CopyResult, error) {
	result := &CopyResult{}

	if _, err := tx.Exec(fmt.Sprintf(`
		CREATE TEMPORARY TABLE selected_messages AS
		SELECT id FROM %smessages
		WHERE %s
		ORDER BY COALESCE(sent_at, received_at, internal_date)
			DESC, id DESC LIMIT ?`, srcQ, LiveMessagesWhere("", true)), rowCount); err != nil {
		return nil, fmt.Errorf("select messages: %w", err)
	}

	// Try copying with oauth_app column first; fall back to NULL
	// for source databases created before this column existed.
	res, err := tx.Exec(fmt.Sprintf(`
		INSERT INTO %ssources
			(id, source_type, identifier, display_name, google_user_id,
			 last_sync_at, sync_cursor, sync_config, oauth_app,
			 created_at, updated_at)
		SELECT id, source_type, identifier, display_name, google_user_id,
		       last_sync_at, sync_cursor, sync_config, oauth_app,
		       created_at, updated_at
		FROM %ssources
		WHERE id IN (
			SELECT DISTINCT source_id FROM %smessages
			WHERE id IN (SELECT id FROM selected_messages)
		)`, dstQ, srcQ, srcQ))
	if err != nil && isSQLiteError(err, "no such column") {
		res, err = tx.Exec(fmt.Sprintf(`
			INSERT INTO %ssources
				(id, source_type, identifier, display_name, google_user_id,
				 last_sync_at, sync_cursor, sync_config, oauth_app,
				 created_at, updated_at)
			SELECT id, source_type, identifier, display_name, google_user_id,
			       last_sync_at, sync_cursor, sync_config, NULL,
			       created_at, updated_at
			FROM %ssources
			WHERE id IN (
				SELECT DISTINCT source_id FROM %smessages
				WHERE id IN (SELECT id FROM selected_messages)
			)`, dstQ, srcQ, srcQ))
	}
	if err != nil {
		return nil, fmt.Errorf("copy sources: %w", err)
	}
	if result.Sources, err = res.RowsAffected(); err != nil {
		return nil, fmt.Errorf("sources rows affected: %w", err)
	}

	if err := tx.QueryRow(
		"SELECT COUNT(*) FROM selected_messages",
	).Scan(&result.Messages); err != nil {
		return nil, fmt.Errorf("count selected messages: %w", err)
	}

	res, err = tx.Exec(fmt.Sprintf(`
		INSERT INTO %sconversations SELECT * FROM %sconversations
		WHERE id IN (
			SELECT DISTINCT conversation_id FROM %smessages
			WHERE id IN (SELECT id FROM selected_messages)
		)`, dstQ, srcQ, srcQ))
	if err != nil {
		return nil, fmt.Errorf("copy conversations: %w", err)
	}
	if result.Conversations, err = res.RowsAffected(); err != nil {
		return nil, fmt.Errorf("conversations rows affected: %w", err)
	}

	res, err = tx.Exec(fmt.Sprintf(`
		INSERT INTO %sparticipants SELECT * FROM %sparticipants
		WHERE id IN (
			SELECT sender_id FROM %smessages
			WHERE id IN (SELECT id FROM selected_messages)
			UNION
			SELECT participant_id FROM %smessage_recipients
			WHERE message_id IN (SELECT id FROM selected_messages)
			UNION
			SELECT participant_id FROM %sreactions
			WHERE message_id IN (SELECT id FROM selected_messages)
		)`, dstQ, srcQ, srcQ, srcQ, srcQ))
	if err != nil {
		return nil, fmt.Errorf("copy participants: %w", err)
	}
	if result.Participants, err = res.RowsAffected(); err != nil {
		return nil, fmt.Errorf("participants rows affected: %w", err)
	}

	if _, err := tx.Exec(fmt.Sprintf(`
		INSERT INTO %sparticipant_identifiers
		SELECT * FROM %sparticipant_identifiers
		WHERE participant_id IN (SELECT id FROM %sparticipants)`, dstQ, srcQ, dstQ)); err != nil {
		return nil, fmt.Errorf("copy participant_identifiers: %w", err)
	}

	if _, err := tx.Exec(fmt.Sprintf(`
		INSERT INTO %sconversation_participants
		SELECT * FROM %sconversation_participants
		WHERE conversation_id IN (SELECT id FROM %sconversations)
		  AND participant_id IN (SELECT id FROM %sparticipants)`, dstQ, srcQ, dstQ, dstQ)); err != nil {
		return nil, fmt.Errorf("copy conversation_participants: %w", err)
	}

	if _, err := tx.Exec(fmt.Sprintf(`
		INSERT INTO %smessages SELECT * FROM %smessages
		WHERE id IN (SELECT id FROM selected_messages)`, dstQ, srcQ)); err != nil {
		return nil, fmt.Errorf("copy messages: %w", err)
	}

	// Null out reply_to_message_id when the parent message wasn't
	// selected, to avoid FK violations from dangling references.
	if _, err := tx.Exec(fmt.Sprintf(`
		UPDATE %smessages SET reply_to_message_id = NULL
		WHERE reply_to_message_id IS NOT NULL
		  AND reply_to_message_id NOT IN (
			SELECT id FROM selected_messages
		)`, dstQ)); err != nil {
		return nil, fmt.Errorf("clear orphan reply refs: %w", err)
	}

	if _, err := tx.Exec(fmt.Sprintf(`
		INSERT INTO %smessage_bodies SELECT * FROM %smessage_bodies
		WHERE message_id IN (SELECT id FROM selected_messages)`, dstQ, srcQ)); err != nil {
		return nil, fmt.Errorf("copy message_bodies: %w", err)
	}

	if _, err := tx.Exec(fmt.Sprintf(`
		INSERT INTO %smessage_raw SELECT * FROM %smessage_raw
		WHERE message_id IN (SELECT id FROM selected_messages)`, dstQ, srcQ)); err != nil {
		return nil, fmt.Errorf("copy message_raw: %w", err)
	}

	if _, err := tx.Exec(fmt.Sprintf(`
		INSERT INTO %smessage_recipients
		SELECT * FROM %smessage_recipients
		WHERE message_id IN (SELECT id FROM selected_messages)`, dstQ, srcQ)); err != nil {
		return nil, fmt.Errorf("copy message_recipients: %w", err)
	}

	if _, err := tx.Exec(fmt.Sprintf(`
		INSERT INTO %sreactions SELECT * FROM %sreactions
		WHERE message_id IN (SELECT id FROM selected_messages)`, dstQ, srcQ)); err != nil {
		return nil, fmt.Errorf("copy reactions: %w", err)
	}

	attSelect, attInsertCols := "*", ""
	if attachmentCols != "" {
		attSelect = attachmentCols
		attInsertCols = " (" + attachmentCols + ")"
	}
	if _, err := tx.Exec(fmt.Sprintf(`
		INSERT INTO %sattachments%s SELECT %s FROM %sattachments
		WHERE message_id IN (SELECT id FROM selected_messages)`,
		dstQ, attInsertCols, attSelect, srcQ)); err != nil {
		return nil, fmt.Errorf("copy attachments: %w", err)
	}

	res, err = tx.Exec(fmt.Sprintf(`
		INSERT INTO %slabels SELECT * FROM %slabels
		WHERE source_id IN (SELECT id FROM %ssources)
		   OR id IN (
			SELECT label_id FROM %smessage_labels
			WHERE message_id IN (SELECT id FROM selected_messages)
		)`, dstQ, srcQ, dstQ, srcQ))
	if err != nil {
		return nil, fmt.Errorf("copy labels: %w", err)
	}
	if result.Labels, err = res.RowsAffected(); err != nil {
		return nil, fmt.Errorf("labels rows affected: %w", err)
	}

	if _, err := tx.Exec(fmt.Sprintf(`
		INSERT INTO %smessage_labels SELECT * FROM %smessage_labels
		WHERE message_id IN (SELECT id FROM selected_messages)
		  AND label_id IN (SELECT id FROM %slabels)`, dstQ, srcQ, dstQ)); err != nil {
		return nil, fmt.Errorf("copy message_labels: %w", err)
	}

	if _, err := tx.Exec(
		"DROP TABLE IF EXISTS selected_messages",
	); err != nil {
		return nil, fmt.Errorf("drop temp table: %w", err)
	}

	return result, nil
}

// updateConversationCounts updates the denormalized counts on
// conversations to be consistent with the copied subset.
func updateConversationCounts(db *sql.DB) error {
	_, err := db.Exec(`
		UPDATE conversations SET
			message_count = (
				SELECT COUNT(*) FROM messages
				WHERE conversation_id = conversations.id
			),
			participant_count = (
				SELECT COUNT(*) FROM conversation_participants
				WHERE conversation_id = conversations.id
			),
			last_message_at = (
				SELECT MAX(COALESCE(sent_at, received_at, internal_date))
				FROM messages
				WHERE conversation_id = conversations.id
			)`)
	return err
}

// populateFTS rebuilds the FTS5 index from the copied data.
func populateFTS(db *sql.DB) error {
	_, err := db.Exec(`
		INSERT OR REPLACE INTO messages_fts(
			rowid, message_id, subject, body,
			from_addr, to_addr, cc_addr
		)
		SELECT m.id, m.id, COALESCE(m.subject, ''),
			COALESCE(mb.body_text, ''),
			COALESCE(
				CASE WHEN m.message_type != 'email' AND m.message_type IS NOT NULL AND m.message_type != ''
				     THEN (SELECT COALESCE(p.phone_number, p.email_address) FROM participants p WHERE p.id = m.sender_id)
				END,
				(SELECT GROUP_CONCAT(p.email_address, ' ')
				 FROM message_recipients mr
				 JOIN participants p ON p.id = mr.participant_id
				 WHERE mr.message_id = m.id
				   AND mr.recipient_type = 'from'),
				''
			),
			COALESCE((
				SELECT GROUP_CONCAT(p.email_address, ' ')
				FROM message_recipients mr
				JOIN participants p ON p.id = mr.participant_id
				WHERE mr.message_id = m.id
				  AND mr.recipient_type = 'to'
			), ''),
			COALESCE((
				SELECT GROUP_CONCAT(p.email_address, ' ')
				FROM message_recipients mr
				JOIN participants p ON p.id = mr.participant_id
				WHERE mr.message_id = m.id
				  AND mr.recipient_type = 'cc'
			), '')
		FROM messages m
		LEFT JOIN message_bodies mb ON mb.message_id = m.id`)
	return err
}
