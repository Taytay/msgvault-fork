// Package projection materializes msgvault's local SQLite read-replica from a
// Dolt (MySQL-wire) system of record.
//
// Under the Dolt architecture, Dolt is the durable, versioned, syncable store,
// while the fast read/search/analytics stack (DuckDB/Parquet, FTS5 keyword
// search, sqlite-vec semantic search) runs against a local SQLite replica that
// is rebuilt from Dolt. Project copies the base tables Dolt -> SQLite and
// backfills the FTS5 index; callers then run build-cache (Parquet) and the
// embedding worker against the replica, all unchanged.
package projection

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"go.kenn.io/msgvault/internal/store"
)

// tableLoadOrder lists the base tables in foreign-key dependency order. Inserts
// run with foreign_keys=OFF (see Project) so order is not strictly required, but
// keeping it dependency-ordered keeps the copy readable and robust if that ever
// changes.
var tableLoadOrder = []string{
	"sources",
	"participants",
	"participant_identifiers",
	"conversations",
	"conversation_participants",
	"messages",
	"message_recipients",
	"reactions",
	"attachments",
	"labels",
	"message_labels",
	"message_bodies",
	"message_raw",
	"sync_runs",
	"sync_checkpoints",
	"source_import_items",
	"collections",
	"collection_sources",
	"account_identities",
	"applied_migrations",
}

// Report summarizes a projection run.
type Report struct {
	Rows       map[string]int64 // rows copied per table
	FTSIndexed int64            // messages indexed into FTS5
}

// Project rebuilds the local SQLite read-replica dst from the Dolt system of
// record src. dst must already be schema-initialized (store.InitSchema). It
// clears dst's base tables, copies every row from src, then backfills FTS5.
//
// The whole copy runs on a single dst connection with foreign_keys=OFF so
// self-referential rows (messages.reply_to_message_id) and any insertion order
// load cleanly; the SQLite store's own connections keep foreign_keys=ON.
func Project(ctx context.Context, src, dst *store.Store) (*Report, error) {
	conn, err := dst.DB().Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire replica connection: %w", err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.ExecContext(ctx, "PRAGMA foreign_keys=OFF"); err != nil {
		return nil, fmt.Errorf("disable foreign keys: %w", err)
	}

	// Clear InitSchema-seeded rows (e.g. the default collection) so the copy is
	// authoritative and the operation is idempotent / re-runnable.
	for _, table := range tableLoadOrder {
		if _, err := conn.ExecContext(ctx, "DELETE FROM `"+table+"`"); err != nil {
			return nil, fmt.Errorf("clear %s: %w", table, err)
		}
	}

	rep := &Report{Rows: make(map[string]int64, len(tableLoadOrder))}
	for _, table := range tableLoadOrder {
		n, err := copyTable(ctx, src.DB(), conn, table)
		if err != nil {
			return nil, fmt.Errorf("project %s: %w", table, err)
		}
		rep.Rows[table] = n
	}

	indexed, err := dst.BackfillFTS(nil)
	if err != nil {
		return nil, fmt.Errorf("backfill FTS: %w", err)
	}
	rep.FTSIndexed = indexed
	return rep, nil
}

// copyTable copies every row of one table from src (Dolt) to the dst replica
// connection. The column set comes from the replica schema, so MySQL-only
// columns (e.g. the generated dedup_content_hash) are naturally excluded. Rows
// load in a single transaction for speed.
func copyTable(ctx context.Context, src *sql.DB, dst *sql.Conn, table string) (int64, error) {
	cols, err := replicaColumns(ctx, dst, table)
	if err != nil {
		return 0, err
	}
	if len(cols) == 0 {
		return 0, fmt.Errorf("no columns found for table %q in replica", table)
	}

	colList := quoteJoin(cols)
	srcRows, err := src.QueryContext(ctx, "SELECT "+colList+" FROM `"+table+"`")
	if err != nil {
		return 0, fmt.Errorf("read from source: %w", err)
	}
	defer func() { _ = srcRows.Close() }()

	tx, err := dst.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin replica tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // no-op once committed

	placeholders := "(" + strings.Repeat("?,", len(cols)-1) + "?)"
	stmt, err := tx.PrepareContext(ctx, "INSERT INTO `"+table+"` ("+colList+") VALUES "+placeholders)
	if err != nil {
		return 0, fmt.Errorf("prepare insert: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}

	var count int64
	for srcRows.Next() {
		if err := srcRows.Scan(ptrs...); err != nil {
			return 0, fmt.Errorf("scan source row: %w", err)
		}
		for i := range vals {
			// The MySQL driver returns text/JSON columns as []byte. Inserting
			// []byte into SQLite stores a BLOB, which would corrupt TEXT columns
			// (search/scan then misbehaves), so convert to string — except for
			// genuinely binary columns (message_raw.raw_data stays a BLOB).
			if b, ok := vals[i].([]byte); ok && !isBinaryColumn(table, cols[i]) {
				vals[i] = string(b)
			}
		}
		if _, err := stmt.ExecContext(ctx, vals...); err != nil {
			return 0, fmt.Errorf("insert into replica: %w", err)
		}
		count++
	}
	if err := srcRows.Err(); err != nil {
		return 0, fmt.Errorf("iterate source rows: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit replica tx: %w", err)
	}
	return count, nil
}

// replicaColumns returns the column names of a table in the SQLite replica, in
// schema order, via PRAGMA table_info. table is from the fixed tableLoadOrder
// allowlist, so interpolating it is safe.
func replicaColumns(ctx context.Context, dst *sql.Conn, table string) ([]string, error) {
	rows, err := dst.QueryContext(ctx, "PRAGMA table_info(`"+table+"`)")
	if err != nil {
		return nil, fmt.Errorf("read replica columns: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var cols []string
	for rows.Next() {
		var (
			cid       int
			name      string
			ctype     string
			notNull   int
			dfltValue sql.NullString
			pk        int
		)
		if err := rows.Scan(&cid, &name, &ctype, &notNull, &dfltValue, &pk); err != nil {
			return nil, fmt.Errorf("scan column info: %w", err)
		}
		cols = append(cols, name)
	}
	return cols, rows.Err()
}

// isBinaryColumn reports whether a column holds binary data that must stay
// []byte rather than being coerced to a string during the copy.
func isBinaryColumn(table, col string) bool {
	return table == "message_raw" && col == "raw_data"
}

// quoteJoin backtick-quotes and comma-joins identifiers. Backticks are accepted
// by both Dolt (MySQL) and SQLite, so the same column list works on both sides.
func quoteJoin(idents []string) string {
	quoted := make([]string, len(idents))
	for i, id := range idents {
		quoted[i] = "`" + id + "`"
	}
	return strings.Join(quoted, ", ")
}
