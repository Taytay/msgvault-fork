package store

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"time"
)

// doltSubsetExporter copies a subset into a brand-new database on the same Dolt
// server. dest is the new database name.
type doltSubsetExporter struct{ src *Store }

func (e doltSubsetExporter) DestinationHint() string {
	return "a name for a new database on the Dolt server"
}

func (e doltSubsetExporter) ExportSubset(ctx context.Context, rowCount int, dest string) (*CopyResult, error) {
	return e.src.exportSubsetDolt(ctx, rowCount, dest)
}

// validDoltDBName guards a database name that is interpolated unescaped into
// CREATE DATABASE and <db>.<table> references. Restrict to identifier
// characters so it can never carry SQL.
func validDoltDBName(name string) bool {
	if name == "" {
		return false
	}
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_':
		default:
			return false
		}
	}
	return true
}

// exportSubsetDolt creates a new database `dest` on the same Dolt server and
// copies the rowCount most recent messages (and everything they reference) into
// it via cross-database INSERT…SELECT. Mirrors the SQLite CopySubset, using the
// mechanisms verified against live Dolt: schema init through the normal store
// path, FOREIGN_KEY_CHECKS=0 for order-independent bulk copy (Dolt enforces FKs
// inline, unlike SQLite's deferred check), and a single transaction whose
// cross-db writes commit atomically.
func (s *Store) exportSubsetDolt(ctx context.Context, rowCount int, dest string) (*CopyResult, error) {
	if rowCount <= 0 {
		return nil, fmt.Errorf("rowCount must be positive, got %d", rowCount)
	}
	if !validDoltDBName(dest) {
		return nil, fmt.Errorf("invalid destination database name %q: use letters, digits, and underscore only", dest)
	}
	start := time.Now()

	var srcDB string
	if err := s.DB().QueryRowContext(ctx, "SELECT DATABASE()").Scan(&srcDB); err != nil {
		return nil, fmt.Errorf("resolve source database: %w", err)
	}
	if !validDoltDBName(srcDB) {
		return nil, fmt.Errorf("unexpected source database name %q", srcDB)
	}

	// Create the destination database and initialize its schema through the
	// normal store path (schema_mysql.sql).
	if _, err := s.DB().ExecContext(ctx, "CREATE DATABASE "+dest); err != nil {
		return nil, fmt.Errorf("create destination database %q (it may already exist): %w", dest, err)
	}
	destDSN, err := withDatabase(s.dbPath, dest)
	if err != nil {
		return nil, err
	}
	destStore, err := Open(destDSN)
	if err != nil {
		return nil, fmt.Errorf("open destination database: %w", err)
	}
	if err := destStore.InitSchema(); err != nil {
		_ = destStore.Close()
		return nil, fmt.Errorf("initialize destination schema: %w", err)
	}
	_ = destStore.Close()

	// Run the copy on a dedicated connection so FOREIGN_KEY_CHECKS=0 is scoped
	// to it and restored before the connection returns to the pool. The
	// cross-db writes commit atomically (verified against live Dolt).
	conn, err := s.DB().Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire connection: %w", err)
	}
	defer func() {
		_, _ = conn.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS=1")
		_ = conn.Close()
	}()
	if _, err := conn.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS=0"); err != nil {
		return nil, fmt.Errorf("disable foreign key checks: %w", err)
	}

	// attachments has a STORED generated column (dedup_content_hash, see
	// schema_mysql.sql) that cannot be written. Copy every other column by
	// listing the writable ones explicitly. Dolt's information_schema does not
	// flag the column as generated (and tags created_at as DEFAULT_GENERATED),
	// so it can't be detected automatically — exclude it by name.
	attCols, err := doltInsertableColumns(ctx, conn, srcDB, "attachments", "dedup_content_hash")
	if err != nil {
		return nil, err
	}

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}
	result, err := copyData(tx, srcDB+".", dest+".", attCols, rowCount)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit subset copy: %w", err)
	}

	// Post-copy fixups run against the destination database. Unlike SQLite
	// there is no FTS5 index to populate (Dolt keyword search is served by
	// doltvec) and no PRAGMA foreign_key_check (FKs were enforced per-insert
	// once re-enabled).
	if err := s.finalizeDoltSubset(ctx, destDSN, dest, result); err != nil {
		return nil, err
	}
	result.Elapsed = time.Since(start)
	return result, nil
}

// finalizeDoltSubset refreshes denormalized conversation counts and records the
// destination database size, using a connection whose default database is dest.
func (s *Store) finalizeDoltSubset(ctx context.Context, destDSN, dest string, result *CopyResult) error {
	destStore, err := Open(destDSN)
	if err != nil {
		return fmt.Errorf("reopen destination database: %w", err)
	}
	defer func() { _ = destStore.Close() }()

	if err := updateConversationCounts(destStore.DB()); err != nil {
		return fmt.Errorf("update conversation counts: %w", err)
	}

	var size int64
	_ = destStore.DB().QueryRowContext(ctx,
		"SELECT COALESCE(SUM(data_length + index_length), 0) FROM information_schema.tables WHERE table_schema = ?",
		dest).Scan(&size)
	result.DBSize = size
	return nil
}

// doltInsertableColumns returns the comma-separated columns of a table in
// ordinal order, omitting the named generated columns. Generated columns can't
// be written, so they must be excluded from an INSERT … SELECT. They are passed
// by name because Dolt's information_schema does not reliably flag generated
// columns (and labels ordinary DEFAULT columns as DEFAULT_GENERATED). Listing
// every other column keeps the copy resilient to future schema additions.
func doltInsertableColumns(ctx context.Context, conn *sql.Conn, db, table string, exclude ...string) (string, error) {
	skip := make(map[string]bool, len(exclude))
	for _, c := range exclude {
		skip[c] = true
	}
	rows, err := conn.QueryContext(ctx, `
		SELECT column_name FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ?
		ORDER BY ordinal_position`, db, table)
	if err != nil {
		return "", fmt.Errorf("read %s columns: %w", table, err)
	}
	defer func() { _ = rows.Close() }()
	var cols []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return "", fmt.Errorf("scan %s column: %w", table, err)
		}
		if !skip[c] {
			cols = append(cols, c)
		}
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if len(cols) == 0 {
		return "", fmt.Errorf("no writable columns found for %s.%s", db, table)
	}
	return strings.Join(cols, ", "), nil
}

// withDatabase returns a copy of a mysql:// / dolt:// DSN pointing at a
// different database name on the same server.
func withDatabase(dsn, db string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("parse source DSN: %w", err)
	}
	u.Path = "/" + db
	return u.String(), nil
}
