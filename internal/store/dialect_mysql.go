package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/go-sql-driver/mysql"
)

// MySQLDialect implements Dialect for MySQL-wire-protocol backends, which in
// msgvault means Dolt (https://github.com/dolthub/dolt). Dolt is the durable,
// versioned, remotely-syncable *system of record*; full-text and semantic
// search are served by a local SQLite read-replica rebuilt from Dolt (see
// docs/research/msgvault-dolt-architecture-plan.md). Consequently this dialect
// does NOT implement the optional FTSIndexer capability (fts.go), so the FTS
// code paths are skipped entirely on Dolt rather than dispatched to no-ops.
//
// MySQL/Dolt SQL-compatibility notes (verified against Dolt 2.1.4):
//   - `?` is the native placeholder, so Rebind is the identity function.
//   - INSERT ... RETURNING IS supported by Dolt (unlike stock MySQL), so
//     IsReturningError returns false and the native RETURNING path is used.
//   - Upsert uses INSERT IGNORE / ON DUPLICATE KEY UPDATE rather than
//     SQLite's INSERT OR IGNORE or PG's ON CONFLICT.
//   - MySQL UNIQUE indexes permit multiple NULLs, which is why the PG/SQLite
//     partial unique indexes (WHERE col IS NOT NULL) map to plain UNIQUE keys
//     in schema_mysql.sql.
type MySQLDialect struct{}

func (d *MySQLDialect) DriverName() string { return "mysql" }

// Rebind is a no-op: MySQL uses `?` placeholders natively.
func (d *MySQLDialect) Rebind(query string) string { return query }

// Upsert-translation regexes. msgvault writes upserts once in the canonical
// SQLite/PostgreSQL form (ON CONFLICT (...) DO UPDATE SET x = excluded.x /
// DO NOTHING). RewriteUpsert rewrites that form to MySQL/Dolt syntax so the
// ~12 inline upsert call sites stay backend-agnostic. The two backends already
// share the ON CONFLICT spelling, so only MySQL needs the rewrite.
var (
	// "ON CONFLICT [(target)] [WHERE pred] DO UPDATE SET" -> "ON DUPLICATE KEY UPDATE".
	// The conflict target and any partial-index WHERE predicate are dropped:
	// MySQL fires ON DUPLICATE KEY UPDATE on any unique-key violation. Non-greedy
	// so it stops at the first DO UPDATE SET.
	reOnConflictDoUpdate = regexp.MustCompile(`(?is)\bON\s+CONFLICT\b.*?\bDO\s+UPDATE\s+SET\b`)
	// "ON CONFLICT [...] DO NOTHING" -> "" (paired with the INSERT IGNORE rewrite).
	reOnConflictDoNothing = regexp.MustCompile(`(?is)\bON\s+CONFLICT\b.*?\bDO\s+NOTHING\b`)
	// excluded.col / EXCLUDED.col -> VALUES(col) (the proposed-row reference).
	reExcludedRef = regexp.MustCompile(`(?i)\bexcluded\.([a-zA-Z_][a-zA-Z0-9_]*)`)
	// Leading INSERT INTO -> INSERT IGNORE INTO (for the DO NOTHING path).
	reLeadingInsert = regexp.MustCompile(`(?i)^(\s*)INSERT\s+INTO\b`)
)

// RewriteUpsert converts a canonical ON CONFLICT upsert into MySQL/Dolt syntax.
// It is a no-op for any statement without an ON CONFLICT clause (the common
// case), so it is safe to run over every statement. openDolt composes this with
// Rebind in the loggedDB rewrite hook, so all package-internal upserts are
// translated transparently — no call site changes.
//
//   - ON CONFLICT (...) DO UPDATE SET a = excluded.a
//     -> ON DUPLICATE KEY UPDATE a = VALUES(a)
//   - ON CONFLICT (...) [WHERE ...] DO NOTHING
//     -> INSERT IGNORE INTO ... (clause stripped)
//
// Existing-row references that PG/SQLite write as table-qualified columns
// (e.g. conversations.title) are left intact — in MySQL's ON DUPLICATE KEY
// UPDATE a qualified/bare column already denotes the current row's value.
// RETURNING is preserved (Dolt supports INSERT ... ON DUPLICATE KEY UPDATE ...
// RETURNING).
func (d *MySQLDialect) RewriteUpsert(query string) string {
	if !strings.Contains(query, "ON CONFLICT") {
		return query
	}
	if reOnConflictDoNothing.MatchString(query) {
		q := reOnConflictDoNothing.ReplaceAllString(query, "")
		return reLeadingInsert.ReplaceAllString(q, "${1}INSERT IGNORE INTO")
	}
	q := reOnConflictDoUpdate.ReplaceAllString(query, "ON DUPLICATE KEY UPDATE")
	return reExcludedRef.ReplaceAllString(q, "VALUES($1)")
}

// RewriteLikeEscape doubles the backslash in a canonical LIKE ... ESCAPE '\'
// clause. SQLite and PostgreSQL read '\' in a string literal as a single
// backslash; MySQL/Dolt process backslash escapes, so '\' parses as an escaped
// quote and the literal must become ESCAPE '\\'. No-op for statements without
// the clause, so it is safe to run over every statement (like RewriteUpsert).
func (d *MySQLDialect) RewriteLikeEscape(query string) string {
	return strings.ReplaceAll(query, `ESCAPE '\'`, `ESCAPE '\\'`)
}

// Now returns the MySQL expression for the current timestamp.
func (d *MySQLDialect) Now() string { return "NOW()" }

// BoolTrueExpr compares against 1 — MySQL stores BOOLEAN as TINYINT(1).
func (d *MySQLDialect) BoolTrueExpr(col string) string { return col + " = 1" }

// RandomFunc returns MySQL/Dolt's random function (RAND(), not RANDOM()).
func (d *MySQLDialect) RandomFunc() string { return "RAND()" }

// JSONBindExpr returns a bare placeholder. MySQL/Dolt accept a JSON string
// literal bound to a JSON column directly.
func (d *MySQLDialect) JSONBindExpr() string { return "?" }

// Note: MySQLDialect intentionally does NOT implement the FTSIndexer
// capability (fts.go). Under the Dolt architecture, keyword search is served
// by the local SQLite replica, so Store.ftsIndexer() returns (nil, false) on
// Dolt and all FTS code paths are skipped — no defensive no-op methods needed.

// InsertOrIgnore rewrites SQLite's "INSERT OR IGNORE INTO" to MySQL's
// "INSERT IGNORE INTO". No trailing conflict clause is needed — IGNORE lives
// in the verb itself, mirroring the SQLite shape.
func (d *MySQLDialect) InsertOrIgnore(sqlStr string) string {
	return strings.Replace(sqlStr, "INSERT OR IGNORE INTO", "INSERT IGNORE INTO", 1)
}

// InsertOrIgnorePrefix keeps IGNORE in the verb for chunked inserts whose
// VALUES tuples are appended separately; the suffix is therefore empty.
func (d *MySQLDialect) InsertOrIgnorePrefix(sqlStr string) string {
	return strings.Replace(sqlStr, "INSERT OR IGNORE INTO", "INSERT IGNORE INTO", 1)
}

// InsertOrIgnoreSuffix returns "" — MySQL carries IGNORE in the prefix.
func (d *MySQLDialect) InsertOrIgnoreSuffix() string { return "" }

// LegacyColumnMigrations returns nil: schema_mysql.sql is always complete, so
// fresh Dolt databases need no ALTER TABLE catch-up (same stance as PG, which
// keeps schema_pg.sql complete).
func (d *MySQLDialect) LegacyColumnMigrations() []ColumnMigration { return nil }

// UsesPartialIndexMigrations is false: MySQL/Dolt declares the equivalent
// UNIQUE keys inline in schema_mysql.sql and cannot express partial indexes.
func (d *MySQLDialect) UsesPartialIndexMigrations() bool { return false }

// DatabaseSize sums data + index length for the current schema.
func (d *MySQLDialect) DatabaseSize(db *sql.DB, _ string) (int64, error) {
	var size sql.NullInt64
	err := db.QueryRow(
		`SELECT COALESCE(SUM(data_length + index_length), 0)
		   FROM information_schema.tables
		  WHERE table_schema = DATABASE()`,
	).Scan(&size)
	if err != nil {
		return 0, fmt.Errorf("information_schema size: %w", err)
	}
	return size.Int64, nil
}

// InitConn is a no-op: per-connection settings (parseTime, etc.) are applied
// via the DSN at open time.
func (d *MySQLDialect) InitConn(db *sql.DB) error { return nil }

// SchemaFiles returns the single MySQL schema file.
func (d *MySQLDialect) SchemaFiles() []string { return []string{"schema_mysql.sql"} }

// CheckpointWAL is a no-op (MySQL/Dolt has no WAL checkpoint).
func (d *MySQLDialect) CheckpointWAL(db *sql.DB) error { return nil }

// SchemaStaleCheck probes for a recent column via information_schema.
func (d *MySQLDialect) SchemaStaleCheck() string {
	return mysqlColumnExistsSQL("conversations", "conversation_type")
}

// IsDuplicateColumnError reports MySQL error 1060 (ER_DUP_FIELDNAME).
func (d *MySQLDialect) IsDuplicateColumnError(err error) bool {
	return isMySQLError(err, 1060)
}

// IsConflictError reports a unique-constraint conflict. Two shapes occur on
// Dolt: the standard insert-time duplicate (1062), and a commit-time
// constraint violation from Dolt's optimistic transaction model — concurrent
// inserts of the same unique key each succeed locally and the violation
// surfaces only when the working sets merge at COMMIT. Both are retryable.
func (d *MySQLDialect) IsConflictError(err error) bool {
	return isMySQLError(err, 1062) || isDoltConstraintViolation(err)
}

// isDoltConstraintViolation matches Dolt's commit-time constraint-violation
// error, a generic (1105) error whose message names the violation (e.g.
// "Committing this transaction resulted in a working set with constraint
// violations ... Unique Key Constraint Violation"). Retrying the idempotent
// upsert resolves it: on the retry the conflicting row is committed and visible,
// so ON CONFLICT collapses to it.
func isDoltConstraintViolation(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "constraint violation")
}

// IsNoSuchTableError reports MySQL error 1146 (ER_NO_SUCH_TABLE).
func (d *MySQLDialect) IsNoSuchTableError(err error) bool {
	return isMySQLError(err, 1146)
}

// IsNoSuchModuleError is always false (no module concept in MySQL).
func (d *MySQLDialect) IsNoSuchModuleError(err error) bool { return false }

// IsReturningError is false: Dolt supports INSERT ... RETURNING.
func (d *MySQLDialect) IsReturningError(err error) bool { return false }

// IsBusyError reports lock-wait timeout (1205) or deadlock (1213).
func (d *MySQLDialect) IsBusyError(err error) bool {
	return isMySQLError(err, 1205) || isMySQLError(err, 1213)
}

// BeginExclusive opens a transaction for serialized writes. MySQL/Dolt have no
// direct analogue of SQLite's database-wide BEGIN EXCLUSIVE; this starts a
// transaction and relies on row/gap locks (and the caller's SelectForUpdate)
// to serialize the sync write path. Under the recommended Dolt architecture
// the live write path runs against the SQLite store, so this is exercised only
// by version/admin operations.
func (d *MySQLDialect) BeginExclusive(ctx context.Context, conn *sql.Conn) error {
	_, err := conn.ExecContext(ctx, "START TRANSACTION")
	return err
}

// BeginWriteSQL begins a transaction; pair with SelectForUpdate to row-lock.
func (d *MySQLDialect) BeginWriteSQL() string { return "START TRANSACTION" }

// SelectForUpdate row-locks matched rows inside a write transaction.
func (d *MySQLDialect) SelectForUpdate() string { return " FOR UPDATE" }

// isMySQLError reports whether err is a *mysql.MySQLError with the given code.
func isMySQLError(err error, number uint16) bool {
	var myErr *mysql.MySQLError
	if errors.As(err, &myErr) {
		return myErr.Number == number
	}
	return false
}

func mysqlColumnExistsSQL(tableName, columnName string) string {
	return fmt.Sprintf(`SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = DATABASE()
		  AND table_name = '%s'
		  AND column_name = '%s'`, tableName, columnName)
}
