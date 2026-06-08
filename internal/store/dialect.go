package store

import (
	"context"
	"database/sql"
)

// FTSDoc is the set of fields the dialect needs to upsert a message into
// the full-text search index.
type FTSDoc struct {
	MessageID int64
	Subject   string
	Body      string
	FromAddr  string
	ToAddrs   string
	CcAddrs   string
}

// ColumnMigration is a single ALTER TABLE ADD COLUMN statement used by
// SQLiteDialect.LegacyColumnMigrations to evolve older SQLite databases.
type ColumnMigration struct {
	SQL  string // full ALTER TABLE ... ADD COLUMN statement
	Desc string // short label for error messages
}

// Dialect abstracts database-specific SQL generation and behavior.
// Implementations exist for SQLite (default) and PostgreSQL (opt-in).
type Dialect interface {
	// DriverName returns the database/sql driver name ("sqlite3" or "pgx").
	DriverName() string

	// Rebind converts a query with ? placeholders to the appropriate format
	// for the database driver. No-op for SQLite; converts to $1, $2, ... for PostgreSQL.
	Rebind(query string) string

	// Now returns the SQL expression for the current timestamp.
	// SQLite: "datetime('now')"  PostgreSQL: "NOW()"
	Now() string

	// InsertOrIgnore rewrites a complete INSERT statement to silently ignore conflicts.
	// SQLite: INSERT OR IGNORE INTO ...  PostgreSQL: INSERT INTO ... ON CONFLICT DO NOTHING
	// The input sql must be a complete statement in SQLite form
	// (starting with "INSERT OR IGNORE INTO"). For chunked inserts that
	// build the VALUES list incrementally, use InsertOrIgnorePrefix +
	// InsertOrIgnoreSuffix instead.
	InsertOrIgnore(sql string) string

	// InsertOrIgnorePrefix rewrites the prefix portion of a chunked
	// INSERT OR IGNORE whose VALUES tuples are appended separately.
	// The input must be a SQLite-form prefix ending in "VALUES ".
	// SQLite returns the prefix unchanged (OR IGNORE stays); PostgreSQL
	// strips "OR IGNORE" so conflict handling can come from the suffix.
	// Always pair this with InsertOrIgnoreSuffix at the end of the statement.
	InsertOrIgnorePrefix(sql string) string

	// InsertOrIgnoreSuffix returns a SQL suffix to append after VALUES for
	// conflict-ignoring inserts built incrementally (e.g., by insertInChunks).
	// SQLite: "" (OR IGNORE is in the prefix)
	// PostgreSQL: " ON CONFLICT DO NOTHING"
	InsertOrIgnoreSuffix() string

	// Full-text search is an OPTIONAL capability, not part of the core
	// Dialect: SQLite and PostgreSQL implement the FTSIndexer interface
	// (see fts.go), while MySQL/Dolt does not. Callers reach it via
	// Store.ftsIndexer() (a type assertion), so the common Dialect does not
	// force every backend to answer FTS questions it has no answer for.

	// LegacyColumnMigrations returns ALTER TABLE ADD COLUMN statements to
	// bring older databases up to date with schema columns added over time.
	// Both dialects return the same logical list, translated to the
	// dialect's column-type spellings. Statements are idempotent
	// (`IF NOT EXISTS` on PG; IsDuplicateColumnError silences re-runs on
	// SQLite). Fresh installs see no-op ALTERs because the columns are
	// already present in schema.sql / schema_pg.sql.
	LegacyColumnMigrations() []ColumnMigration

	// DatabaseSize returns the on-disk or logical size of the database in
	// bytes. For SQLite: file size at dbPath. For PostgreSQL: queries
	// pg_database_size(). Returns 0 if the size cannot be determined;
	// an error only for genuine failures (not missing files).
	DatabaseSize(db *sql.DB, dbPath string) (int64, error)

	// Connection lifecycle

	// InitConn performs driver-specific connection initialization.
	// Called after opening a connection. For SQLite: no-op (PRAGMAs are set via
	// DSN parameters). For PostgreSQL: SET search_path, statement_timeout, etc.
	InitConn(db *sql.DB) error

	// SchemaFiles returns the filenames of embedded schema files to execute during InitSchema.
	SchemaFiles() []string

	// CheckpointWAL checkpoints the WAL (SQLite) or is a no-op (PostgreSQL).
	CheckpointWAL(db *sql.DB) error

	// Schema migration

	// SchemaStaleCheck returns the SQL to check whether migrations are needed.
	SchemaStaleCheck() string

	// IsDuplicateColumnError returns true if the error indicates an ALTER TABLE
	// ADD COLUMN failed because the column already exists.
	IsDuplicateColumnError(err error) bool

	// Error handling

	// IsConflictError returns true if the error indicates a unique constraint violation.
	IsConflictError(err error) bool

	// IsNoSuchTableError returns true if the error indicates a missing table.
	IsNoSuchTableError(err error) bool

	// IsNoSuchModuleError returns true if the error indicates a missing module
	// (e.g., FTS5 not compiled in for SQLite). Always false for PostgreSQL.
	IsNoSuchModuleError(err error) bool

	// IsReturningError returns true if the error indicates RETURNING is not supported.
	// This handles SQLite < 3.35 which doesn't support RETURNING.
	// Always false for PostgreSQL (which always supports RETURNING).
	IsReturningError(err error) bool

	// IsBusyError returns true if the error indicates the database is held
	// by another connection, either busy (SQLITE_BUSY) or locked
	// (SQLITE_LOCKED). Used to surface actionable errors from maintenance
	// commands that need exclusive access.
	IsBusyError(err error) bool

	// BoolTrueExpr returns a SQL boolean expression that evaluates to true
	// when col holds a "true" value. SQLite stores booleans as 0/1 INTEGER
	// (emit "col = 1"); PostgreSQL has a real BOOLEAN type and rejects
	// integer comparisons against it, so the bare column name is correct.
	BoolTrueExpr(col string) string

	// JSONBindExpr returns the SQL fragment to use in place of a bare ?
	// when binding a Go string (or []byte) to a JSON column. SQLite has
	// no JSON type and stores JSON as plain TEXT, so the placeholder
	// stays bare. PostgreSQL's JSONB column does not implicitly cast
	// from text; without ?::JSONB the bind raises
	// "column is of type jsonb but expression is of type text".
	JSONBindExpr() string

	// BeginExclusive opens a transaction on conn that blocks concurrent
	// writers to the tables sync code touches (sync_runs in particular,
	// so StartSync's INSERT cannot run until COMMIT/ROLLBACK). Readers
	// may proceed.
	// SQLite: a single "BEGIN EXCLUSIVE" statement (WAL mode allows
	// concurrent reads while blocking writers).
	// PostgreSQL: "BEGIN" followed by LOCK TABLE sync_runs IN EXCLUSIVE
	// MODE, which conflicts with the ROW EXCLUSIVE lock INSERT acquires
	// but does not block ACCESS SHARE (reads).
	BeginExclusive(ctx context.Context, conn *sql.Conn) error

	// BeginWriteSQL returns the SQL to begin a transaction that
	// immediately acquires the write lock, so a read-modify-write under
	// concurrency cannot lose updates to a snapshot race.
	// SQLite: "BEGIN IMMEDIATE" (reserves the writer slot at BEGIN).
	// PostgreSQL: "BEGIN" — pair with SelectForUpdate to row-lock the
	// modified row inside the transaction.
	BeginWriteSQL() string

	// SelectForUpdate returns the row-lock clause to append to a SELECT
	// inside BeginWriteSQL transactions. PostgreSQL needs " FOR UPDATE"
	// to lock the matched row; SQLite already serializes writers under
	// BEGIN IMMEDIATE and returns "".
	SelectForUpdate() string
}
