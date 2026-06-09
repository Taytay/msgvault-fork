// Package query - PostgreSQL query engine construction.
//
// PostgreSQL support is provided by the dialect-parameterized SQLiteEngine
// (see sqlite.go). NewPostgreSQLEngine constructs an engine configured for
// PostgreSQL SQL (tsvector FTS, to_char time truncation, $N placeholders).
// The underlying engine implementation is the same struct used for SQLite.

package query

import (
	"database/sql"
	"errors"

	"go.kenn.io/msgvault/internal/store"
)

// ErrNotImplemented is a sentinel returned by engine methods that the current
// backend cannot satisfy. Handlers wrap their engine-capability checks around
// it so the API can return a stable status code rather than 500.
var ErrNotImplemented = errors.New("query: method not implemented for this engine")

// pgEngine wraps a dialect-parameterized engine for PostgreSQL. It
// embeds the Engine interface — not *SQLiteEngine — so the TextEngine
// methods (ListConversations, TextAggregate, TextSearch, …) defined on
// *SQLiteEngine are NOT promoted onto pgEngine. This is intentional:
// internal/query/sqlite_text.go uses FTS5 MATCH and strftime(), neither
// of which is valid PostgreSQL. Until a PostgreSQL TextEngine
// implementation exists, callers that type-assert the engine to
// query.TextEngine should cleanly get a failed assertion rather than
// silently sending SQLite SQL to PostgreSQL at runtime.
type pgEngine struct {
	Engine
}

// NewPostgreSQLEngine creates a query engine backed by PostgreSQL. The engine
// uses PostgreSQLQueryDialect for all SQL generation: $N placeholders via
// Rebind, to_char() time truncation, tsvector @@ for full-text search.
//
// The returned value is the Engine interface (not the concrete
// *SQLiteEngine) so the SQLite-specific TextEngine implementation is
// hidden from type assertions on the PG path.
func NewPostgreSQLEngine(db *sql.DB) Engine {
	return &pgEngine{Engine: NewEngineWithDialect(db, PostgreSQLQueryDialect{})}
}

// doltEngine wraps a dialect-parameterized engine for Dolt (MySQL wire
// protocol). Like pgEngine it embeds the Engine interface — not *SQLiteEngine —
// so the SQLite-only TextEngine methods (FTS5 MATCH + strftime) are not
// promoted; keyword/semantic search on Dolt is served by the doltvec backend,
// and a type assertion to query.TextEngine cleanly fails here.
type doltEngine struct {
	Engine
}

// NewDoltEngine creates a query engine backed by Dolt. It uses
// MySQLQueryDialect (? placeholders, DATE_FORMAT time truncation, LIKE-based
// free-text fallback) so aggregates, listing, and stats run directly against
// the Dolt system of record — no SQLite replica or Parquet cache, mirroring how
// PostgreSQL is served.
func NewDoltEngine(db *sql.DB) Engine {
	return &doltEngine{Engine: NewEngineWithDialect(db, MySQLQueryDialect{})}
}

// NewEngineForStore builds the direct query engine for a Store, selecting the
// SQL dialect from the store's backend. This is the single place engine
// selection consults backend identity; callers ask the store for an engine
// rather than re-deriving the dialect (or threading a backend boolean) at each
// call site. The return type is the Engine interface so the SQLite-only
// TextEngine is hidden on the PostgreSQL and Dolt paths.
func NewEngineForStore(s *store.Store) Engine {
	switch s.Backend() {
	case store.BackendPostgreSQL:
		return NewPostgreSQLEngine(s.DB())
	case store.BackendDolt:
		return NewDoltEngine(s.DB())
	default:
		return NewSQLiteEngine(s.DB())
	}
}
