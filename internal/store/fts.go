package store

import "database/sql"

// FTSIndexer is the optional full-text-search capability a backend's dialect
// exposes. SQLite (FTS5) and PostgreSQL (tsvector) implement it; the
// MySQL/Dolt dialect does not — under the Dolt architecture, keyword search is
// served by the local SQLite replica, so the Dolt store reports no FTS
// capability and all FTS code paths become clean no-ops.
//
// This mirrors VersionController: a focused capability discovered via type
// assertion (Store.ftsIndexer) rather than a fat set of methods every Dialect
// must answer — which previously forced the MySQL dialect to carry ~10
// defensive no-op stubs.
type FTSIndexer interface {
	// FTSUpsert inserts or updates the search index for a single message.
	FTSUpsert(q querier, doc FTSDoc) error
	// FTSSearchClause returns SQL fragments (join, where, orderBy) for a
	// full-text query using ? placeholders, plus how many times the search
	// term must be re-bound for the orderBy.
	FTSSearchClause() (join, where, orderBy string, orderArgCount int)
	// FTSDeleteSQL removes FTS entries for a source (one ? = source_id).
	FTSDeleteSQL() string
	// FTSBackfillBatchSQL populates the index for an id range (?from, ?to).
	FTSBackfillBatchSQL() string
	// FTSAvailable reports whether FTS is usable at runtime (e.g. FTS5
	// compiled in / the tsvector column present).
	FTSAvailable(db *sql.DB) bool
	// FTSNeedsBackfill reports whether the index needs populating.
	FTSNeedsBackfill(db *sql.DB) bool
	// FTSClearSQL clears all FTS data before a full backfill.
	FTSClearSQL() string
	// SchemaFTS returns the embedded filename of separate FTS DDL, or "".
	SchemaFTS() string
	// FTSRebuildSchema tears down and recreates the FTS infrastructure.
	FTSRebuildSchema(db *sql.DB) error
	// BuildFTSArg formats user search terms into the single bound argument
	// FTSSearchClause's WHERE expects ("" when nothing usable survives).
	BuildFTSArg(terms []string) string
}

// Compile-time checks: the SQL backends that support full-text search.
var (
	_ FTSIndexer = (*SQLiteDialect)(nil)
	_ FTSIndexer = (*PostgreSQLDialect)(nil)
)

// ftsIndexer returns the dialect's full-text-search capability, or
// (nil, false) when the backend does not support FTS (MySQL/Dolt).
func (s *Store) ftsIndexer() (FTSIndexer, bool) {
	idx, ok := s.dialect.(FTSIndexer)
	return idx, ok
}
