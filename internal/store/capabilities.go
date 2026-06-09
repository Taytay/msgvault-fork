package store

import (
	"context"
	"fmt"
	"strings"
)

// This file collects the optional capabilities a backend may expose, alongside
// FTSIndexer (fts.go) and VersionController (version.go). The pattern is
// uniform: a backend advertises a capability by having the relevant Store
// accessor return a non-nil implementation and true; backends that lack it
// return (nil, false). Callers ask "can you do X?" — they never branch on
// "what backend is this?". Adding a new backend means implementing the
// capabilities it can support; no call site changes.
//
// The three capabilities below all coincide with the single-file SQLite
// backend (a local file you can run PRAGMAs against, VACUUM INTO a copy of,
// and feed to the DuckDB/Parquet ETL). They are nonetheless modeled as
// distinct, behavior-named interfaces so a future backend could implement one
// without the others.

// AnalyticsCache is exposed by backends whose system of record is a local file
// that the Parquet/DuckDB analytics ETL can read. PostgreSQL and Dolt (the
// client/server backends) do not expose it; their callers query the dialect
// engine directly. Under the Dolt architecture, analytics run against the
// local SQLite replica, which is itself a SQLite store that DOES expose this.
type AnalyticsCache interface {
	// SourcePath returns the local database file path the DuckDB/Parquet ETL
	// reads from.
	SourcePath() string
}

// IntegrityChecker is exposed by backends with an in-engine integrity check
// (SQLite's PRAGMA integrity_check). Client/server backends rely on external
// admin tooling (pg_amcheck, Dolt verification) and do not expose it.
type IntegrityChecker interface {
	// CheckIntegrity runs the backend's integrity check and returns the list of
	// problems found (empty when healthy).
	CheckIntegrity(ctx context.Context) ([]string, error)
}

// SnapshotBackup is exposed by backends that can snapshot themselves to a
// single destination file (SQLite's VACUUM INTO). Client/server backups go
// through server-side tooling (pg_dump, Dolt) and are not exposed here.
type SnapshotBackup interface {
	// BackupTo writes a consistent snapshot of the database to dst.
	BackupTo(ctx context.Context, dst string) error
}

// AnalyticsCache returns the analytics-cache capability and true for local-file
// backends; (nil, false) otherwise.
func (s *Store) AnalyticsCache() (AnalyticsCache, bool) {
	if lf, ok := s.localFile(); ok {
		return lf, true
	}
	return nil, false
}

// IntegrityChecker returns the integrity-check capability and true for
// local-file backends; (nil, false) otherwise.
func (s *Store) IntegrityChecker() (IntegrityChecker, bool) {
	if lf, ok := s.localFile(); ok {
		return lf, true
	}
	return nil, false
}

// SnapshotBackup returns the snapshot-backup capability and true for local-file
// backends; (nil, false) otherwise.
func (s *Store) SnapshotBackup() (SnapshotBackup, bool) {
	if lf, ok := s.localFile(); ok {
		return lf, true
	}
	return nil, false
}

// RequireAnalyticsCache returns the analytics-cache capability, or an error
// explaining why the active backend cannot provide it. The remediation hint is
// authored here — the single place that knows each backend — so refusing
// commands do not branch on backend identity to build their message.
func (s *Store) RequireAnalyticsCache() (AnalyticsCache, error) {
	if c, ok := s.AnalyticsCache(); ok {
		return c, nil
	}
	return nil, &UnsupportedError{
		Feature: "the Parquet analytics cache",
		Backend: s.Backend().String(),
		Hint:    "this backend is queried directly and needs no analytics cache",
	}
}

// RequireSnapshotBackup returns the snapshot-backup capability, or an error
// naming the backend that lacks it. Like RequireAnalyticsCache, the message is
// authored here so callers need not branch on backend identity.
func (s *Store) RequireSnapshotBackup() (SnapshotBackup, error) {
	if b, ok := s.SnapshotBackup(); ok {
		return b, nil
	}
	return nil, &UnsupportedError{
		Feature: "file-snapshot backup",
		Backend: s.Backend().String(),
		Hint:    "snapshot the database with the backend's native tooling out-of-band",
	}
}

// UnsupportedError reports that the active backend does not provide a
// capability a command requires. Hint carries optional backend-authored
// remediation advice.
type UnsupportedError struct {
	Feature string
	Backend string
	Hint    string
}

func (e *UnsupportedError) Error() string {
	msg := fmt.Sprintf("%s is not available on the %s backend", e.Feature, e.Backend)
	if e.Hint != "" {
		msg += "; " + e.Hint
	}
	return msg
}

// localFile bundles the capabilities of a single-file SQLite store. Because the
// local-file capabilities all coincide with SQLite, one type implements them
// and the accessors gate on the same condition; the capability interfaces keep
// callers from depending on that coincidence.
type localFile struct {
	db   *loggedDB
	path string
}

// localFile reports whether this store is a single local SQLite file and, if
// so, returns the bound capability implementation. In-memory databases are
// excluded: there is no file for the ETL or VACUUM INTO to operate on.
func (s *Store) localFile() (localFile, bool) {
	if s.Backend() == BackendSQLite && s.dbPath != "" && !strings.Contains(s.dbPath, ":memory:") {
		return localFile{db: s.db, path: s.dbPath}, true
	}
	return localFile{}, false
}

func (f localFile) SourcePath() string { return f.path }

func (f localFile) CheckIntegrity(ctx context.Context) ([]string, error) {
	rows, err := f.db.QueryContext(ctx, "PRAGMA integrity_check(100)")
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var problems []string
	for rows.Next() {
		var result string
		if err := rows.Scan(&result); err != nil {
			return nil, err
		}
		if result != "ok" {
			problems = append(problems, result)
		}
	}
	return problems, rows.Err()
}

func (f localFile) BackupTo(ctx context.Context, dst string) error {
	if _, err := f.db.ExecContext(ctx, "VACUUM INTO ?", dst); err != nil {
		return fmt.Errorf("vacuum into %s: %w", dst, err)
	}
	return nil
}
