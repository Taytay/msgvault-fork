package store

import "strings"

// Backend identifies which database engine backs a Store.
//
// Backend exists for the two places where backend identity is legitimate: the
// factory that maps a DSN to a concrete implementation (Open), and
// human-readable messages. Behavioral differences between backends are
// expressed as capabilities (see capabilities.go, fts.go, version.go), NOT by
// switching on Backend in callers — a new backend should be addable by
// implementing the capabilities it supports, without editing call sites.
type Backend int

const (
	BackendSQLite Backend = iota
	BackendPostgreSQL
	BackendDolt
)

// String returns the human-readable backend name used in user-facing messages.
func (b Backend) String() string {
	switch b {
	case BackendPostgreSQL:
		return "PostgreSQL"
	case BackendDolt:
		return "Dolt"
	default:
		return "SQLite"
	}
}

// BackendOfDSN classifies a DSN/path into a Backend. This is the single place
// backend identity is read from a string: it serves the Open factory and the
// occasional pre-open command entry guard. Anything past the factory boundary
// should ask the opened Store for a capability instead.
//
// Dolt speaks the MySQL wire protocol, so mysql:// and dolt:// both map to
// BackendDolt.
func BackendOfDSN(dsn string) Backend {
	switch {
	case strings.HasPrefix(dsn, "postgresql://"), strings.HasPrefix(dsn, "postgres://"):
		return BackendPostgreSQL
	case strings.HasPrefix(dsn, "mysql://"), strings.HasPrefix(dsn, "dolt://"):
		return BackendDolt
	default:
		return BackendSQLite
	}
}

// Backend reports the engine backing an open Store, derived from the dialect's
// driver name.
func (s *Store) Backend() Backend {
	switch s.dialect.DriverName() {
	case "pgx":
		return BackendPostgreSQL
	case "mysql":
		return BackendDolt
	default:
		return BackendSQLite
	}
}

// IsPostgreSQL reports whether this store is backed by PostgreSQL. Retained as
// a thin convenience over Backend(); prefer capability accessors for
// behavioral decisions and reserve this for the query-engine dialect factory.
func (s *Store) IsPostgreSQL() bool { return s.Backend() == BackendPostgreSQL }

// IsMySQL reports whether this store is backed by MySQL/Dolt.
func (s *Store) IsMySQL() bool { return s.Backend() == BackendDolt }

// isMySQL is the unexported form used by intra-package backend gating.
func (s *Store) isMySQL() bool { return s.Backend() == BackendDolt }
