package cmd

import "go.kenn.io/msgvault/internal/store"

// nativeVectorBackend returns the vector backend a store hosts natively:
// "sqlite-vec" for a SQLite store, "dolt" for a Dolt store, and "" for backends
// with no vector support yet (PostgreSQL).
//
// This is the single place the store→vector-backend mapping lives. Vector
// setup and the embeddings commands reason about the returned kind instead of
// interrogating the backend type in several spots, so adding vector support for
// a new backend is a one-line change here.
func nativeVectorBackend(s *store.Store) string {
	switch s.Backend() {
	case store.BackendDolt:
		return "dolt"
	case store.BackendSQLite:
		return "sqlite-vec"
	default:
		return ""
	}
}
