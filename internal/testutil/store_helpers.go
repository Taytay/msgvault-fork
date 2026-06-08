package testutil

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql" // Register mysql driver for Dolt test setup
	_ "github.com/jackc/pgx/v5/stdlib" // Register pgx driver for test setup
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/store"
)

// NewTestStore creates a temporary database for testing.
// The database is automatically cleaned up when the test completes.
//
// Backend selection via MSGVAULT_TEST_DB env var:
//   - unset or empty: SQLite (default)
//   - starts with "postgres://" or "postgresql://": PostgreSQL
//
// For PostgreSQL, each test gets its own schema (created and dropped for isolation).
func NewTestStore(t *testing.T) *store.Store {
	t.Helper()

	testDB := os.Getenv("MSGVAULT_TEST_DB")
	if strings.HasPrefix(testDB, "postgres://") || strings.HasPrefix(testDB, "postgresql://") {
		return newPostgresTestStore(t, testDB)
	}
	if strings.HasPrefix(testDB, "mysql://") || strings.HasPrefix(testDB, "dolt://") {
		return newMySQLTestStore(t, testDB)
	}

	dbPath := filepath.Join(t.TempDir(), "test.db")
	st, err := store.OpenForTest(dbPath)
	require.NoError(t, err, "open store")

	t.Cleanup(func() {
		_ = st.Close()
	})

	require.NoError(t, st.InitSchema(), "init schema")

	return st
}

// SkipIfPostgres skips the calling test when MSGVAULT_TEST_DB targets
// PostgreSQL. Use this for tests that exercise SQLite-only constructs
// (FTS5 MATCH, PRAGMA, BEGIN EXCLUSIVE, SQLite trigger syntax) where
// PostgreSQL's portable equivalent is covered by a separate test or
// by the Dialect interface.
func SkipIfPostgres(t *testing.T, reason string) {
	t.Helper()
	testDB := os.Getenv("MSGVAULT_TEST_DB")
	if strings.HasPrefix(testDB, "postgres://") || strings.HasPrefix(testDB, "postgresql://") {
		t.Skipf("skipping on PostgreSQL: %s", reason)
	}
}

// SkipIfServerBackend skips the calling test when MSGVAULT_TEST_DB targets any
// client/server backend (PostgreSQL or MySQL/Dolt). Use it for tests that
// exercise SQLite-only constructs (FTS5 MATCH, PRAGMA) whose portable
// equivalents are covered elsewhere or are served from the SQLite replica.
func SkipIfServerBackend(t *testing.T, reason string) {
	t.Helper()
	testDB := os.Getenv("MSGVAULT_TEST_DB")
	if strings.Contains(testDB, "://") {
		t.Skipf("skipping on server backend: %s", reason)
	}
}

// newMySQLTestStore creates a test-isolated Dolt/MySQL store using a random
// database name. The database is created and dropped for isolation, mirroring
// the per-schema isolation used for PostgreSQL.
func newMySQLTestStore(t *testing.T, dbURL string) *store.Store {
	t.Helper()

	u, err := url.Parse(dbURL)
	require.NoError(t, err, "parse MySQL/Dolt URL")

	buf := make([]byte, 8)
	_, err = rand.Read(buf)
	require.NoError(t, err, "random db name")
	dbName := "msgvault_test_" + hex.EncodeToString(buf)

	// Build a driver DSN (no database) for the create/drop control connection.
	host := u.Host
	if host == "" {
		host = "127.0.0.1:3306"
	} else if !strings.Contains(host, ":") {
		host += ":3306"
	}
	var userInfo string
	if u.User != nil {
		if pass, ok := u.User.Password(); ok {
			userInfo = u.User.Username() + ":" + pass + "@"
		} else {
			userInfo = u.User.Username() + "@"
		}
	}
	setupDSN := fmt.Sprintf("%stcp(%s)/?parseTime=true&multiStatements=true", userInfo, host)

	setupDB, err := sql.Open("mysql", setupDSN)
	require.NoError(t, err, "open setup connection")
	_, createErr := setupDB.Exec("CREATE DATABASE " + dbName)
	_ = setupDB.Close()
	require.NoErrorf(t, createErr, "create database %s", dbName)

	var st *store.Store
	t.Cleanup(func() {
		if st != nil {
			_ = st.Close()
		}
		cleanupDB, err := sql.Open("mysql", setupDSN)
		if err != nil {
			return
		}
		defer func() { _ = cleanupDB.Close() }()
		_, _ = cleanupDB.Exec("DROP DATABASE IF EXISTS " + dbName)
	})

	// Per-test connection URL targeting the isolated database.
	testU := *u
	testU.Path = "/" + dbName
	st, err = store.Open(testU.String())
	require.NoError(t, err, "open store")
	require.NoError(t, st.InitSchema(), "init schema")

	return st
}

// newPostgresTestStore creates a test-isolated PostgreSQL store using a random schema name.
// The schema is dropped on test cleanup.
func newPostgresTestStore(t *testing.T, dbURL string) *store.Store {
	t.Helper()

	// Generate a random schema name for test isolation
	buf := make([]byte, 8)
	_, err := rand.Read(buf)
	require.NoError(t, err, "random schema name")
	schemaName := "msgvault_test_" + hex.EncodeToString(buf)

	// Create the schema using a separate connection
	setupDB, err := sql.Open("pgx", dbURL)
	require.NoError(t, err, "open setup connection")
	_, schemaErr := setupDB.Exec("CREATE SCHEMA " + schemaName)
	_ = setupDB.Close()
	require.NoErrorf(t, schemaErr, "create schema %s", schemaName)

	// Register schema cleanup immediately so that any failure below this
	// point (store.Open, InitSchema) doesn't leak the schema.
	var st *store.Store
	t.Cleanup(func() {
		if st != nil {
			_ = st.Close()
		}
		cleanupDB, err := sql.Open("pgx", dbURL)
		if err != nil {
			return
		}
		defer func() { _ = cleanupDB.Close() }()
		_, _ = cleanupDB.Exec(fmt.Sprintf("DROP SCHEMA %s CASCADE", schemaName))
	})

	// Build a URL that uses the test schema via search_path
	testURL := dbURL
	sep := "?"
	if strings.Contains(dbURL, "?") {
		sep = "&"
	}
	testURL += sep + "search_path=" + schemaName

	st, err = store.Open(testURL)
	require.NoError(t, err, "open store")
	require.NoError(t, st.InitSchema(), "init schema")

	return st
}
