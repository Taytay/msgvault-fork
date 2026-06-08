//go:build sqlite_vec

package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/config"
	"go.kenn.io/msgvault/internal/store"
	"go.kenn.io/msgvault/internal/vector/doltvec"
	"go.kenn.io/msgvault/internal/vector/sqlitevec"
)

// vectorTestConfig returns a minimal valid vector config with the given
// backend selection. The embeddings endpoint is never called here — it only
// has to satisfy Config.Validate().
func vectorTestConfig(t *testing.T, backend string) *config.Config {
	t.Helper()
	c := config.NewDefaultConfig()
	c.Data.DataDir = t.TempDir()
	c.Vector.Enabled = true
	c.Vector.Backend = backend
	c.Vector.Embeddings.Endpoint = "http://localhost:9/v1"
	c.Vector.Embeddings.Model = "test-model"
	c.Vector.Embeddings.Dimension = 3
	c.Vector.Embeddings.BatchSize = 8
	return c
}

// TestSetupVectorFeatures_Selection verifies the backend-selection wiring:
// "auto"/"sqlite-vec" pick sqlitevec for a file store, the mismatched
// combinations error clearly, and "auto" picks doltvec for a Dolt store.
func TestSetupVectorFeatures_Selection(t *testing.T) {
	saved := cfg
	t.Cleanup(func() { cfg = saved })
	ctx := context.Background()

	t.Run("auto selects sqlite-vec for a file store", func(t *testing.T) {
		cfg = vectorTestConfig(t, "auto")
		dbPath := filepath.Join(t.TempDir(), "main.db")
		s, err := store.OpenForTest(dbPath)
		require.NoError(t, err)
		defer func() { _ = s.Close() }()
		require.NoError(t, s.InitSchema())

		vf, err := setupVectorFeatures(ctx, s.DB(), dbPath)
		require.NoError(t, err)
		require.NotNil(t, vf)
		defer func() { _ = vf.Close() }()

		_, ok := vf.Backend.(*sqlitevec.Backend)
		assert.True(t, ok, "file store should select the sqlite-vec backend")
		assert.NotNil(t, vf.Worker, "sqlite-vec path wires the embed worker")
		assert.NotNil(t, vf.HybridEngine)
	})

	t.Run("backend=dolt on a file store errors", func(t *testing.T) {
		cfg = vectorTestConfig(t, "dolt")
		_, err := setupVectorFeatures(ctx, nil, filepath.Join(t.TempDir(), "main.db"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "requires a Dolt")
	})

	t.Run("backend=sqlite-vec on a Dolt store errors", func(t *testing.T) {
		cfg = vectorTestConfig(t, "sqlite-vec")
		_, err := setupVectorFeatures(ctx, nil, "mysql://root@127.0.0.1:3306/x")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot run against a Dolt store")
	})

	t.Run("auto selects doltvec for a Dolt store", func(t *testing.T) {
		base := os.Getenv("MSGVAULT_TEST_DB")
		if !strings.HasPrefix(base, "mysql://") && !strings.HasPrefix(base, "dolt://") {
			t.Skip("set MSGVAULT_TEST_DB=mysql://... to run the Dolt selection case")
		}
		dbURL := createDoltTestDB(t, base)
		s, err := store.Open(dbURL)
		require.NoError(t, err)
		defer func() { _ = s.Close() }()
		require.NoError(t, s.InitSchema())

		cfg = vectorTestConfig(t, "auto")
		vf, err := setupVectorFeatures(ctx, s.DB(), dbURL)
		require.NoError(t, err)
		require.NotNil(t, vf)
		defer func() { _ = vf.Close() }()

		_, ok := vf.Backend.(*doltvec.Backend)
		assert.True(t, ok, "Dolt store should select the doltvec backend")
		assert.NotNil(t, vf.Worker, "Dolt path wires the embed worker (via doltvec.Queue)")
		assert.NotNil(t, vf.Enqueuer, "Dolt path wires an enqueuer")
		assert.NotNil(t, vf.HybridEngine)
	})
}

// createDoltTestDB provisions an isolated database on the Dolt server named by
// base and returns a connection URL targeting it. The database is dropped on
// test cleanup.
func createDoltTestDB(t *testing.T, base string) string {
	t.Helper()
	u, err := url.Parse(base)
	require.NoError(t, err)
	host := u.Host
	if !strings.Contains(host, ":") {
		host += ":3306"
	}
	var userInfo string
	if u.User != nil {
		userInfo = u.User.Username() + "@"
	}
	setupDSN := fmt.Sprintf("%stcp(%s)/?multiStatements=true", userInfo, host)
	setupDB, err := sql.Open("mysql", setupDSN)
	require.NoError(t, err)
	dbName := fmt.Sprintf("msgvault_vsel_%d", time.Now().UnixNano())
	_, err = setupDB.Exec("CREATE DATABASE " + dbName)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = setupDB.Exec("DROP DATABASE IF EXISTS " + dbName)
		_ = setupDB.Close()
	})
	u.Path = "/" + dbName
	return u.String()
}
