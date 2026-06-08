package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"go.kenn.io/msgvault/internal/projection"
	"go.kenn.io/msgvault/internal/store"
)

// projectCmd rebuilds the local SQLite read-replica and its Parquet analytics
// cache from the Dolt system of record. Under the Dolt backend, Dolt is the
// durable/versioned/syncable store while reads (list, search, analytics) run
// against the local replica; this command refreshes that replica.
var projectCmd = &cobra.Command{
	Use:   "project",
	Short: "Rebuild the local read-replica (SQLite + Parquet) from the Dolt system of record",
	Long: `Rebuild the local read-replica from the Dolt backing store.

When [data].database_url points at a Dolt/MySQL backend, Dolt is the system of
record but the fast read/search/analytics stack runs against a local SQLite
replica. This command:

  1. copies every table Dolt -> local SQLite replica (<data_dir>/replica.db),
  2. backfills the SQLite FTS5 keyword-search index, and
  3. rebuilds the Parquet analytics cache from the replica.

Run it after syncing new mail into Dolt (or after 'dolt pull' on another
machine). Rebuild embeddings separately with 'msgvault embeddings build'.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		dbURL := cfg.DatabaseDSN()
		replicaPath := cfg.ReplicaPath()
		analyticsDir := cfg.AnalyticsDir()
		ctx := context.Background()

		// Source of record: Dolt.
		src, err := store.Open(dbURL)
		if err != nil {
			return fmt.Errorf("open Dolt source: %w", err)
		}
		defer func() { _ = src.Close() }()

		// project rebuilds the local replica FROM Dolt; it is meaningful only
		// when Dolt is the system of record. SQLite/PostgreSQL backends are
		// queried directly and use 'build-cache' instead.
		if src.Backend() != store.BackendDolt {
			return fmt.Errorf(
				"project is only for the Dolt backend; set [data].database_url "+
					"to a mysql:// or dolt:// URL (the %s backend is queried "+
					"directly and uses 'build-cache' instead)", src.Backend())
		}

		// Fresh replica: remove any prior file so the rebuild is authoritative.
		for _, suffix := range []string{"", "-wal", "-shm"} {
			if err := os.Remove(replicaPath + suffix); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("remove stale replica %s: %w", replicaPath+suffix, err)
			}
		}
		dst, err := store.Open(replicaPath)
		if err != nil {
			return fmt.Errorf("open replica: %w", err)
		}
		if err := dst.InitSchema(); err != nil {
			_ = dst.Close()
			return fmt.Errorf("init replica schema: %w", err)
		}

		fmt.Printf("Projecting Dolt -> %s ...\n", replicaPath)
		rep, err := projection.Project(ctx, src, dst)
		if err != nil {
			_ = dst.Close()
			return fmt.Errorf("project: %w", err)
		}
		// Close the replica before build-cache opens its own DuckDB/SQLite handles.
		if err := dst.Close(); err != nil {
			return fmt.Errorf("close replica: %w", err)
		}
		fmt.Printf("Copied %d messages (%d indexed for search) into the replica.\n",
			rep.Rows["messages"], rep.FTSIndexed)

		// Rebuild the Parquet analytics cache from the replica (unchanged path).
		result, err := buildCache(replicaPath, analyticsDir, true)
		if err != nil {
			return fmt.Errorf("build analytics cache: %w", err)
		}
		fmt.Printf("Built analytics cache (%d messages) in %s\n", result.ExportedCount, result.OutputDir)
		fmt.Println("\nProjection complete. Rebuild embeddings with 'msgvault embeddings build' if you use semantic search.")
		return nil
	},
}

func init() {
	rootCmd.AddCommand(projectCmd)
}
