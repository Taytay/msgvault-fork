package cmd

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"go.kenn.io/msgvault/internal/store"
)

// projectCmd exports the Dolt system of record into a local SQLite + Parquet
// replica. This is OPTIONAL: msgvault queries Dolt directly for reads, search,
// and analytics (see query.NewDoltEngine), so the replica is only useful as an
// offline SQLite snapshot or to get the DuckDB/Parquet aggregate speedup on
// very large archives. It is no longer required after a sync.
var projectCmd = &cobra.Command{
	Use:   "project",
	Short: "Export the Dolt system of record to a local SQLite + Parquet replica (optional)",
	Long: `Export the Dolt backing store to a local SQLite + Parquet replica.

This is OPTIONAL. When [data].database_url points at a Dolt/MySQL backend,
msgvault reads, searches, and aggregates directly against Dolt — no replica is
needed for normal use. Run this only to produce an offline SQLite snapshot, or
to get the DuckDB/Parquet aggregate speedup on a very large archive. It:

  1. copies every table Dolt -> local SQLite replica (<data_dir>/replica.db),
  2. backfills the SQLite FTS5 keyword-search index, and
  3. rebuilds the Parquet analytics cache from the replica.

Rebuild embeddings separately with 'msgvault embeddings build'.`,
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

		// project rebuilds the local replica FROM a versioned system of record.
		// That property — not the concrete backend — is what makes the source
		// projectable, so gate on the capability rather than the type.
		if _, ok := src.VersionController(); !ok {
			return errors.New(
				"project requires a versioned system of record (set " +
					"[data].database_url to a mysql:// or dolt:// URL); other " +
					"backends are queried directly and use 'build-cache' instead")
		}

		fmt.Printf("Projecting Dolt -> %s ...\n", replicaPath)
		rep, result, err := projectReplica(ctx, src, replicaPath, analyticsDir)
		if err != nil {
			return err
		}
		fmt.Printf("Copied %d messages (%d indexed for search) into the replica.\n",
			rep.Rows["messages"], rep.FTSIndexed)
		fmt.Printf("Built analytics cache (%d messages) in %s\n", result.ExportedCount, result.OutputDir)
		fmt.Println("\nProjection complete. Rebuild embeddings with 'msgvault embeddings build' if you use semantic search.")
		return nil
	},
}

func init() {
	rootCmd.AddCommand(projectCmd)
}
