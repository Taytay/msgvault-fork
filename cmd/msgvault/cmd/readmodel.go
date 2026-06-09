package cmd

import (
	"context"
	"fmt"
	"os"

	"go.kenn.io/msgvault/internal/projection"
	"go.kenn.io/msgvault/internal/store"
)

// refreshReadModel brings a backend's derived read structures up to date after
// a write. Callers (sync, import, the daemon) simply ask "I'm done writing —
// make the read model current" without knowing which backend they're on:
//
//   - local SQLite (AnalyticsCache): refresh the Parquet analytics cache in
//     place from the database file.
//   - backends queried directly (PostgreSQL, Dolt): nothing is derived, so this
//     is a no-op — their analytics/list queries run straight against the system
//     of record via the dialect query engine (see query.NewEngineForStore).
//
// This is the single place that knows the per-backend strategy; the mapping is
// expressed over capabilities rather than backend types.
func refreshReadModel(_ context.Context, s *store.Store) error {
	cache, ok := s.AnalyticsCache()
	if !ok {
		return nil
	}
	analyticsDir := cfg.AnalyticsDir()
	fullRebuild := cacheNeedsBuild(s, analyticsDir).FullRebuild
	_, err := buildCache(cache.SourcePath(), analyticsDir, fullRebuild)
	return err
}

// refreshReadModelAfterWrite opens the store at dbPath and refreshes its read
// model, logging (not failing) on error — the system of record already holds
// the data durably. This is the post-sync / post-import hook; it replaces the
// old SQLite-only rebuildCacheAfterWrite so Dolt syncs also refresh the replica.
func refreshReadModelAfterWrite(dbPath string) {
	s, err := store.Open(dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: read-model refresh skipped (open db: %v)\n", err)
		return
	}
	defer func() { _ = s.Close() }()
	if err := refreshReadModel(context.Background(), s); err != nil {
		fmt.Fprintf(os.Stderr,
			"Warning: read-model refresh failed: %v\nRun 'msgvault build-cache' (or 'msgvault project' on Dolt) to retry.\n", err)
	}
}

// projectReplica rebuilds the local SQLite read-replica from a versioned source
// store, then rebuilds the analytics cache from that replica. Used only by the
// optional `project` command — the normal read path queries Dolt directly, so
// this is not part of refreshReadModel.
func projectReplica(ctx context.Context, src *store.Store, replicaPath, analyticsDir string) (*projection.Report, *buildResult, error) {
	// Fresh replica: remove any prior file so the rebuild is authoritative.
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if err := os.Remove(replicaPath + suffix); err != nil && !os.IsNotExist(err) {
			return nil, nil, fmt.Errorf("remove stale replica %s: %w", replicaPath+suffix, err)
		}
	}
	dst, err := store.Open(replicaPath)
	if err != nil {
		return nil, nil, fmt.Errorf("open replica: %w", err)
	}
	if err := dst.InitSchema(); err != nil {
		_ = dst.Close()
		return nil, nil, fmt.Errorf("init replica schema: %w", err)
	}
	rep, err := projection.Project(ctx, src, dst)
	if err != nil {
		_ = dst.Close()
		return nil, nil, fmt.Errorf("project: %w", err)
	}
	// Close the replica before build-cache opens its own DuckDB/SQLite handles.
	if err := dst.Close(); err != nil {
		return nil, nil, fmt.Errorf("close replica: %w", err)
	}
	result, err := buildCache(replicaPath, analyticsDir, true)
	if err != nil {
		return rep, nil, fmt.Errorf("build analytics cache: %w", err)
	}
	return rep, result, nil
}
