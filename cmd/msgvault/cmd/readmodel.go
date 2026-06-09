package cmd

import (
	"context"
	"fmt"
	"os"

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
// the data durably. This is the post-sync / post-import hook.
func refreshReadModelAfterWrite(dbPath string) {
	s, err := store.Open(dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: read-model refresh skipped (open db: %v)\n", err)
		return
	}
	defer func() { _ = s.Close() }()
	if err := refreshReadModel(context.Background(), s); err != nil {
		fmt.Fprintf(os.Stderr,
			"Warning: read-model refresh failed: %v\nRun 'msgvault build-cache' to retry.\n", err)
	}
}
