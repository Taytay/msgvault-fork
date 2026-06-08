package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
)

// VersionController is the optional capability a version-controlled backend
// (Dolt) exposes for snapshotting the data store. Stores without it return
// (nil, false) from Store.VersionController. It is the seam future
// push / pull / clone operations will extend.
type VersionController interface {
	// Commit stages all working-set changes and records a commit with the
	// given message and author ("Name <email>"), returning the new commit
	// hash. When there is nothing to commit it returns ("", nil) — a no-op,
	// not an error — so it is safe to call on every sync boundary.
	Commit(ctx context.Context, message, author string) (hash string, err error)
}

// defaultCommitAuthor is used when Commit is called with an empty author.
const defaultCommitAuthor = "msgvault <msgvault@localhost>"

// VersionController returns the store's version-control capability and true
// when the backend supports it (Dolt). SQLite and PostgreSQL return
// (nil, false): they are not version-controlled, so callers treat the absence
// as "snapshotting is a no-op here".
func (s *Store) VersionController() (VersionController, bool) {
	if s.isMySQL() {
		return &doltVersionController{db: s.db}, true
	}
	return nil, false
}

// doltVersionController commits the Dolt working set via the DOLT_COMMIT stored
// procedure. It is created per call by Store.VersionController and borrows the
// store's logged handle.
type doltVersionController struct {
	db *loggedDB
}

func (d *doltVersionController) Commit(ctx context.Context, message, author string) (string, error) {
	if message == "" {
		message = "msgvault sync"
	}
	if author == "" {
		author = defaultCommitAuthor
	}
	// -A stages every table; --skip-empty turns a no-change commit into a
	// silent no-op (the procedure returns no row) so this is safe to call on
	// every sync boundary. On a real commit it returns one row: the hash.
	var hash string
	err := d.db.QueryRowContext(ctx,
		"CALL DOLT_COMMIT('-A', '--skip-empty', '-m', ?, '--author', ?)",
		message, author).Scan(&hash)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil // nothing to commit
	}
	if err != nil {
		return "", fmt.Errorf("dolt commit: %w", err)
	}
	return hash, nil
}

// commitSyncBoundary snapshots the data store after a completed sync when the
// backend is version-controlled (Dolt). It is best-effort: the sync's data is
// already durably written, so a commit failure is logged but does not fail the
// sync — the next boundary re-snapshots the accumulated changes.
func (s *Store) commitSyncBoundary(syncID int64) {
	vc, ok := s.VersionController()
	if !ok {
		return
	}
	hash, err := vc.Commit(context.Background(), s.syncCommitMessage(syncID), "")
	switch {
	case err != nil:
		slog.Warn("dolt commit at sync boundary failed", "sync_id", syncID, "error", err.Error())
	case hash != "":
		slog.Info("dolt commit at sync boundary", "sync_id", syncID, "hash", hash)
	}
}

// syncCommitMessage builds a human-readable commit message from the completed
// sync run, falling back to a generic message if the lookup fails.
func (s *Store) syncCommitMessage(syncID int64) string {
	var (
		srcType, identifier sql.NullString
		added, updated      int64
	)
	err := s.db.QueryRow(`
		SELECT so.source_type, so.identifier, sr.messages_added, sr.messages_updated
		  FROM sync_runs sr JOIN sources so ON so.id = sr.source_id
		 WHERE sr.id = ?`, syncID).Scan(&srcType, &identifier, &added, &updated)
	if err != nil {
		return fmt.Sprintf("msgvault sync run %d", syncID)
	}
	return fmt.Sprintf("sync %s:%s — +%d new, %d updated (run %d)",
		srcType.String, identifier.String, added, updated, syncID)
}
