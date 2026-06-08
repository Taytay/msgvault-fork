package doltvec

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"slices"
	"time"

	"go.kenn.io/msgvault/internal/vector"
)

// Queue is the Dolt parallel to embed.Queue: a crash-safe claim-mark-complete
// pattern over vec_pending_embeddings. It structurally satisfies
// embed.PendingQueue, so the shared embed.Worker drives it unchanged.
//
// MySQL/Dolt cannot run sqlite's single-statement claim
// (UPDATE ... WHERE (g,m) IN (SELECT ... LIMIT ?) RETURNING — rejected by
// error 1093 and the LIMIT-in-IN-subquery restriction), so Claim does a
// two-step claim inside one transaction: SELECT ... FOR UPDATE the candidate
// rows, then UPDATE them by explicit id. The row locks serialize concurrent
// claimers.
type Queue struct {
	db *sql.DB
}

// NewQueue returns a Queue bound to the Dolt store's handle (borrowed).
func NewQueue(db *sql.DB) *Queue { return &Queue{db: db} }

// Claim marks up to batch available rows for gen with a fresh token and
// returns their message IDs (ascending) plus the token.
func (q *Queue) Claim(ctx context.Context, gen vector.GenerationID, batch int) ([]int64, string, error) {
	if batch <= 0 {
		return nil, "", nil
	}
	token, err := newToken()
	if err != nil {
		return nil, "", err
	}

	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, "", fmt.Errorf("doltvec queue: begin claim: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.QueryContext(ctx, `
		SELECT message_id FROM vec_pending_embeddings
		 WHERE generation_id = ? AND claimed_at IS NULL
		 ORDER BY message_id LIMIT ? FOR UPDATE`, int64(gen), batch)
	if err != nil {
		return nil, "", fmt.Errorf("doltvec queue: select candidates: %w", err)
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, "", err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, "", err
	}
	_ = rows.Close()
	if len(ids) == 0 {
		return nil, "", tx.Commit()
	}

	inSQL, inArgs := inClause("message_id", ids)
	args := append([]any{time.Now(), token, int64(gen)}, inArgs...)
	if _, err := tx.ExecContext(ctx,
		"UPDATE vec_pending_embeddings SET claimed_at = ?, claim_token = ? WHERE generation_id = ? AND "+inSQL,
		args...); err != nil {
		return nil, "", fmt.Errorf("doltvec queue: mark claimed: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, "", fmt.Errorf("doltvec queue: commit claim: %w", err)
	}
	slices.Sort(ids)
	return ids, token, nil
}

// Complete removes claimed rows whose claim_token matches token.
func (q *Queue) Complete(ctx context.Context, gen vector.GenerationID, token string, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	inSQL, inArgs := inClause("message_id", ids)
	args := append([]any{int64(gen), token}, inArgs...)
	_, err := q.db.ExecContext(ctx,
		"DELETE FROM vec_pending_embeddings WHERE generation_id = ? AND claim_token = ? AND "+inSQL, args...)
	if err != nil {
		return fmt.Errorf("doltvec queue: complete: %w", err)
	}
	return nil
}

// Release returns claimed rows (matching token) to the pool.
func (q *Queue) Release(ctx context.Context, gen vector.GenerationID, token string, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	inSQL, inArgs := inClause("message_id", ids)
	args := append([]any{int64(gen), token}, inArgs...)
	_, err := q.db.ExecContext(ctx,
		"UPDATE vec_pending_embeddings SET claimed_at = NULL, claim_token = NULL WHERE generation_id = ? AND claim_token = ? AND "+inSQL,
		args...)
	if err != nil {
		return fmt.Errorf("doltvec queue: release: %w", err)
	}
	return nil
}

// ReclaimStale clears claims older than olderThan; returns rows reclaimed.
func (q *Queue) ReclaimStale(ctx context.Context, olderThan time.Duration) (int, error) {
	cutoff := time.Now().Add(-olderThan)
	res, err := q.db.ExecContext(ctx,
		`UPDATE vec_pending_embeddings SET claimed_at = NULL, claim_token = NULL
		  WHERE claimed_at IS NOT NULL AND claimed_at < ?`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("doltvec queue: reclaim stale: %w", err)
	}
	n, err := res.RowsAffected()
	return int(n), err
}

func newToken() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
