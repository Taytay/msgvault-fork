package doltvec

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"go.kenn.io/msgvault/internal/vector"
)

// Enqueuer inserts message IDs into vec_pending_embeddings for every
// non-retired generation. It is the Dolt parallel to embed.Enqueuer and
// structurally satisfies the internal/sync.EmbedEnqueuer interface, so the sync
// pipeline can keep queuing freshly-synced messages for embedding on the Dolt
// backend. (Draining the queue is the job of a Dolt-aware embed worker, which
// is not yet implemented — the queue simply accumulates until then.)
type Enqueuer struct {
	db *sql.DB
}

// NewEnqueuer returns an Enqueuer bound to the Dolt store's handle.
func NewEnqueuer(db *sql.DB) *Enqueuer { return &Enqueuer{db: db} }

// EnqueueMessages adds the given IDs to vec_pending_embeddings for every
// generation not in state 'retired'. Duplicates are ignored (INSERT IGNORE).
func (e *Enqueuer) EnqueueMessages(ctx context.Context, messageIDs []int64) error {
	if len(messageIDs) == 0 {
		return nil
	}
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("doltvec: begin enqueue tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.QueryContext(ctx,
		`SELECT id FROM vec_index_generations WHERE state != ?`, string(vector.GenerationRetired))
	if err != nil {
		return fmt.Errorf("doltvec: select non-retired generations: %w", err)
	}
	var gens []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return err
		}
		gens = append(gens, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	_ = rows.Close()
	if len(gens) == 0 {
		return tx.Commit()
	}

	const chunk = 500
	for _, g := range gens {
		for i := 0; i < len(messageIDs); i += chunk {
			end := i + chunk
			if end > len(messageIDs) {
				end = len(messageIDs)
			}
			batch := messageIDs[i:end]
			var b strings.Builder
			b.WriteString("INSERT IGNORE INTO vec_pending_embeddings (generation_id, message_id, enqueued_at) VALUES ")
			args := make([]any, 0, len(batch)*2)
			for j, mid := range batch {
				if j > 0 {
					b.WriteByte(',')
				}
				b.WriteString("(?, ?, NOW(6))")
				args = append(args, g, mid)
			}
			if _, err := tx.ExecContext(ctx, b.String(), args...); err != nil {
				return fmt.Errorf("doltvec: enqueue pending (gen=%d): %w", g, err)
			}
		}
	}
	return tx.Commit()
}
