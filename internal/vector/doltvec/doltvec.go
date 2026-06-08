// Package doltvec implements vector.Backend (and vector.FusingBackend) on top
// of Dolt (MySQL wire protocol), as a parallel to internal/vector/sqlitevec.
//
// Unlike the sqlite-vec backend (which keeps embeddings in a separate
// vectors.db and ATTACHes the main DB for fused search), doltvec stores its
// tables in the SAME Dolt database as the messages it indexes. That makes
// fused keyword+vector search a co-located join — no cross-database ATTACH.
//
// Mechanics on Dolt 2.1.4 (verified):
//   - vectors live in a per-dimension table vec_embeddings_d<N> with a Dolt
//     VECTOR INDEX on a JSON NOT NULL column; ANN is ORDER BY VEC_DISTANCE.
//   - "best chunk per message" is GROUP BY message_id, MIN(VEC_DISTANCE(...)).
//   - keyword search uses a MySQL FULLTEXT index added to messages(subject,
//     snippet) and MATCH(...) AGAINST(...) (natural-language; TF-IDF relevance).
//   - hybrid fusion is Reciprocal Rank Fusion computed in Go over the two
//     ranked lists (Dolt has no FULL OUTER JOIN for a single-query CTE).
//
// Limitations vs sqlite-vec (documented, not bugs): keyword search covers
// subject+snippet only (FULLTEXT cannot span messages+message_bodies), and the
// relevance is TF-IDF rather than tunable BM25. RRF consumes ranks, so fusion
// quality is largely preserved. The backend borrows the store's *sql.DB and
// does not own it (Close is a no-op).
package doltvec

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"go.kenn.io/msgvault/internal/store"
	"go.kenn.io/msgvault/internal/vector"
)

// ftIndexName is the FULLTEXT index doltvec adds to the messages table.
const ftIndexName = "vec_ft_subject_snippet"

// Options configures a Dolt vector backend.
type Options struct {
	DB        *sql.DB // the Dolt store's database handle (co-located with messages)
	Dimension int     // default embedding dimension (pre-creates its table)
}

// Backend implements vector.Backend over Dolt.
type Backend struct {
	db *sql.DB
}

var _ vector.FusingBackend = (*Backend)(nil)

// Open prepares the Dolt vector schema (generation/pending tables, the
// messages FULLTEXT index, and the per-dimension embeddings table) and returns
// a backend. The *sql.DB is borrowed, not owned.
func Open(ctx context.Context, opts Options) (*Backend, error) {
	if opts.DB == nil {
		return nil, errors.New("doltvec: Options.DB is required")
	}
	b := &Backend{db: opts.DB}
	if err := b.ensureBaseSchema(ctx); err != nil {
		return nil, err
	}
	if opts.Dimension > 0 {
		if err := b.ensureVectorTable(ctx, opts.Dimension); err != nil {
			return nil, err
		}
	}
	return b, nil
}

// DB exposes the underlying handle (parity with sqlitevec.Backend.DB).
func (b *Backend) DB() *sql.DB { return b.db }

// Close is a no-op: doltvec borrows the store's connection pool.
func (b *Backend) Close() error { return nil }

func vecTable(dim int) string { return fmt.Sprintf("vec_embeddings_d%d", dim) }

func vecJSON(v []float32) (string, error) {
	b, err := json.Marshal(v)
	return string(b), err
}

// ---- schema -------------------------------------------------------------

func (b *Backend) ensureBaseSchema(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS vec_index_generations (
			id            BIGINT AUTO_INCREMENT PRIMARY KEY,
			model         VARCHAR(255) NOT NULL,
			dimension     INT NOT NULL,
			fingerprint   VARCHAR(512) NOT NULL,
			started_at    DATETIME(6) NOT NULL,
			seeded_at     DATETIME(6),
			completed_at  DATETIME(6),
			activated_at  DATETIME(6),
			state         VARCHAR(16) NOT NULL,
			message_count BIGINT NOT NULL DEFAULT 0,
			KEY idx_vec_generations_state (state)
		)`,
		`CREATE TABLE IF NOT EXISTS vec_pending_embeddings (
			generation_id BIGINT NOT NULL,
			message_id    BIGINT NOT NULL,
			enqueued_at   DATETIME(6) NOT NULL,
			claimed_at    DATETIME(6),
			claim_token   VARCHAR(64),
			PRIMARY KEY (generation_id, message_id)
		)`,
	}
	for _, s := range stmts {
		if _, err := b.db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("doltvec: create base schema: %w", err)
		}
	}

	// Add the FULLTEXT index to messages once (MySQL has no IF NOT EXISTS for
	// ADD INDEX, so probe information_schema first).
	var n int
	if err := b.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM information_schema.statistics
		  WHERE table_schema = DATABASE() AND table_name = 'messages' AND index_name = ?`,
		ftIndexName).Scan(&n); err != nil {
		return fmt.Errorf("doltvec: probe fulltext index: %w", err)
	}
	if n == 0 {
		if _, err := b.db.ExecContext(ctx,
			"ALTER TABLE messages ADD FULLTEXT INDEX "+ftIndexName+" (subject, snippet)"); err != nil {
			return fmt.Errorf("doltvec: add messages fulltext index: %w", err)
		}
	}
	return nil
}

func (b *Backend) ensureVectorTable(ctx context.Context, dim int) error {
	tbl := vecTable(dim)
	var n int
	if err := b.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM information_schema.tables
		  WHERE table_schema = DATABASE() AND table_name = ?`, tbl).Scan(&n); err != nil {
		return fmt.Errorf("doltvec: probe vector table: %w", err)
	}
	if n > 0 {
		return nil
	}
	create := fmt.Sprintf(`CREATE TABLE %s (
		generation_id    BIGINT NOT NULL,
		message_id       BIGINT NOT NULL,
		chunk_index      INT NOT NULL DEFAULT 0,
		embedding        JSON NOT NULL,
		embedded_at      DATETIME(6) NOT NULL,
		source_char_len  INT NOT NULL DEFAULT 0,
		chunk_char_start INT NOT NULL DEFAULT 0,
		chunk_char_end   INT NOT NULL DEFAULT 0,
		truncated        TINYINT NOT NULL DEFAULT 0,
		PRIMARY KEY (generation_id, message_id, chunk_index),
		KEY idx_%s_msg (message_id)
	)`, tbl, tbl)
	if _, err := b.db.ExecContext(ctx, create); err != nil {
		return fmt.Errorf("doltvec: create %s: %w", tbl, err)
	}
	if _, err := b.db.ExecContext(ctx,
		fmt.Sprintf("CREATE VECTOR INDEX %s_vec ON %s (embedding)", tbl, tbl)); err != nil {
		return fmt.Errorf("doltvec: create vector index on %s: %w", tbl, err)
	}
	return nil
}

// ---- generation lifecycle ----------------------------------------------

func (b *Backend) CreateGeneration(ctx context.Context, model string, dim int, fingerprint string) (vector.GenerationID, error) {
	if fingerprint == "" {
		fingerprint = fmt.Sprintf("%s:%d", model, dim)
	}
	if err := b.ensureVectorTable(ctx, dim); err != nil {
		return 0, err
	}

	// Resume an existing building generation when the fingerprint matches.
	var (
		existingID int64
		existingFP string
	)
	err := b.db.QueryRowContext(ctx,
		`SELECT id, fingerprint FROM vec_index_generations WHERE state = 'building' LIMIT 1`).
		Scan(&existingID, &existingFP)
	switch {
	case err == nil:
		if existingFP != fingerprint {
			return 0, fmt.Errorf("%w: building=%q requested=%q",
				vector.ErrBuildingInProgress, existingFP, fingerprint)
		}
		if err := b.EnsureSeeded(ctx, vector.GenerationID(existingID)); err != nil {
			return 0, err
		}
		return vector.GenerationID(existingID), nil
	case errors.Is(err, sql.ErrNoRows):
		// fall through to create
	default:
		return 0, fmt.Errorf("doltvec: check building generation: %w", err)
	}

	res, err := b.db.ExecContext(ctx,
		`INSERT INTO vec_index_generations (model, dimension, fingerprint, started_at, state)
		 VALUES (?, ?, ?, NOW(6), 'building')`, model, dim, fingerprint)
	if err != nil {
		return 0, fmt.Errorf("doltvec: insert generation: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	gen := vector.GenerationID(id)
	if err := b.seedPending(ctx, gen); err != nil {
		return 0, err
	}
	if err := b.markSeeded(ctx, gen); err != nil {
		return 0, err
	}
	return gen, nil
}

// seedPending enqueues every live message for embedding (idempotent).
func (b *Backend) seedPending(ctx context.Context, gen vector.GenerationID) error {
	_, err := b.db.ExecContext(ctx, fmt.Sprintf(
		`INSERT IGNORE INTO vec_pending_embeddings (generation_id, message_id, enqueued_at)
		 SELECT ?, m.id, NOW(6) FROM messages m WHERE %s`,
		store.LiveMessagesWhere("m", true)), int64(gen))
	if err != nil {
		return fmt.Errorf("doltvec: seed pending: %w", err)
	}
	return nil
}

func (b *Backend) markSeeded(ctx context.Context, gen vector.GenerationID) error {
	_, err := b.db.ExecContext(ctx,
		`UPDATE vec_index_generations SET seeded_at = COALESCE(seeded_at, NOW(6)) WHERE id = ?`, int64(gen))
	return err
}

func (b *Backend) EnsureSeeded(ctx context.Context, gen vector.GenerationID) error {
	var (
		state  string
		seeded sql.NullTime
	)
	err := b.db.QueryRowContext(ctx,
		`SELECT state, seeded_at FROM vec_index_generations WHERE id = ?`, int64(gen)).
		Scan(&state, &seeded)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: %d", vector.ErrUnknownGeneration, gen)
	}
	if err != nil {
		return err
	}
	if state != string(vector.GenerationBuilding) {
		return fmt.Errorf("%w: generation %d is %q", vector.ErrGenerationNotBuilding, gen, state)
	}
	if seeded.Valid {
		return nil
	}
	if err := b.seedPending(ctx, gen); err != nil {
		return err
	}
	return b.markSeeded(ctx, gen)
}

func (b *Backend) ActivateGeneration(ctx context.Context, gen vector.GenerationID) error {
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx,
		`UPDATE vec_index_generations SET state = 'retired',
		   completed_at = COALESCE(completed_at, NOW(6)) WHERE state = 'active'`); err != nil {
		return fmt.Errorf("doltvec: retire active: %w", err)
	}
	res, err := tx.ExecContext(ctx,
		`UPDATE vec_index_generations SET state = 'active', activated_at = NOW(6),
		   completed_at = COALESCE(completed_at, NOW(6)) WHERE id = ? AND state = 'building'`, int64(gen))
	if err != nil {
		return fmt.Errorf("doltvec: activate: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("doltvec: generation %d not in 'building' state", gen)
	}
	return tx.Commit()
}

func (b *Backend) RetireGeneration(ctx context.Context, gen vector.GenerationID) error {
	_, err := b.db.ExecContext(ctx,
		`UPDATE vec_index_generations SET state = 'retired' WHERE id = ?`, int64(gen))
	return err
}

func (b *Backend) ActiveGeneration(ctx context.Context) (vector.Generation, error) {
	g, err := b.generationByState(ctx, string(vector.GenerationActive))
	if errors.Is(err, sql.ErrNoRows) {
		return vector.Generation{}, vector.ErrNoActiveGeneration
	}
	if err != nil {
		return vector.Generation{}, err
	}
	return g, nil
}

func (b *Backend) BuildingGeneration(ctx context.Context) (*vector.Generation, error) {
	g, err := b.generationByState(ctx, string(vector.GenerationBuilding))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil //nolint:nilnil // "no building generation" is (nil, nil) per the Backend contract
	}
	if err != nil {
		return nil, err
	}
	return &g, nil
}

func (b *Backend) generationByState(ctx context.Context, state string) (vector.Generation, error) {
	var (
		g         vector.Generation
		started   time.Time
		completed sql.NullTime
		activated sql.NullTime
	)
	err := b.db.QueryRowContext(ctx,
		`SELECT id, model, dimension, fingerprint, state, started_at, completed_at, activated_at, message_count
		   FROM vec_index_generations WHERE state = ? LIMIT 1`, state).
		Scan(&g.ID, &g.Model, &g.Dimension, &g.Fingerprint, &g.State, &started, &completed, &activated, &g.MessageCount)
	if err != nil {
		return vector.Generation{}, err
	}
	g.StartedAt = started
	if completed.Valid {
		g.CompletedAt = &completed.Time
	}
	if activated.Valid {
		g.ActivatedAt = &activated.Time
	}
	return g, nil
}

func (b *Backend) generationDimension(ctx context.Context, gen vector.GenerationID) (int, error) {
	var dim int
	err := b.db.QueryRowContext(ctx,
		`SELECT dimension FROM vec_index_generations WHERE id = ?`, int64(gen)).Scan(&dim)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("%w: %d", vector.ErrUnknownGeneration, gen)
	}
	return dim, err
}

// ---- write path ---------------------------------------------------------

func (b *Backend) Upsert(ctx context.Context, gen vector.GenerationID, chunks []vector.Chunk) error {
	if len(chunks) == 0 {
		return nil
	}
	dim, err := b.generationDimension(ctx, gen)
	if err != nil {
		return err
	}
	for _, c := range chunks {
		if len(c.Vector) != dim {
			return fmt.Errorf("%w: chunk dim %d != generation dim %d",
				vector.ErrDimensionMismatch, len(c.Vector), dim)
		}
	}
	tbl := vecTable(dim)
	mids := distinctMessageIDs(chunks)

	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	// message_count delta: messages newly gaining a chunk (idempotent re-upsert
	// of the same message contributes 0).
	existing, err := countPresent(ctx, tx, tbl, gen, mids)
	if err != nil {
		return err
	}

	inSQL, inArgs := inClause("message_id", mids)
	if _, err := tx.ExecContext(ctx,
		fmt.Sprintf("DELETE FROM %s WHERE generation_id = ? AND %s", tbl, inSQL),
		append([]any{int64(gen)}, inArgs...)...); err != nil {
		return fmt.Errorf("doltvec: delete prior chunks: %w", err)
	}

	ins, err := tx.PrepareContext(ctx, fmt.Sprintf(
		`INSERT INTO %s (generation_id, message_id, chunk_index, embedding, embedded_at,
			source_char_len, chunk_char_start, chunk_char_end, truncated)
		 VALUES (?, ?, ?, ?, NOW(6), ?, ?, ?, ?)`, tbl))
	if err != nil {
		return err
	}
	defer func() { _ = ins.Close() }()
	for _, c := range chunks {
		ev, err := vecJSON(c.Vector)
		if err != nil {
			return err
		}
		if _, err := ins.ExecContext(ctx, int64(gen), c.MessageID, c.ChunkIndex, ev,
			c.SourceCharLen, c.ChunkCharStart, c.ChunkCharEnd, boolToInt(c.Truncated)); err != nil {
			return fmt.Errorf("doltvec: insert chunk: %w", err)
		}
	}

	delta := int64(len(mids)) - existing
	if delta != 0 {
		if _, err := tx.ExecContext(ctx,
			`UPDATE vec_index_generations SET message_count = message_count + ? WHERE id = ?`,
			delta, int64(gen)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (b *Backend) Delete(ctx context.Context, gen vector.GenerationID, messageIDs []int64) error {
	if len(messageIDs) == 0 {
		return nil
	}
	dim, err := b.generationDimension(ctx, gen)
	if err != nil {
		return err
	}
	tbl := vecTable(dim)

	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	present, err := countPresent(ctx, tx, tbl, gen, messageIDs)
	if err != nil {
		return err
	}
	inSQL, inArgs := inClause("message_id", messageIDs)
	if _, err := tx.ExecContext(ctx,
		fmt.Sprintf("DELETE FROM %s WHERE generation_id = ? AND %s", tbl, inSQL),
		append([]any{int64(gen)}, inArgs...)...); err != nil {
		return err
	}
	if present != 0 {
		if _, err := tx.ExecContext(ctx,
			`UPDATE vec_index_generations SET message_count = message_count - ? WHERE id = ?`,
			present, int64(gen)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// countPresent returns how many of mids already have ≥1 chunk in tbl/gen.
func countPresent(ctx context.Context, tx *sql.Tx, tbl string, gen vector.GenerationID, mids []int64) (int64, error) {
	inSQL, inArgs := inClause("message_id", mids)
	var n int64
	err := tx.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT COUNT(DISTINCT message_id) FROM %s WHERE generation_id = ? AND %s", tbl, inSQL),
		append([]any{int64(gen)}, inArgs...)...).Scan(&n)
	return n, err
}

// ---- read path ----------------------------------------------------------

func (b *Backend) Search(ctx context.Context, gen vector.GenerationID, queryVec []float32, k int, filter vector.Filter) ([]vector.Hit, error) {
	dim, err := b.generationDimension(ctx, gen)
	if err != nil {
		return nil, err
	}
	if len(queryVec) != dim {
		return nil, fmt.Errorf("%w: query dim %d != generation dim %d",
			vector.ErrDimensionMismatch, len(queryVec), dim)
	}
	qv, err := vecJSON(queryVec)
	if err != nil {
		return nil, err
	}
	tbl := vecTable(dim)

	// Distance arg (SELECT) precedes generation_id (WHERE); filter args follow;
	// k (LIMIT) is last — matching the placeholder order in the statement text.
	args := []any{qv, int64(gen)}
	fSQL, fArgs := b.filterClause(filter)
	args = append(args, fArgs...)
	args = append(args, k)

	q := fmt.Sprintf(`
		SELECT e.message_id, MIN(VEC_DISTANCE(e.embedding, ?)) AS dist
		  FROM %s e JOIN messages m ON m.id = e.message_id
		 WHERE e.generation_id = ? AND %s%s
		 GROUP BY e.message_id
		 ORDER BY dist ASC
		 LIMIT ?`, tbl, store.LiveMessagesWhere("m", true), fSQL)

	rows, err := b.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("doltvec: ann query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var hits []vector.Hit
	for rows.Next() {
		var (
			mid  int64
			dist float64
		)
		if err := rows.Scan(&mid, &dist); err != nil {
			return nil, err
		}
		hits = append(hits, vector.Hit{MessageID: mid, Score: 1.0 - dist, Rank: len(hits) + 1})
	}
	return hits, rows.Err()
}

func (b *Backend) LoadVector(ctx context.Context, messageID int64) ([]float32, error) {
	active, err := b.ActiveGeneration(ctx)
	if err != nil {
		return nil, err
	}
	tbl := vecTable(active.Dimension)
	var raw string
	err = b.db.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT embedding FROM %s WHERE generation_id = ? AND message_id = ? AND chunk_index = 0`, tbl),
		int64(active.ID), messageID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("doltvec: message %d not embedded in active generation %d", messageID, active.ID)
	}
	if err != nil {
		return nil, err
	}
	var v []float32
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		return nil, fmt.Errorf("doltvec: decode embedding: %w", err)
	}
	return v, nil
}

func (b *Backend) Stats(ctx context.Context, gen vector.GenerationID) (vector.Stats, error) {
	var s vector.Stats
	if gen != 0 {
		dim, err := b.generationDimension(ctx, gen)
		if err != nil {
			return s, err
		}
		if err := b.db.QueryRowContext(ctx, fmt.Sprintf(
			`SELECT COUNT(DISTINCT message_id) FROM %s WHERE generation_id = ?`, vecTable(dim)),
			int64(gen)).Scan(&s.EmbeddingCount); err != nil {
			return s, err
		}
		if err := b.db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM vec_pending_embeddings WHERE generation_id = ?`, int64(gen)).
			Scan(&s.PendingCount); err != nil {
			return s, err
		}
		return s, nil
	}

	// Aggregate across every per-dimension embeddings table.
	tables, err := b.vectorTables(ctx)
	if err != nil {
		return s, err
	}
	for _, tbl := range tables {
		var n int64
		if err := b.db.QueryRowContext(ctx, fmt.Sprintf(
			`SELECT COUNT(*) FROM (SELECT DISTINCT generation_id, message_id FROM %s) t`, tbl)).Scan(&n); err != nil {
			return s, err
		}
		s.EmbeddingCount += n
	}
	if err := b.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM vec_pending_embeddings`).Scan(&s.PendingCount); err != nil {
		return s, err
	}
	return s, nil
}

func (b *Backend) vectorTables(ctx context.Context) ([]string, error) {
	rows, err := b.db.QueryContext(ctx,
		`SELECT table_name FROM information_schema.tables
		  WHERE table_schema = DATABASE() AND table_name LIKE 'vec_embeddings_d%'`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// ---- fused (keyword + vector) search ------------------------------------

func (b *Backend) FusedSearch(ctx context.Context, req vector.FusedRequest) ([]vector.FusedHit, bool, error) {
	kperK := req.KPerSignal
	if kperK <= 0 {
		kperK = 100
	}
	rrfK := req.RRFK
	if rrfK <= 0 {
		rrfK = 60
	}

	// Keyword signal (FULLTEXT). Fetch KPerSignal+1 to detect pool saturation.
	kw, kwPool, err := b.keywordRanking(ctx, req.FTSQuery, req.Filter, kperK+1)
	if err != nil {
		return nil, false, err
	}

	// Vector signal (ANN), reusing Search (filter + live applied there).
	var vec []vector.Hit
	if req.QueryVec != nil {
		vec, err = b.Search(ctx, req.Generation, req.QueryVec, kperK+1, req.Filter)
		if err != nil {
			return nil, false, err
		}
	}
	vecPool := len(vec)

	type acc struct {
		rrf    float64
		bm25   float64
		vector float64
	}
	scores := map[int64]*acc{}
	get := func(id int64) *acc {
		a := scores[id]
		if a == nil {
			a = &acc{bm25: math.NaN(), vector: math.NaN()}
			scores[id] = a
		}
		return a
	}
	for rank, s := range capList(kw, kperK) {
		a := get(s.id)
		a.rrf += 1.0 / float64(rrfK+rank+1)
		a.bm25 = s.score
	}
	for rank, h := range capHits(vec, kperK) {
		a := get(h.MessageID)
		a.rrf += 1.0 / float64(rrfK+rank+1)
		a.vector = h.Score
	}

	hits := make([]vector.FusedHit, 0, len(scores))
	for id, a := range scores {
		hits = append(hits, vector.FusedHit{
			MessageID: id, RRFScore: a.rrf, BM25Score: a.bm25, VectorScore: a.vector,
		})
	}

	if req.SubjectBoost > 1.0 && len(req.SubjectTerms) > 0 {
		if err := b.applySubjectBoost(ctx, hits, req.SubjectTerms, req.SubjectBoost); err != nil {
			return nil, false, err
		}
	}

	sort.Slice(hits, func(i, j int) bool {
		if hits[i].RRFScore != hits[j].RRFScore {
			return hits[i].RRFScore > hits[j].RRFScore
		}
		return hits[i].MessageID < hits[j].MessageID
	})
	if req.Limit > 0 && len(hits) > req.Limit {
		hits = hits[:req.Limit]
	}

	saturated := kwPool > kperK || vecPool > kperK
	return hits, saturated, nil
}

type scored struct {
	id    int64
	score float64
}

// keywordRanking runs the FULLTEXT natural-language query, restricted to live
// messages and the filter, ordered by relevance. Returns the ranked list and
// the pool size fetched (for saturation detection).
func (b *Backend) keywordRanking(ctx context.Context, query string, filter vector.Filter, limit int) ([]scored, int, error) {
	if strings.TrimSpace(query) == "" {
		return nil, 0, nil
	}
	args := []any{query, query}
	fSQL, fArgs := b.filterClause(filter)
	args = append(args, fArgs...)
	args = append(args, limit)

	q := fmt.Sprintf(`
		SELECT m.id, MATCH(m.subject, m.snippet) AGAINST(?) AS rel
		  FROM messages m
		 WHERE %s AND MATCH(m.subject, m.snippet) AGAINST(?) > 0%s
		 ORDER BY rel DESC
		 LIMIT ?`, store.LiveMessagesWhere("m", true), fSQL)

	rows, err := b.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("doltvec: fulltext query: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []scored
	for rows.Next() {
		var s scored
		if err := rows.Scan(&s.id, &s.score); err != nil {
			return nil, 0, err
		}
		out = append(out, s)
	}
	return out, len(out), rows.Err()
}

func (b *Backend) applySubjectBoost(ctx context.Context, hits []vector.FusedHit, terms []string, boost float64) error {
	if len(hits) == 0 {
		return nil
	}
	ids := make([]int64, len(hits))
	for i, h := range hits {
		ids[i] = h.MessageID
	}
	inSQL, inArgs := inClause("id", ids)
	rows, err := b.db.QueryContext(ctx,
		fmt.Sprintf("SELECT id, COALESCE(subject, '') FROM messages WHERE %s", inSQL), inArgs...)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	subj := map[int64]string{}
	for rows.Next() {
		var id int64
		var s string
		if err := rows.Scan(&id, &s); err != nil {
			return err
		}
		subj[id] = strings.ToLower(s)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for i := range hits {
		s := subj[hits[i].MessageID]
		for _, t := range terms {
			if t != "" && strings.Contains(s, t) {
				hits[i].RRFScore *= boost
				hits[i].SubjectBoosted = true
				break
			}
		}
	}
	return nil
}

// ---- filter -------------------------------------------------------------

// filterClause builds the AND-prefixed SQL fragment and args for a Filter,
// operating against the `m` (messages) alias. An empty filter yields ("", nil).
func (b *Backend) filterClause(f vector.Filter) (string, []any) {
	var sb strings.Builder
	var args []any

	if len(f.SourceIDs) > 0 {
		in, a := inClause("m.source_id", f.SourceIDs)
		sb.WriteString(" AND " + in)
		args = append(args, a...)
	}
	addGroups := func(recipientType string, groups [][]int64) {
		for _, g := range groups {
			if len(g) == 0 {
				continue
			}
			in, a := inClause("mr.participant_id", g)
			sb.WriteString(fmt.Sprintf(
				" AND EXISTS (SELECT 1 FROM message_recipients mr"+
					" WHERE mr.message_id = m.id AND mr.recipient_type = '%s' AND %s)", recipientType, in))
			args = append(args, a...)
		}
	}
	addGroups("from", f.SenderGroups)
	addGroups("to", f.ToGroups)
	addGroups("cc", f.CcGroups)
	addGroups("bcc", f.BccGroups)
	for _, g := range f.LabelGroups {
		if len(g) == 0 {
			continue
		}
		in, a := inClause("ml.label_id", g)
		sb.WriteString(" AND EXISTS (SELECT 1 FROM message_labels ml WHERE ml.message_id = m.id AND " + in + ")")
		args = append(args, a...)
	}
	if f.HasAttachment != nil {
		sb.WriteString(" AND m.has_attachments = ?")
		args = append(args, boolToInt(*f.HasAttachment))
	}
	if f.After != nil {
		sb.WriteString(" AND m.sent_at >= ?")
		args = append(args, *f.After)
	}
	if f.Before != nil {
		sb.WriteString(" AND m.sent_at < ?")
		args = append(args, *f.Before)
	}
	if f.LargerThan != nil {
		sb.WriteString(" AND m.size_estimate > ?")
		args = append(args, *f.LargerThan)
	}
	if f.SmallerThan != nil {
		sb.WriteString(" AND m.size_estimate < ?")
		args = append(args, *f.SmallerThan)
	}
	for _, sub := range f.SubjectSubstrings {
		sb.WriteString(` AND m.subject LIKE ? ESCAPE '\'`)
		args = append(args, "%"+escapeLike(sub)+"%")
	}
	return sb.String(), args
}

// ---- helpers ------------------------------------------------------------

func distinctMessageIDs(chunks []vector.Chunk) []int64 {
	seen := map[int64]bool{}
	var out []int64
	for _, c := range chunks {
		if !seen[c.MessageID] {
			seen[c.MessageID] = true
			out = append(out, c.MessageID)
		}
	}
	return out
}

func inClause(col string, ids []int64) (string, []any) {
	if len(ids) == 0 {
		return "1=0", nil // matches nothing
	}
	ph := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	return col + " IN (" + ph + ")", args
}

func escapeLike(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return r.Replace(s)
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func capList(s []scored, k int) []scored {
	if len(s) > k {
		return s[:k]
	}
	return s
}

func capHits(h []vector.Hit, k int) []vector.Hit {
	if len(h) > k {
		return h[:k]
	}
	return h
}
