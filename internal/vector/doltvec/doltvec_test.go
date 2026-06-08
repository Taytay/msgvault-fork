package doltvec_test

import (
	"context"
	"os"
	"strings"
	"testing"

	assertpkg "github.com/stretchr/testify/assert"
	requirepkg "github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/testutil/storetest"
	"go.kenn.io/msgvault/internal/vector"
	"go.kenn.io/msgvault/internal/vector/doltvec"
	"go.kenn.io/msgvault/internal/vector/hybrid"
)

// stubEmbed returns a fixed query vector regardless of input, so the test can
// drive hybrid.Engine without a real embedding endpoint.
type stubEmbed struct{ vec []float32 }

func (s stubEmbed) Embed(_ context.Context, inputs []string) ([][]float32, error) {
	out := make([][]float32, len(inputs))
	for i := range inputs {
		out[i] = s.vec
	}
	return out, nil
}

// TestDoltVec exercises the Dolt vector backend end-to-end against a live Dolt
// server: schema setup, generation lifecycle, Upsert, ANN Search, and a
// hybrid (FULLTEXT + ANN, RRF-fused) query driven by the real hybrid.Engine.
// Gated on MSGVAULT_TEST_DB=mysql://...
func TestDoltVec(t *testing.T) {
	testDB := os.Getenv("MSGVAULT_TEST_DB")
	if !strings.HasPrefix(testDB, "mysql://") && !strings.HasPrefix(testDB, "dolt://") {
		t.Skip("set MSGVAULT_TEST_DB=mysql://... to run the Dolt vector backend test")
	}
	require := requirepkg.New(t)
	assert := assertpkg.New(t)
	ctx := context.Background()

	f := storetest.New(t) // Dolt-backed store + source + conversation
	require.True(f.Store.IsMySQL())

	// Three messages in distinct "topics"; toy 3-d embeddings encode the topic.
	subjects := []string{
		"invoice from acme corp due next week",
		"quarterly revenue report and figures",
		"lunch plans for friday afternoon",
	}
	vecs := [][]float32{{1, 0, 0}, {0, 1, 0}, {0, 0, 1}}
	ids := make([]int64, len(subjects))
	for i, s := range subjects {
		ids[i] = f.NewMessage().
			WithSourceMessageID(strings.Fields(s)[0]+"-msg").
			WithSubject(s).
			WithSnippet(s).
			Create(t, f.Store)
	}

	be, err := doltvec.Open(ctx, doltvec.Options{DB: f.Store.DB(), Dimension: 3})
	require.NoError(err)
	defer func() { _ = be.Close() }()

	gen, err := be.CreateGeneration(ctx, "toy-model", 3, "")
	require.NoError(err)
	require.Positive(int64(gen))

	chunks := make([]vector.Chunk, len(ids))
	for i, mid := range ids {
		chunks[i] = vector.Chunk{MessageID: mid, ChunkIndex: 0, Vector: vecs[i], SourceCharLen: len(subjects[i])}
	}
	require.NoError(be.Upsert(ctx, gen, chunks))

	// Idempotent re-upsert must not inflate message_count.
	require.NoError(be.Upsert(ctx, gen, chunks))
	stats, err := be.Stats(ctx, gen)
	require.NoError(err)
	assert.Equal(int64(3), stats.EmbeddingCount, "3 distinct messages embedded")

	require.NoError(be.ActivateGeneration(ctx, gen))
	active, err := be.ActiveGeneration(ctx)
	require.NoError(err)
	assert.Equal(gen, active.ID)
	assert.Equal(3, active.Dimension)

	// Pure ANN: a query near the "invoice" vector returns it first.
	hits, err := be.Search(ctx, gen, []float32{0.9, 0.1, 0.0}, 3, vector.Filter{})
	require.NoError(err)
	require.NotEmpty(hits)
	assert.Equal(ids[0], hits[0].MessageID, "nearest vector is the invoice message")
	assert.Equal(1, hits[0].Rank)

	// LoadVector round-trips the stored embedding.
	got, err := be.LoadVector(ctx, ids[0])
	require.NoError(err)
	require.Len(got, 3)
	assert.InDelta(1.0, got[0], 1e-6)

	// Hybrid via the real engine: keyword "invoice" + vector near invoice both
	// point to the same message, so RRF puts it first.
	eng := hybrid.NewEngine(be, f.Store.DB(), stubEmbed{vec: []float32{0.9, 0.1, 0.0}},
		hybrid.Config{RRFK: 60, KPerSignal: 10, SubjectBoost: 2.0})

	res, _, err := eng.Search(ctx, hybrid.SearchRequest{
		Mode: hybrid.ModeHybrid, FreeText: "invoice", Limit: 5,
	})
	require.NoError(err)
	require.NotEmpty(res)
	assert.Equal(ids[0], res[0].MessageID, "fused top hit is the invoice message")

	// Vector-only mode also works through the engine.
	resV, _, err := eng.Search(ctx, hybrid.SearchRequest{
		Mode: hybrid.ModeVector, FreeText: "anything", Limit: 3,
	})
	require.NoError(err)
	require.NotEmpty(resV)
	assert.Equal(ids[0], resV[0].MessageID)

	// Filter narrows results: restrict to a non-matching source -> no hits.
	none, err := be.Search(ctx, gen, []float32{0.9, 0.1, 0.0}, 3,
		vector.Filter{SourceIDs: []int64{f.Source.ID + 9999}})
	require.NoError(err)
	assert.Empty(none, "filter excluding all sources yields no hits")
}
