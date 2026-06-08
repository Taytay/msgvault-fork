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
	"go.kenn.io/msgvault/internal/vector/embed"
	"go.kenn.io/msgvault/internal/vector/hybrid"
	"go.kenn.io/msgvault/internal/vector/localembed"
)

// TestDoltVec_EmbedWorker drives the FULL Dolt embed+search pipeline with a
// real (CPU stand-in) embedding model: CreateGeneration seeds the pending
// queue from live messages, the shared embed.Worker — wired with a
// doltvec.Queue and the localembed embedder — drains the queue into the
// doltvec backend, the generation is activated, and both a raw ANN Search and
// a hybrid engine query retrieve the lexically-matching message.
//
// This proves the decoupling: one Worker, swapped only at the PendingQueue
// seam, embeds against Dolt. Gated on MSGVAULT_TEST_DB=mysql://...
func TestDoltVec_EmbedWorker(t *testing.T) {
	testDB := os.Getenv("MSGVAULT_TEST_DB")
	if !strings.HasPrefix(testDB, "mysql://") && !strings.HasPrefix(testDB, "dolt://") {
		t.Skip("set MSGVAULT_TEST_DB=mysql://... to run the Dolt embed-worker test")
	}
	require := requirepkg.New(t)
	assert := assertpkg.New(t)
	ctx := context.Background()
	const dim = 64

	f := storetest.New(t)
	require.True(f.Store.IsMySQL())

	subjects := []string{
		"invoice from acme corp due next week",
		"quarterly revenue report figures",
		"lunch plans for friday afternoon",
	}
	ids := make([]int64, len(subjects))
	for i, s := range subjects {
		ids[i] = f.NewMessage().
			WithSourceMessageID(strings.Fields(s)[0]+"-msg").
			WithSubject(s).
			WithSnippet(s).
			Create(t, f.Store)
	}

	be, err := doltvec.Open(ctx, doltvec.Options{DB: f.Store.DB(), Dimension: dim})
	require.NoError(err)
	defer func() { _ = be.Close() }()

	// CreateGeneration seeds vec_pending_embeddings from the live messages.
	gen, err := be.CreateGeneration(ctx, "localembed", dim, "")
	require.NoError(err)
	pending, err := be.Stats(ctx, gen)
	require.NoError(err)
	require.Equal(int64(len(ids)), pending.PendingCount, "all live messages seeded as pending")

	// The shared Worker, driving Dolt via the injected doltvec.Queue and the
	// deterministic CPU embedder.
	emb := localembed.New(dim)
	worker := embed.NewWorker(embed.WorkerDeps{
		Backend:       be,
		Queue:         doltvec.NewQueue(f.Store.DB()),
		MainDB:        f.Store.DB(),
		Client:        emb,
		MaxInputChars: 2000,
		BatchSize:     16,
	})

	res, err := worker.RunOnce(ctx, gen)
	require.NoError(err)
	assert.Equal(len(ids), res.Succeeded, "every message embedded")
	assert.Zero(res.Failed)

	// Queue drained, embeddings persisted.
	after, err := be.Stats(ctx, gen)
	require.NoError(err)
	assert.Zero(after.PendingCount, "pending queue drained")
	assert.Equal(int64(len(ids)), after.EmbeddingCount, "all messages embedded")

	require.NoError(be.ActivateGeneration(ctx, gen))

	// Raw ANN with a query embedded by the same model: "invoice acme" shares
	// tokens with message 0, so it ranks first.
	qvecs, err := emb.Embed(ctx, []string{"invoice acme"})
	require.NoError(err)
	hits, err := be.Search(ctx, gen, qvecs[0], 3, vector.Filter{})
	require.NoError(err)
	require.NotEmpty(hits)
	assert.Equal(ids[0], hits[0].MessageID, "ANN returns the invoice message first")

	// Hybrid engine driven by the same local embedder end-to-end.
	eng := hybrid.NewEngine(be, f.Store.DB(), emb,
		hybrid.Config{RRFK: 60, KPerSignal: 10, SubjectBoost: 2.0})
	fused, _, err := eng.Search(ctx, hybrid.SearchRequest{
		Mode: hybrid.ModeHybrid, FreeText: "invoice acme", Limit: 5,
	})
	require.NoError(err)
	require.NotEmpty(fused)
	assert.Equal(ids[0], fused[0].MessageID, "hybrid top hit is the invoice message")
}
