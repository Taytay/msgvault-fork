package localembed_test

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/vector/localembed"
)

func dot(a, b []float32) float64 {
	var s float64
	for i := range a {
		s += float64(a[i]) * float64(b[i])
	}
	return s
}

func TestEmbedder(t *testing.T) {
	e := localembed.New(64)
	require.Equal(t, 64, e.Dimension())
	ctx := context.Background()

	out, err := e.Embed(ctx, []string{
		"invoice from acme corp", // 0
		"acme invoice payment",   // 1: shares "invoice","acme" with 0
		"lunch plans on friday",  // 2: shares nothing with 0
	})
	require.NoError(t, err)
	require.Len(t, out, 3)

	for _, v := range out {
		require.Len(t, v, 64)
		assert.InDelta(t, 1.0, math.Sqrt(dot(v, v)), 1e-5, "vectors are L2-normalized")
	}

	// Cosine (= dot for unit vectors): lexically similar texts are closer.
	cosShared := dot(out[0], out[1])
	cosUnrelated := dot(out[0], out[2])
	assert.Greater(t, cosShared, cosUnrelated, "shared-token texts should be nearer")

	// Deterministic: same input → identical vector.
	again, err := e.Embed(ctx, []string{"invoice from acme corp"})
	require.NoError(t, err)
	assert.Equal(t, out[0], again[0])

	// Empty input → zero vector (no tokens to hash).
	zero, err := e.Embed(ctx, []string{"   !!!   "})
	require.NoError(t, err)
	assert.InDelta(t, 0.0, math.Sqrt(dot(zero[0], zero[0])), 1e-9)
}
