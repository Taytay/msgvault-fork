// Package localembed provides a deterministic, CPU-only, dependency-free
// embedding model. It exists so the embed/search pipeline can be exercised
// end-to-end in tests (and fully-offline setups) without a model download or
// an embeddings HTTP endpoint.
//
// It implements the same Embed(ctx, inputs) ([][]float32, error) contract as
// the production HTTP client, so it is a drop-in vector/embed EmbeddingClient
// (and hybrid.EmbeddingClient). Embeddings are produced by the "hashing
// trick": each token is hashed to a coordinate and a sign, contributions are
// summed, and the vector is L2-normalized. This captures *lexical* similarity
// (texts sharing tokens land near each other under cosine/L2) — enough to
// drive and verify the pipeline. It is NOT a semantic model; use a real
// embeddings endpoint for production search quality.
package localembed

import (
	"context"
	"hash/fnv"
	"math"
	"unicode"
)

// Embedder is a deterministic feature-hashing embedder of fixed dimension.
type Embedder struct {
	dim int
}

// New returns an Embedder producing dim-dimensional unit vectors. dim must be
// positive.
func New(dim int) *Embedder {
	if dim <= 0 {
		dim = 256
	}
	return &Embedder{dim: dim}
}

// Dimension reports the embedding dimension.
func (e *Embedder) Dimension() int { return e.dim }

// Embed returns one unit vector per input. It never errors and ignores ctx;
// the signature matches the production EmbeddingClient so it is a drop-in.
func (e *Embedder) Embed(_ context.Context, inputs []string) ([][]float32, error) {
	out := make([][]float32, len(inputs))
	for i, in := range inputs {
		out[i] = e.embedOne(in)
	}
	return out, nil
}

func (e *Embedder) embedOne(s string) []float32 {
	v := make([]float32, e.dim)
	forEachToken(s, func(tok string) {
		h := fnv.New64a()
		_, _ = h.Write([]byte(tok))
		sum := h.Sum64()
		idx := int(sum % uint64(e.dim))
		// Use a high bit for the sign so it is independent of idx.
		if sum&(1<<63) != 0 {
			v[idx]--
		} else {
			v[idx]++
		}
	})
	normalize(v)
	return v
}

// forEachToken lowercases s and yields maximal runs of letters/digits.
func forEachToken(s string, fn func(string)) {
	start := -1
	emit := func(end int) {
		if start >= 0 {
			fn(lower(s[start:end]))
			start = -1
		}
	}
	for i, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			if start < 0 {
				start = i
			}
		} else {
			emit(i)
		}
	}
	emit(len(s))
}

func lower(s string) string {
	b := []rune(s)
	for i, r := range b {
		b[i] = unicode.ToLower(r)
	}
	return string(b)
}

func normalize(v []float32) {
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}
	if sum == 0 {
		return
	}
	inv := float32(1.0 / math.Sqrt(sum))
	for i := range v {
		v[i] *= inv
	}
}
