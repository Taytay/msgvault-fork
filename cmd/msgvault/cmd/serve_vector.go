//go:build sqlite_vec

package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"

	"go.kenn.io/msgvault/internal/store"
	"go.kenn.io/msgvault/internal/vector"
	"go.kenn.io/msgvault/internal/vector/doltvec"
	"go.kenn.io/msgvault/internal/vector/embed"
	"go.kenn.io/msgvault/internal/vector/hybrid"
	"go.kenn.io/msgvault/internal/vector/sqlitevec"
)

// setupVectorFeatures opens the vector backend and builds the hybrid engine,
// embed worker, and enqueuer used by the serve daemon and the MCP command.
// Returns (nil, nil) when cfg.Vector.Enabled is false. The returned Close
// function must be called on shutdown.
//
// Backend selection follows the system of record:
//   - Dolt (mysql://): doltvec, co-located with the messages. The shared
//     embed.Worker drives it via an injected doltvec.Queue. (Daemon
//     auto-activation is gated behind a sqlite-specific pending count, so it
//     is skipped on Dolt for now — embeddings still build; activation is a
//     follow-up via Backend.Stats.)
//   - SQLite file: sqlitevec (separate vectors.db, ATTACH-based FusedSearch).
//
// s is the already-opened store; the vector backend follows its system of
// record.
func setupVectorFeatures(ctx context.Context, s *store.Store) (*vectorFeatures, error) {
	if !cfg.Vector.Enabled {
		return nil, nil //nolint:nilnil // vector disabled: callers nil-check vf; (nil, nil) means "no features, no error"
	}
	mainDB := s.DB()
	if s.Backend() == store.BackendPostgreSQL {
		return nil, fmt.Errorf(
			"vector features are SQLite-only; set [vector] enabled = false to use msgvault with PostgreSQL (vector support is planned for PR4)")
	}
	if err := cfg.Vector.Validate(); err != nil {
		return nil, fmt.Errorf("vector config: %w", err)
	}

	client := embed.NewClient(embed.Config{
		Endpoint:   cfg.Vector.Embeddings.Endpoint,
		APIKey:     cfg.Vector.Embeddings.APIKey(),
		Model:      cfg.Vector.Embeddings.Model,
		Dimension:  cfg.Vector.Embeddings.Dimension,
		Timeout:    cfg.Vector.Embeddings.Timeout,
		MaxRetries: cfg.Vector.Embeddings.MaxRetries,
	})

	hybridCfg := hybrid.Config{
		ExpectedFingerprint: cfg.Vector.GenerationFingerprint(),
		RRFK:                cfg.Vector.Search.RRFK,
		KPerSignal:          cfg.Vector.Search.KPerSignal,
		SubjectBoost:        cfg.Vector.Search.SubjectBoost,
	}

	// Resolve the effective backend. "auto" follows the system of record.
	kind := cfg.Vector.Backend
	if kind == "" || kind == "auto" {
		if s.Backend() == store.BackendDolt {
			kind = "dolt"
		} else {
			kind = "sqlite-vec"
		}
	}

	// Dolt backend: search served directly from the system of record.
	if kind == "dolt" {
		if s.Backend() != store.BackendDolt {
			return nil, fmt.Errorf("vector.backend=\"dolt\" requires a Dolt (mysql://) store; got the %s backend", s.Backend())
		}
		backend, err := doltvec.Open(ctx, doltvec.Options{
			DB:        mainDB,
			Dimension: cfg.Vector.Embeddings.Dimension,
		})
		if err != nil {
			return nil, fmt.Errorf("open dolt vector backend: %w", err)
		}
		// The shared embed.Worker drives the Dolt backend by injecting a
		// doltvec.Queue (the only sqlite-coupled collaborator). The main-DB
		// text query and Backend.Upsert are already backend-agnostic.
		worker := embed.NewWorker(newWorkerDeps(backend, doltvec.NewQueue(mainDB), nil, mainDB, client))
		return &vectorFeatures{
			Backend:      backend,
			HybridEngine: hybrid.NewEngine(backend, mainDB, client, hybridCfg),
			Enqueuer:     doltvec.NewEnqueuer(mainDB),
			Worker:       worker,
			Cfg:          cfg.Vector,
			Close:        backend.Close,
		}, nil
	}

	// SQLite backend: separate vectors.db with the sqlite-vec extension.
	if s.Backend() == store.BackendDolt {
		return nil, fmt.Errorf(
			"vector.backend=\"sqlite-vec\" cannot run against a Dolt store; set [vector].backend = \"dolt\" or \"auto\"")
	}
	cache, ok := s.AnalyticsCache()
	if !ok {
		return nil, fmt.Errorf("vector.backend=\"sqlite-vec\" requires a local SQLite store")
	}
	if err := sqlitevec.RegisterExtension(); err != nil {
		return nil, fmt.Errorf("register sqlite-vec: %w", err)
	}

	vecPath := cfg.Vector.DBPath
	if vecPath == "" {
		vecPath = filepath.Join(cfg.Data.DataDir, "vectors.db")
	}
	backend, err := sqlitevec.Open(ctx, sqlitevec.Options{
		Path:      vecPath,
		MainPath:  cache.SourcePath(),
		Dimension: cfg.Vector.Embeddings.Dimension,
		MainDB:    mainDB,
	})
	if err != nil {
		return nil, fmt.Errorf("open vectors.db: %w", err)
	}

	worker := embed.NewWorker(newWorkerDeps(backend, nil, backend.DB(), mainDB, client))

	return &vectorFeatures{
		Backend:      backend,
		HybridEngine: hybrid.NewEngine(backend, mainDB, client, hybridCfg),
		Enqueuer:     embed.NewEnqueuer(backend.DB()),
		Worker:       worker,
		Cfg:          cfg.Vector,
		Close:        backend.Close,
	}, nil
}

// newWorkerDeps assembles embed.WorkerDeps from the current config plus the
// per-backend collaborators. queue may be nil (NewWorker then defaults to a
// sqlite-vec Queue over vectorsDB); inject a doltvec.Queue for the Dolt path.
func newWorkerDeps(backend vector.Backend, queue embed.PendingQueue, vectorsDB, mainDB *sql.DB, client embed.EmbeddingClient) embed.WorkerDeps {
	return embed.WorkerDeps{
		Backend:   backend,
		Queue:     queue,
		VectorsDB: vectorsDB,
		MainDB:    mainDB,
		Client:    client,
		Preprocess: embed.PreprocessConfig{
			StripQuotes:        cfg.Vector.Preprocess.StripQuotesEnabled(),
			StripSignatures:    cfg.Vector.Preprocess.StripSignaturesEnabled(),
			StripHTML:          cfg.Vector.Preprocess.StripHTMLEnabled(),
			StripBase64:        cfg.Vector.Preprocess.StripBase64Enabled(),
			StripURLTracking:   cfg.Vector.Preprocess.StripURLTrackingEnabled(),
			CollapseWhitespace: cfg.Vector.Preprocess.CollapseWhitespaceEnabled(),
		},
		MaxInputChars:   cfg.Vector.Embeddings.MaxInputChars,
		BatchSize:       cfg.Vector.Embeddings.BatchSize,
		EmbedTimeout:    cfg.Vector.Embeddings.Timeout,
		EmbedMaxRetries: cfg.Vector.Embeddings.MaxRetries,
		Log:             logger,
	}
}
