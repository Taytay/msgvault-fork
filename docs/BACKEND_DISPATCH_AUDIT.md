# Backend-Dispatch Audit

An audit of every place msgvault asks "**what backend am I?**"
(`IsPostgresURL`, `IsMySQLURL`, `IsServerURL`, `Store.IsPostgreSQL()`,
`Store.IsMySQL()`, and the `IsPostgres bool` flag threaded through the query
engine) instead of asking "**can this backend do X?**".

## The principle

The codebase already states the right rule, in `internal/store/fts.go` and
`internal/store/dialect.go`:

> Full-text search is an OPTIONAL capability... Callers reach it via
> `Store.ftsIndexer()` (a type assertion)... so the common `Dialect` does not
> force every backend to answer FTS questions it has no answer for.

FTS is modeled as a **capability**: `s.ftsIndexer() (FTSIndexer, bool)`. The
backend that can't do it (Dolt) returns `false`; callers handle the `false`
case. The backend owns the knowledge of what it can do.

Everywhere else below, that knowledge has leaked into the **caller** as a
string sniff on a DSN or a backend-identity method. That is the smell: the
fact "the Parquet ETL is SQLite-only" is a property of the Parquet subsystem,
but it is enforced by `strings.HasPrefix(dbPath, "postgres://")` inside a
Cobra `RunE`. Nothing connects the limitation to the subsystem that owns it,
and nothing stops the next call site from forgetting the check.

## Categories

### A. Legitimate factory dispatch — NOT a smell (leave as-is)

Every system needs exactly one place that maps config → concrete
implementation. Looking at the DSN string is correct *here and only here*,
because there is no opened backend to ask yet.

| Location | What it does |
|---|---|
| `internal/store/store.go:136` `Open()` | `postgres://` → `openPostgres`, `mysql://`/`dolt://` → `openDolt`, else `openSQLite` |
| `internal/store/store.go:152` `OpenForTest()` | same dispatch, test tuning |
| `internal/store/store.go:288` `OpenReadOnly()` | same dispatch + Dolt read-only refusal |

The Dolt read-only refusal at `store.go:298` is backend knowledge, but it sits
*at the factory* where the backend is being constructed, so it is acceptable.

### B. Identity methods used as capability proxies — the core smell

Each of these asks "are you Postgres/MySQL?" and the **caller** encodes what
that backend can't do. Each maps to a real, nameable capability.

| Location | Check | Real capability |
|---|---|---|
| `cmd/.../verify.go:72`, `verify.go:298` (`runIntegrityCheck`) | `s.IsPostgreSQL()` | `IntegrityChecker` (PRAGMA integrity_check) |
| `cmd/.../deduplicate.go:546` (`backupDatabase`) | `s.IsPostgreSQL()` | `FileSnapshotBackup` (VACUUM INTO) |
| `cmd/.../create_subset.go:60` | `IsPostgresURL` | `FileSnapshotBackup` (ATTACH DATABASE) |
| `cmd/.../build_cache.go:73,76` | `IsPostgresURL` / `IsMySQLURL` | Parquet-cache eligibility |
| `cmd/.../tui.go:118` | `!s.IsPostgreSQL()` | Parquet-cache eligibility |
| `cmd/.../tui.go:194` (`cacheNeedsBuild`) | `IsPostgresURL` | Parquet-cache eligibility |
| `cmd/.../serve.go:128` | `!readOpts.IsPostgres` | Parquet-cache eligibility |
| `cmd/.../serve.go:396` | `IsPostgresURL` | Parquet-cache eligibility |

Note how five distinct call sites independently re-derive the single fact
"the Parquet cache is SQLite-only." That rule has no home; it is copy-pasted
as a guard.

### C. Query-engine identity flag plumbing — `IsPostgres bool`

The Store already knows its dialect, yet every read path threads a boolean
back in so the engine can re-derive what to do.

| Location | Pattern |
|---|---|
| `internal/query/postgres.go:51` `NewEngine(db, isPostgres)` | branch on bool → PG or SQLite engine |
| `internal/query/read_engine.go:15,49` `ReadEngineOptions.IsPostgres` | branch bypasses Parquet path |
| `cmd/.../search.go:314`, `list_domains.go:45`, `list_labels.go:45`, `list_senders.go:45`, `show_message.go:94`, `export_eml.go:101`, `export_attachments.go:52` | `query.NewEngine(s.DB(), s.IsPostgreSQL())` |
| `cmd/.../tui.go:133`, `mcp.go:72`, `serve.go:125` | `ReadEngineOptions{IsPostgres: s.IsPostgreSQL()}` |

`Store` has a dialect; it should be able to hand out a query engine (or expose
its query dialect) directly. Callers should never see a backend boolean.

Minor: `postgres.go:47` doc says "pass `store.IsPostgres()`" — that function
does not exist (the method is `IsPostgreSQL()`). Stale comment.

### D. Vector backend selection — `serve_vector.go` / `embed_vector.go`

Mixed: the `"auto"` resolution is legitimate factory logic, but the scattered
refusals are capability statements living in the caller.

| Location | Check | Note |
|---|---|---|
| `serve_vector.go:37`, `serve_vector_stub.go:23` | `IsPostgresURL` | "vector is SQLite-only" refusal |
| `serve_vector.go:64` | `IsMySQLURL` | `auto` → dolt vs sqlite-vec (factory; OK) |
| `serve_vector.go:73` | `!IsMySQLURL` | "dolt backend needs mysql:// store" |
| `serve_vector.go:98` | `IsMySQLURL` | "sqlite-vec can't run on Dolt" |
| `embed_vector.go:25` | `IsMySQLURL` | "embed worker is sqlite-vec-specific" |

### E. Command-level entry guard — `project.go`

| Location | Check | Note |
|---|---|---|
| `cmd/.../project.go:35` | `!IsMySQLURL` | `project` is inherently Dolt-only |

Defensible as a single entry guard for a Dolt-specific command, but it is
still a string sniff that would read better as a typed capability probe.

## Recommended direction

Follow the `FTSIndexer` precedent already in the tree. Collapse the ~25
identity checks into a handful of optional capability interfaces, obtained
from `Store` via `(Capability, bool)` accessors, with the backend that can't
do it supplying its own typed "unsupported" explanation.

The distinct capabilities behind the checks above:

1. **Parquet/local-file cache eligibility** (B, C-read-path) — the single
   most duplicated fact. One accessor, e.g. `s.LocalCacheSource() (…, bool)`,
   replaces five guards.
2. **File-snapshot backup** (VACUUM INTO + subset ATTACH) — `verify`'s
   sibling guards and `create-subset`.
3. **In-engine integrity check** — `verify`.
4. **Vector backend** — keep `auto` factory selection, move the per-backend
   refusals into the backends.
5. **Query dialect** — have `OpenReadEngine`/`NewEngine` take the `Store` (or a
   dialect it vends) instead of a backend boolean.

Suggested sequencing: start with `build_cache.go` and `embed_vector.go` (the
clearest cases, good templates), then the Parquet-eligibility cluster, then
the query-engine boolean, then vector refusals.

The factory dispatch in category A stays — that is where backend identity
legitimately lives.

## Resolution (implemented)

The refactor landed. Backend identity now lives in exactly two sanctioned
places, and behavioral differences are expressed as capabilities.

### Where identity is allowed

- `store.BackendOfDSN(dsn) Backend` — the single DSN sniffer. Used by the
  `Open` factory and the rare pre-open file-existence guard (`build-cache`,
  `create-subset`), where there is no opened store to ask yet.
- `Store.Backend() Backend` + `Backend.String()` — for the query-engine
  dialect factory (`query.NewEngineForStore`) and human-readable messages.

The old free functions `IsPostgresURL` / `IsMySQLURL` / `IsServerURL` are gone.

### Capabilities (the pattern to extend)

Optional capabilities are discovered via `Store` accessors returning
`(Capability, bool)`, exactly like the pre-existing `FTSIndexer`
(`fts.go`) and `VersionController` (`version.go`):

| Capability | Accessor | Implemented by | Replaces |
|---|---|---|---|
| `AnalyticsCache` | `Store.AnalyticsCache()` / `RequireAnalyticsCache()` | local-file SQLite | build-cache, cacheNeedsBuild, tui/serve cache, read-engine selection |
| `IntegrityChecker` | `Store.IntegrityChecker()` | local-file SQLite | `verify` |
| `SnapshotBackup` | `Store.SnapshotBackup()` | local-file SQLite | `deduplicate` backup |

`query.OpenReadEngine` now takes a `*store.Store` and decides the Parquet vs.
direct path purely from `AnalyticsCache` presence; the `IsPostgres bool`
option is gone. The seven `query.NewEngine(db, s.IsPostgreSQL())` call sites
collapsed to `query.NewEngineForStore(s)`.

`UnsupportedError` carries a backend-authored remediation hint (e.g. the Dolt
"run 'msgvault project'" advice), so refusing commands surface guidance
without branching on backend type.

### Adding a new backend

1. Add a `Backend` constant and a `BackendOfDSN` case (factory only).
2. Implement the `Dialect` interface (required SQL behavior).
3. Implement whichever capability interfaces the backend supports; the
   accessors hand them out and every call site Just Works. Implement none and
   the backend is still fully usable for core storage — the cache, integrity,
   backup, and FTS paths become clean no-ops or typed "unsupported" errors.

No command or query call site needs editing to onboard a backend.
