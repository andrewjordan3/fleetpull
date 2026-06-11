# fleetpull — Design Document

**Status:** Design settled through module layout. No code written.
**Working name:** `fleetpull` (PyPI availability confirmed 2026-06-10 for `fleetpull`, `fleetloader`, `telematics`, `telematics-io`; `fleetpull` selected — describes exactly what the package does and nothing more).
**Relationship to fleet-telemetry-hub:** New package, not a rewrite. fleet-telemetry-hub remains in production untouched while fleetpull is built.

---

## 1. Purpose and Scope

fleetpull retrieves fleet telematics data from provider APIs and delivers it as
typed, dtype-coerced, lightly normalized tabular output that stays as close to
the raw API responses as is reasonable.

**In scope**

- Fetching from provider APIs (Motive, Samsara, GeoTab when access lands; extensible to others)
- Dtype coercion and structural normalization (flattening) so output is predictable
- DataFrame output via programmatic API and CLI
- Config-driven incremental updates across multiple endpoints
- Multi-threaded fetching (backfill speed, concurrent endpoints)
- Exact-duplicate dedup at write time (fetch hygiene — see §6)

**Out of scope (non-goals)**

- Merging data across endpoints or providers
- Unified cross-provider schema
- Any assumed end use; downstream processing is the consumer's concern
- Semantic / event-id deduplication (payload-variant collapsing belongs to consumers)
- Loading into warehouses (no L; BigQuery et al. consume the parquet externally)

**Salvaged from fleet-telemetry-hub** (the well-designed Tier 2 layer):

- `EndpointDefinition` abstraction (self-describing endpoints: auth, pagination, request building, response parsing)
- Provider-agnostic HTTP client patterns (retry/backoff, pagination transparency, proxy/SSL handling)
- Endpoint registry
- Pydantic response model library for Motive and Samsara (ported as-is; they stay pure API mirrors)
- Config/credentials handling patterns

**Dropped from fleet-telemetry-hub:** unified schema, flatten functions in
`operations/`, merge logic, `TelemetryPipeline` / `PartitionedTelemetryPipeline`
orchestrators.

---

## 2. Core Technology Decisions

| Decision | Choice | Rationale |
|---|---|---|
| DataFrame engine | **Polars** (not pandas, not DuckDB) | Strict per-column schemas derivable from Pydantic models; clean parquet writer with no pandas-metadata problem (eliminates the canonical-reader issue); native list/struct columns. DuckDB's strength is querying/merging existing parquet — out of scope. Consumers may use DuckDB on our output. Both engines do not ship in the package. |
| Concurrency | **Threads** (`ThreadPoolExecutor`), not asyncio | Work is IO-bound HTTP; async infects every consumer-facing signature. Threads keep per-fetch code synchronous and simple. |
| Validation/config | Pydantic 2.x, `frozen=True`, `extra='forbid'`, `validate_default=True` | House standard. |
| Operational state | **SQLite** (WAL mode), single db at dataset root | Source of truth for watermarks/cursors, run ledger, work units. See §5. |
| HTTP | httpx with explicit timeouts, retry with backoff, resilience to corporate TLS-intercepting proxies (Zscaler-class) | Carried over from fleet-telemetry-hub. |

---

## 3. Output Contract and Storage Layout

One schema per (provider, endpoint). No exceptions, no cross-endpoint merging.

Each endpoint gets its own folder containing its data and a human-readable
metadata file:

```
data/
  motive/
    vehicles/              # single-parquet strategy
      data.parquet
      metadata.json
    vehicle_locations/     # date-partitioned strategy (breadcrumb-scale)
      date=2026-06-01/part.parquet
      date=2026-06-02/part.parquet
      metadata.json
  samsara/
    trips/
      ...
```

**Storage strategy is declared on the endpoint definition, not inferred:**

- `single` — one parquet file; merge is full read-modify-write. Fine for low-volume endpoints (on the order of 10–15k rows/day or less).
- `date_partitioned` — hive-style `date=YYYY-MM-DD` partitions; merge touches only partitions overlapping the fetch window. Required for breadcrumb-scale endpoints. Hive layout is read natively by BigQuery external tables and `pl.scan_parquet`.

`metadata.json` is a **generated human-readable snapshot**, written from SQLite
contents at the end of each successful run. It is never read by the program.
SQLite is the single source of truth (see §5) — no dual-write divergence.

---

## 4. Incremental Model

Per-endpoint incremental state is an **opaque cursor, not a datetime** — a
tagged union:

- `DateWatermark` — Motive/Samsara style: resume from `watermark - lookback`.
- `FeedToken` — GeoTab `GetFeed` style: resume from `fromVersion`/`toVersion` token. No date windows exist for these endpoints.

Each endpoint definition declares which strategy it uses. This is the single
biggest architectural improvement over fleet-telemetry-hub, whose
`latest_data_date - lookback` assumption cannot represent GeoTab.

**Merge semantics (watermark endpoints): delete-by-window, then append.**

1. Fetch window `[start, end]` from the API.
2. In existing storage, delete every row whose event timestamp falls inside `[start, end]`.
3. Append the fresh fetch.

The window is the unit of truth: whatever the API returns for a window
replaces what was held for that window. This handles late-arriving records and
payload-drift updates (providers have been observed returning the same event
with end timestamps drifting by milliseconds-to-seconds across fetches) with
no event-id logic.

**Merge semantics (feed-token endpoints): append-only + exact dedup.** No
window exists to delete.

**Never** overwrite storage with only the current window. Incremental means the
dataset stays complete and current.

---

## 5. SQLite Operational State

One database at the dataset root. WAL mode. Short `busy_timeout`. Owns:

- **Watermarks/cursors** per (provider, endpoint) — the tagged-union state from §4
- **Run ledger** — run id, provider, endpoint, window/cursor range, status, row counts, duration
- **Work units** — backfill decomposes into (endpoint, date-chunk) or (endpoint, vehicle, date-chunk) units; threads claim and complete them; a crash mid-backfill resumes from unclaimed/failed units instead of refetching everything

Rules:

- Transactions are tiny: claim unit → commit; finish unit → commit. **Never hold a transaction across an HTTP call.**
- SQLite is local-disk only; not designed for network filesystems.

**Crash-safety ordering:** write parquet first (temp file + atomic rename),
commit watermark/cursor second. A crash between the two causes a refetch of the
window on the next run, and delete-by-window merge makes that refetch
idempotent. At-least-once fetching + idempotent merge = exactly-once data,
with no transactional coupling between SQLite and the filesystem.

**Writer discipline:** fetch workers run in parallel, but parquet merge per
endpoint is **single-writer**. Fetch workers produce record batches into a
queue; one writer per endpoint drains and merges. Date-partitioned endpoints
may parallelize writes *across* partitions (each partition is an independent
file), never within one.

---

## 6. Deduplication Policy

- **Exact-duplicate dedup at write time: in scope, default ON** (config flag to disable for truly-raw output). Chunk-seam duplication and pagination drift are structural artifacts of *our* fetching, not of the provider's data; "the result comes out the way one expects" includes not handing consumers rows our pagination duplicated.
- **Semantic / event-id dedup: out of scope.** Same-key-different-payload collapsing is a consumer concern. (Delete-by-window merge already resolves most payload drift within refetched windows as a side effect.)

---

## 7. Rate Limiting and Concurrency

### Two independent controls per quota scope

| Control | Purpose | Mechanism |
|---|---|---|
| Rate limit | How many requests may *start* per time window | Token bucket |
| Concurrency limit | How many requests may be *in flight* at once | Semaphore |

Config:

```yaml
rate_limits:
  motive:
    requests_per_period: 100
    period_seconds: 60
    burst: 20          # burst = bucket CAPACITY (cold start may fire 20
                       # immediately, then settle to requests_per_period/period)
    max_concurrency: 5
  samsara:
    requests_per_period: 200
    period_seconds: 60
    burst: 40
    max_concurrency: 8
  geotab:
    requests_per_period: 60
    period_seconds: 60
    burst: 10
    max_concurrency: 4
```

Refill rate = `requests_per_period / period_seconds`, lazy refill on acquire,
capped at `burst`. All timing uses `time.monotonic()`, never wall clock.

### Placement: transport boundary, not orchestrator

A shared `RateLimiterRegistry` keyed by **quota scope string** supplied by the
endpoint definition (`endpoint.quota_scope`, default = provider name). v1
ships one scope per provider, but Samsara documents per-endpoint limits, so
the key is a string from day one — future scope splits are config + one
endpoint field, not a redesign.

The **client** consults the registry immediately before every HTTP request.
The orchestrator never touches the limiter. Quota limits are therefore
respected regardless of caller: config runner, manual Python, tests,
notebooks, custom user executors.

### Per-provider executors (starvation fix)

A single shared thread pool + blocking `request_slot()` causes cross-provider
starvation: a Motive 429 penalty parks workers holding pool slots, stalling
Samsara/GeoTab work. Therefore: **one executor per enabled provider, sized
`max_workers = max_concurrency`.** The semaphore inside the limiter is then
redundant in the orchestrated path but is kept as belt-and-suspenders — it
protects the invariant for any caller outside the orchestrator.

### Acquisition protocol

1. Acquire **semaphore first**, then token. (Token-first wastes start-rate permission while blocked on concurrency; semaphore-first is harmless — an idle thread is not an open connection.)
2. Fire request.
3. Release semaphore when the response completes (context manager; exceptions release it).

### Hard rules

- **Every HTTP attempt consumes a token.** Not every logical task, not every page window — every actual HTTP request. Retries each pass through `request_slot()` again.
- **Every page is an attempt.** `request_slot()` wraps the single httpx call *inside* the pagination loop, never around the loop. (This rule regresses silently if the pagination iterator is refactored — it lives here so it doesn't.)
- **429 / Retry-After penalizes the whole quota scope:** `pause_until = max(pause_until, monotonic() + penalty_seconds)` — max-merged, never overwritten with a smaller penalty.
- `request_slot()` checks penalty **before** bucket tokens (no token consumption while the scope is globally paused).
- Retry policy logic may live in the retry layer, but **Retry-After waiting is represented in the shared limiter**, never as a local sleep — otherwise only the thread that saw the 429 learns the penalty.
- No scattered `sleep()` calls in endpoint code.

### Implementation notes

- One `threading.Condition` per limiter guards both `pause_until` and the token count. `request_slot()` loops: `while paused or tokens < 1: cond.wait(timeout=min(time_until_unpause, time_until_next_token))`, recomputing on every wake (spurious wakeups harmless by construction). `Condition.wait()` releases the lock while waiting — this is the sanctioned way to "sleep"; a plain `time.sleep()` while holding a `Lock` is a bug.
- `penalize()` extends `pause_until` and calls `notify_all()` so waiters recompute against the new penalty instead of firing into a paused scope when their token math says go.

Interface:

```python
class QuotaScopeLimiter:
    def request_slot(self) -> AbstractContextManager[None]: ...  # blocks; yields in-flight slot
    def penalize(self, seconds: float) -> None: ...              # scope-wide pause, max-merged
```

---

## 8. Records, Flattening, and Schema Derivation

**Models stay pure API mirrors.** Ported Pydantic models carry no use-case
logic. Flattening and schema derivation are generic transforms in
`records.py`, written once against Pydantic introspection — this is what
makes GeoTab cheap: define models + endpoints, get flattening and schema
derivation for free.

**Flattening: default ON.** Nested objects flatten to underscore-joined
columns. Per-endpoint opt-out exists for the (currently hypothetical) case
where nesting genuinely helps. Arrays cannot flatten without exploding rows;
default representation is Polars list columns, overridable per endpoint.
The line is structural, never semantic.

**Schema pipeline (`records.py` contract, fixed from day one):**

1. **Auto derivation** — Pydantic model → Polars schema (the happy path)
2. **`schema_overrides: dict[str, pl.DataType]`** — per-endpoint escape hatch for derivation gaps
3. **`coercion_overrides`** — per-endpoint value-level fixes (e.g. Motive's stringly-typed numerics)
4. **Required-column validation** — fail loudly when an endpoint's output is missing declared columns

Assume auto derivation will be incomplete; the overrides are part of the
public internal contract, not a later retrofit.

---

## 9. Public API and CLI

**Programmatic:**

- `iter_records(endpoint, **params)` — typed iterator of Pydantic models, pagination transparent. (Renamed from fleet-telemetry-hub's `fetch_all`, whose "all" misleadingly suggested all endpoints rather than all pages.) This is the escape hatch for consumers who don't want Polars; the dataframe path is built on top of it.
- DataFrame retrieval per endpoint (built on `iter_records` + `records.py`)
- Read path over managed storage (single or partitioned) returning a dataframe

**CLI — two verbs, no more:**

- `fetch` — one provider/endpoint/window → parquet or stdout dataframe
- `sync` — config-driven, multi-endpoint, incremental (work units, executors, writers)

---

## 10. Module Layout

```
fleetpull/
  config.py        # Pydantic config models + YAML loader (providers, rate_limits, storage, sync plan)
  limits.py        # QuotaScopeLimiter, RateLimiterRegistry
  client.py        # HTTP transport, retry policy, limiter consultation, pagination iterator
  network/
    truststore_context.py  # SSLContext factory backed by the OS trust store (Zscaler-class proxies)
  timing/
    clock.py       # injectable Clock Protocol; SystemClock and FrozenClock implementations
  endpoints/
    base.py        # EndpointDefinition ABC: auth, pagination style, quota_scope,
                   #   incremental strategy (watermark | feed_token), storage strategy
    motive.py
    samsara.py
    geotab.py      # stub until access lands
  models/          # response Pydantic models per provider (largely ported from fleet-telemetry-hub)
  records.py       # flattening; Pydantic → Polars schema derivation + overrides + validation
  storage.py       # Polars merge/write: delete-by-window + append; single vs partitioned
  state.py         # SQLite: watermarks/cursors, run ledger, work units
  orchestrator.py  # sync planner: builds work units, per-provider executors, per-endpoint writer threads
  cli.py           # fetch, sync
```

The package root holds user-facing modules only; internal code lives in
subpackages. Open question (settle before Prompt 1): the flat placement of the
remaining internal modules (`limits.py`, `client.py`, `records.py`,
`storage.py`, `state.py`, `orchestrator.py`) predates that rule and needs
restructuring or an explicit exemption.

Boundary rules:

- `storage.py` knows nothing about state; `state.py` knows nothing about parquet. The orchestrator sequences them (parquet-then-watermark ordering, §5).
- `client.py` owns the pagination iterator, because pagination, retry, and limiter consultation are interleaved per-request concerns; splitting them across modules is how the token-per-attempt / token-per-page rules get violated.
- The orchestrator never touches the limiter (§7).

---

## 11. House Code Standards (carried into this package)

- Asserts only in tests, never production code
- Annotated locals; explicit type hints everywhere
- No real VINs / internal fleet identifiers in committed files
- Docstrings with Args/Returns/Raises/Side Effects
- `logging.getLogger(__name__)`; no `print` in production code
- Explicit timeouts on all network calls; specific exception handling
- Blast-radius minimization over DRY where coupling risk is real
- `StrEnum` for enums

---

## 12. Open Questions

- GeoTab specifics pending API access: auth model, `GetFeed` semantics in practice, real rate limits, which entities map to which storage strategies
- Real rate-limit values for Motive/Samsara (YAML numbers above are placeholders)
- Whether any endpoint actually warrants the flattening opt-out
- Per-endpoint quota scopes for Samsara (config-only change when needed)
- Final name confirmation before repo creation (`fleetpull` is the working selection)

## 13. Next Steps

1. Review/amend this document
2. Repo scaffold + first CC prompts, in dependency order: `limits.py` → `client.py` → `endpoints/base.py` → `records.py` → `storage.py` → `state.py` → `orchestrator.py` → `cli.py`
3. Port Motive/Samsara models and endpoint definitions onto the new base
4. GeoTab integration when access lands
