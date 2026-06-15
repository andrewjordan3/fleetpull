# fleetpull — Design Document

**Status:** Design settled through module layout; implementation underway (see §14 for build progress).
**Name:** `fleetpull` — final. Describes exactly what the package does and nothing more (PyPI availability confirmed 2026-06-10).
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
- Loading into warehouses — the package extracts and lightly transforms; it never performs a load step. Downstream systems (BigQuery et al.) consume the parquet externally.

**Salvaged from fleet-telemetry-hub** — the provider-API abstraction layer (endpoint definitions, HTTP client patterns, response models), not the predecessor's orchestration or schema-unification layers, which the "Dropped" list below covers:

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
| DataFrame engine | **Polars** (not pandas, not DuckDB) | Strict per-column schemas derivable from Pydantic models; clean parquet writer with no pandas-metadata problem (eliminates the canonical-reader issue); native list/struct columns. DuckDB's strength is querying/merging existing parquet — out of scope. Consumers may use DuckDB on our output. pandas and DuckDB do not ship in the package; polars is a core dependency. |
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

These two carriers live in `incremental/` — a pure, dependency-free leaf, so
an endpoint can name a strategy without importing the SQLite layer.
Serialization of a cursor to its SQLite form is owned by `state/`, not the
cursor.

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
capped at `burst`. All limiter timing flows through the injected
`Clock.monotonic_seconds()`, never wall clock.

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
- **429 / Retry-After penalizes the whole quota scope:** `pause_until = max(pause_until, clock.monotonic_seconds() + penalty_seconds)` — max-merged, never overwritten with a smaller penalty.
- `request_slot()` checks penalty **before** bucket tokens (no token consumption while the scope is globally paused).
- Retry policy logic may live in the retry layer, but **Retry-After waiting is represented in the shared limiter**, never as a local sleep — otherwise only the thread that saw the 429 learns the penalty.
- No scattered `sleep()` calls in endpoint code.

### Implementation notes

- One `threading.Condition` per limiter guards both `pause_until` and the token count. `request_slot()` loops on the condition under two invariants that must not regress: every wake recomputes its wait from scratch (spurious wakeups harmless by construction), and the penalty is waited out before any token is consumed. `Condition.wait()` releases the lock while waiting — this is the sanctioned way to "sleep"; a plain `time.sleep()` while holding a `Lock` is a bug.
- `penalize()` extends `pause_until` and calls `notify_all()` so waiters recompute against the new penalty instead of firing into a paused scope when their token math says go.

Interface:

```python
class QuotaScopeLimiter:
    def __init__(self, quota_scope: str, config: RateLimitConfig, clock: Clock = SystemClock()) -> None: ...
    def request_slot(self) -> AbstractContextManager[None]: ...  # blocks; yields in-flight slot
    def penalize(self, seconds: float) -> None: ...              # scope-wide pause, max-merged
```

The clock is injected (`fleetpull.timing.Clock`) so limiter tests are
deterministic.

### Retry policy (implemented: `config/retry.py`, `network/retry/decision.py`)

Pure policy the client consults after each retryable failure — no loop, no
sleep, no state. The answer travels as a frozen `RetryDecision`
(`should_retry`; `local_delay_seconds`, inert at 0.0 when not retrying)
rather than an overloaded `float | None` — the None-versus-0.0 distinction
is exactly the kind of subtle contract retry bugs breed in.

- **Who sleeps:** the limiter owns ALL rate-limit waiting — a 429 penalizes
  the shared quota scope and the next `request_slot()` waits it out, so
  RATE_LIMITED decisions never carry a local delay. Local sleeping exists
  only for TRANSIENT backoff: the policy computes the delay, the client
  performs the sleep.
- **Failure counts** are one-based within the current retryable category
  and independent per category — a RATE_LIMITED failure neither resets nor
  advances the TRANSIENT count. The comparison is
  `failure_count > max_failures`, so `max_failures = N` retries failures
  1..N and exhausts on the (N+1)th: at most N + 1 requests. On exhaustion
  the client raises `RetriesExhaustedError` with the terminal failure count
  as `attempt_count` — equal by definition (every attempt failed), so the
  two vocabularies never drift.
- **TRANSIENT backoff is full-jitter:** a delay drawn uniformly from
  `[0, min(cap, base * 2 ** (n - 1)))`. Jitter randomness enters through a
  single-method `RandomFractionGenerator` protocol that `random.Random`
  satisfies structurally — the Clock precedent applied to jitter; tests
  stub it for exact arithmetic.
- **`fallback_penalty_seconds`:** when a rate-limited response carries no
  usable Retry-After, the client passes this value to the limiter's
  `penalize()`, logging the raw header value to keep the case diagnosable.
  It errs long because it fires only when a provider is already misbehaving.

Defaults (`RetryConfig`, frozen Pydantic in `config/`):

| Field | Default |
|---|---|
| `transient_max_failures` | 3 |
| `transient_backoff_base_seconds` | 1.0 |
| `transient_backoff_cap_seconds` | 30.0 |
| `rate_limited_max_failures` | 10 — a circuit breaker against 429 storms, not a pacer |
| `fallback_penalty_seconds` | 60.0 |

---

## 8. Authentication and Response Classification

### Auth is a strategy, not a constant

A provider-agnostic `AuthStrategy` protocol:

- `prepare(spec) -> spec` — injects credentials: a header for Motive/Samsara static keys; JSON-RPC body params plus the resolved host for GeoTab.
- `on_auth_failure() -> bool` — answers "did I fix anything worth one retry?" Static keys: False — a rejected API key cannot be fixed by retrying. Sessions: invalidate + refresh, True.

Provider names appear ONLY at the composition root that constructs
strategies; the client and everything downstream is provider-blind. The
`AuthStrategy` **protocol** lives in the contract surface
(`network/contract/auth.py`); its **implementations** live in
`network/auth/strategies.py`, beside the session manager. The split is
structural: `GeotabSessionAuth` wraps the `GeotabSessionManager`, so
homing it in the contract surface would make the surface depend on
`network/auth` — the dependency that re-forms the import cycle once the
contract face is populated. The protocol depends only on `RequestSpec`,
so it stays. Implemented: `StaticHeaderAuth` (Motive/Samsara) and
`GeotabSessionAuth`, which injects session credentials into the JSON-RPC
body, retargets the URL to the session's resolved host, and pins the
last-prepared session in a `threading.local` slot so a failure on one
worker thread invalidates the session that actually failed, never a
fresher one another thread just prepared with. `prepare()` is called
fresh for every HTTP attempt — every page, every retry — symmetric with
token-per-attempt.

### GeoTab session lifecycle (implemented: `network/auth/`)

GeoTab authenticates by session: `Authenticate` returns a session id and a
resolved host; the session lives ~14 days but can die early (password
change; a 100-concurrent-session LRU cap per account).

- One session per process, shared by all threads.
- **Single-flight refresh:** one lock, held ACROSS the authenticate call, with a generation counter as the staleness/stampede guard — ten workers hitting expiry simultaneously produce one `Authenticate` call, not ten. (Deliberately the opposite of the SQLite never-hold-a-transaction-across-HTTP rule; the blocking is the point.)
- **Reactive invalidation is primary; proactive refresh is insurance:** 14-day assumed lifetime, 1-day margin, pessimistic timestamping (the session is stamped before the network call, so latency counts against the lifetime rather than extending it).
- `authenticate_fn` is injected so the **session manager** stays pure state and choreography — it never imports httpx. The real implementation (`network/auth/authenticate.py`) IS the HTTP attempt: it is the one module in `network/auth/` that imports httpx, and it passes through the rate limiter — no exceptions to token-per-attempt.
- **No disk persistence:** a session id is a bearer-equivalent secret, and at one process per scheduled run the steady-state session count stays far below GeoTab's 100-session cap.
- Passwords are `SecretStr`, extracted only inside the real `authenticate_fn`; the manager never reads the secret and never logs session ids.

### The real authenticator (implemented: `network/auth/authenticate.py`)

A single-concern, single-shot, loop-free function behind a factory
(`build_geotab_authenticator(http_config, limiter_registry, quota_scope)`)
that closes the transport dependencies over a named inner function matching
the manager's single-arg injectable type. The quota scope arrives as a
parameter — the composition root names it — so even GeoTab-specific
machinery honors the names-at-composition-root rule.

- **Two actions only**, and the classifier is deliberately NOT reused: the classifier's `ResponseCategory` encodes the CLIENT's dispatch (five outcomes), but Authenticate has exactly two — fix credentials (`AuthenticationError`) or fail loud (`ProviderResponseError`). Reusing the classifier would map categories only to re-map them. `InvalidUserException` on Authenticate is bad credentials (`AuthenticationError`) — the context-disambiguation principle (the same type on a data call is a dead session, the auth strategy's concern, not this function's). Any other error type, a non-200 status, a non-JSON body, or an envelope with neither result nor error is `ProviderResponseError`.
- **v1 postures, with re-litigation triggers:** Authenticate outcomes arrive in HTTP 200 per verification, so a non-200 is the API not speaking its protocol — loud-and-typed beats a retry loop against the 10/min auth quota (trigger: first observed Authenticate 5xx in production). An unknown error type fails loud rather than guessing retryability (trigger: `OverLimitException` seen here despite the local limiter).
- **`ThisServer` resolution:** the result's `path` is either the literal `ThisServer` (use the host we called — `config.server`) or an alternate host (use it, logged at INFO as a redirect — handled-not-assumed, since no capture shows one). `ThisServer` is a GeoTab protocol sentinel held as a module `Final[str]`, never user config — no operator should set it.
- **Dedicated Authenticate quota scope:** Authenticate is rate-limited at a fixed 10/min, outside the per-provider tiering; the composition root configures a dedicated scope in the registry, and an unconfigured scope propagates `UnknownQuotaScopeError` naturally (no catching).
- **Boundary seam:** the `_Authenticate*` Pydantic models validate the inbound wire response (`strict=True`, `extra='ignore'`); the function returns the existing frozen `AuthenticationResult` dataclass built from the validated fields. Pydantic at the boundary, dataclass within — a Pydantic model is never returned into the program's interior. Every inbound read flows through a slice model (the Prompt-12 structural rule); transport exceptions propagate raw and untyped (the client owns prepare-time transport-failure classification).

### Response classification, single-producer

A closed vocabulary `ResponseCategory` (StrEnum): SUCCESS, TRANSIENT,
RATE_LIMITED, AUTH_FAILURE, FATAL. The vocabulary means "what the client
does next," and each member earns its slot by demanding a distinct client
action (parse / retry / penalize-shared-scope-then-retry / ask the auth
strategy / raise). **Closure invariant: a new category is admissible only
if it arrives with a new client action.**

`ResponseCategory` is dependency-free package vocabulary, homed in the leaf
`fleetpull/vocabulary/` (importing nothing internal) and spoken by three
layers at once: the classification layer produces it, the retry layer takes
it as input, and the exception hierarchy carries it on the public
`RetriesExhaustedError.category`. Shared vocabulary depended on by a
user-facing layer cannot live transport-deep without inverting the
dependency direction — siting it under `network/contract/` is what formed
the former `exceptions` → `contract` import cycle, so it lives in the leaf
instead.

Classification results travel as a frozen `ClassifiedResponse` (category;
`retry_after_seconds: float | None`; `detail: str | None`;
`parsed_body: JsonValue | None` — fields inert outside their category). The
carrier `ClassifiedResponse` is transport-internal and stays in
`network/contract/outcome.py` (produced by the classifiers, consumed by
the client in `network/client/`); only the vocabulary it references is in
the leaf. Classifiers that parse the body to classify (GeoTab) hand the
parse forward in `parsed_body`; the
client parses only when `parsed_body` is None, and never re-parses when it
is populated.

The producer is a per-provider `ResponseClassifier` ABC:

- `classify_response(status, headers, body)` — abstract; provider envelopes differ (GeoTab returns JSON-RPC errors inside HTTP 200, so a status-code-only client cannot see failure).
- `classify_transport_exception(exc)` — CONCRETE in the base, written once: timeouts and connection failures are below the provider envelope and must not vary per provider.

The classifier is the SOLE producer of the vocabulary; the client only
consumes, dispatching on category. House rule this establishes:
**Protocol for pure shape** (`AuthStrategy` — zero shared code), **ABC for
shared substance** (`ResponseClassifier`). The contract surface lives in
`network/contract/`: `outcome.py` (the `ClassifiedResponse` carrier),
`classifier.py` (the ABC), `auth.py` (the protocol), `pagination.py`,
`envelopes.py`, `request.py`; the `ResponseCategory` vocabulary they all
speak lives in `fleetpull/vocabulary/`. The **provider implementations** —
the classifiers, the paginators, the auth strategies — are **peers** of the
contract surface (`network/classifiers/`, `network/paginators/`,
`network/auth/strategies.py`), not children of it, and import the surface
through its `__init__` face. The protocol/implementation boundary is thus a
package boundary the import guard enforces: the surface may never import an
implementation, and an implementation reaches the surface only through the
face — the structure that keeps the surface free of the `network/auth`
dependency that would re-form the import cycle.

**Specific codes by name, bands by constant:** provider classifiers
compare specific well-known statuses against `http.HTTPStatus` members
(`TOO_MANY_REQUESTS`, `UNAUTHORIZED`, `FORBIDDEN`); band membership uses the
shared `SUCCESS_STATUS_RANGE` / `SERVER_ERROR_FLOOR` constants. Never
construct `HTTPStatus` from an arbitrary code — `HTTPStatus(code)` raises
`ValueError` on nonstandard statuses (e.g. 522) and a classifier must
classify every status, not crash on one.

### Pagination contract (implemented: `network/contract/pagination.py`, `paginators/`)

The client is pagination-blind the way it is auth-blind and
classification-blind: one loop shape for every provider and for
unpaginated endpoints alike. A per-provider `PaginationStrategy`
(Protocol — the implementations share zero concrete behavior) supplies
`first_request(spec)` (decorate the base spec for page one) and
`advance(sent, envelope) -> PageAdvance` (the verdict for the page just
received). Strategies are frozen dataclasses holding configuration
fields only; the client threads the loop, the strategy computes.
Implemented strategies: `SinglePageStrategy` (unpaginated endpoints —
replaces any is-paginated flag), `MotivePagination` (page-numbered),
`SamsaraPagination` (cursor), `GeotabFeedPagination` (GetFeed
`toVersion` feed).

**Verdict versus raise:** components return verdicts when the consumer
must choose among actions; they raise when only one action exists.
`advance` returns a verdict (continue/complete — the client
dispatches). A structurally violating pagination envelope has exactly
one action — raise `ProviderResponseError` ("contract-violating"
covers it) — so strategies raise it directly. A malformed SENT request
reaching a strategy is a caller bug and stays stdlib `ValueError`.
Samsara's continuation-without-cursor is the canonical single-action
case: silently finishing would truncate data, the one failure mode a
fetch library must never have.

**Durable progress:** `PageAdvance.durable_progress` carries cursor
progress that must outlive the fetch — GeoTab's `toVersion`, the state
layer's FeedToken commit value — on EVERY page including the terminal
one (the terminal page's value is the resume point; per-page progress
is what makes a crash mid-feed resumable). None for providers whose
cursors are fetch-private (Motive, Samsara).

**Envelope-slice models:** wire metadata is an API contract, so it is
validated by Pydantic models — private, per-consumer, frozen,
`extra='ignore'`, `strict=True`. The two config flags are deliberately
opposed and each earns its place: `extra='ignore'` tolerates ADDITIONS
to provider-owned envelopes (semantically safe — the `extra='forbid'`
house default is only for schemas WE own, i.e. config); `strict=True`
refuses TYPE DRIFT on the fields we act on (a stringified number, a
bool-ish string), because coercing drift is a changed contract being
silently adapted to — the failure mode this layer exists to make loud.
Crash, investigate, widen only if a drift proves benign. The models
validate the whole envelope (a two-level slice locating the metadata),
so no naked envelope-walking or `isinstance` ladders exist in the
layer; the shared validate-then-raise composition is
`validated_envelope_slice` (`network/contract/envelopes.py`), relocated
out of `pagination.py` at its second consumer — the GeoTab
authenticator — because the composition is contract-layer semantics,
not pagination semantics. These private slices are not endpoint mirrors
and do not belong in `models/`.

**Wire tokens are constants, not enums:** wire-protocol tokens
(`'fromVersion'`, `'page_no'`, `'after'`, the Authenticate body keys)
are module-private `Final[str]` constants. Enums model closed sets that
code dispatches over (`ResponseCategory`); nothing dispatches over a
wire token. **Constants-scope precedent** (it governs the endpoint
prompts): wire-token constants are colocated with their consuming logic
at the tightest scope that genuinely shares them — module-private
within a provider (a token used by both `first_request` and `advance`
is one constant), never centralized across providers. Token coincidence
across providers is accident, not shared semantics; a shared registry
would couple providers through a file none owns, against
blast-radius-over-DRY at provider boundaries. Envelope keys are never
constants at all — they are consumed via the slice models' fields and
aliases, never walked.

Provider mechanics worth recording: Motive termination recomputes
`page_no * per_page >= total` from each page's freshly echoed values,
so mid-pagination drift in `total` self-corrects — which is why no
empty-page guard exists. GeoTab advances send `fromVersion` with
`search` stripped (verified: the API accepts `fromVersion` alone, and
tolerates both being sent — the strategy always strips);
`resultsLimit` is read from the sent body, so strategy-versus-endpoint
divergence is structurally impossible.

### The exception hierarchy (implemented: `exceptions.py`)

The operational errors consumers catch, mirroring the classification
vocabulary and inheriting its closure invariant: **a new exception type is
admissible only if it demands a distinct consumer action.** Programming
errors (caller bugs) stay stdlib `ValueError`/`RuntimeError` — a hierarchy
that absorbs caller bugs invites broad `except` clauses that silence them.

```
FleetpullError
├── ConfigurationError
│   └── UnknownQuotaScopeError
├── AuthenticationError
├── ProviderResponseError
└── RetriesExhaustedError
```

| Exception | Consumer action |
|---|---|
| `ConfigurationError` | Fix local config/wiring before rerunning. |
| `AuthenticationError` | Fix credentials / account access. |
| `ProviderResponseError` | Provider response was non-retryable or contract-violating; do not blindly rerun. |
| `RetriesExhaustedError` | The transient/rate-limit budget ran out; rerunning later is reasonable. |

Members are plain data carriers: typed fields for programmatic handling, a
composed human message for `str()`. Instances never carry raw request or
response material (headers, bodies, request specs, credentials-adjacent
values) — every instance is safe to log. Pickling is deliberately
unsupported (keyword-only fields break `BaseException`'s positional-args
reconstruction): fleetpull concurrency is threads, and exceptions never
cross a process boundary in this package. The client prompt wires the raise
sites: FATAL classifications → `ProviderResponseError`, exhausted retry
budgets → `RetriesExhaustedError`, failed auth paths →
`AuthenticationError`.

### Observed provider behaviors (verified June 2026)

| Provider | Behavior |
|---|---|
| GeoTab | Application errors arrive inside HTTP 200; `error.data.type` is the authoritative discriminator (present in every captured failure). |
| GeoTab | `InvalidUserException` covers BOTH bad credentials and dead sessions — distinguished only by message text; context disambiguates (data call → invalidate + one retry; Authenticate itself → fatal). |
| GeoTab | `OverLimitException` pairs with an integer `Retry-After` header (e.g. `56`). |
| GeoTab | Success responses carry `X-Rate-Limit-*` budget headers; deliberately unconsumed — the reactive control loop (configured budgets plus the 429 penalty) is the v1 design, and a second feed-forward loop is rejected. Re-litigate on sustained 429 churn on GeoTab in production. |
| GeoTab | `toVersion` is a string cursor; `GetFeed` with `search.fromDate` supports historical bootstrap (feeds the state design). |
| Samsara | 429 with fractional `Retry-After` (e.g. `0.40235`); 401 body is `{"message": ...}`; 5xx bodies are plain strings, never JSON. |
| Motive | 401 body is `{"error_message": ...}`; the documented /vehicle_locations limit was not observed to enforce — generic 429 posture. |

---

## 9. Records, Flattening, and Schema Derivation

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

## 10. Public API and CLI

**Programmatic:**

- `iter_records(endpoint, **params)` — typed iterator of Pydantic models, pagination transparent. (Renamed from fleet-telemetry-hub's `fetch_all`, whose "all" misleadingly suggested all endpoints rather than all pages.) This is the escape hatch for consumers who don't want Polars; the dataframe path is built on top of it.
- DataFrame retrieval per endpoint (built on `iter_records` + `records.py`)
- Read path over managed storage (single or partitioned) returning a dataframe

**CLI — two verbs, no more:**

- `fetch` — one provider/endpoint/window → parquet or stdout dataframe
- `sync` — config-driven, multi-endpoint, incremental (work units, executors, writers)

---

## 11. Module Layout

```
fleetpull/
  exceptions.py    # package exception hierarchy (§8) — user-facing: consumers catch these
  vocabulary/      # shared, dependency-free package vocabulary (imports nothing internal)
    response_category.py  # ResponseCategory (§8) — spoken by exceptions, retry, classification
  config/          # Pydantic models for user-provided YAML, one module per section; the YAML loader joins in a later prompt
    logger.py      # LoggerConfig
    geotab.py      # GeotabAuthConfig (server validated as a bare hostname, §8)
    retry.py       # RetryConfig — attempt budgets, backoff shape, fallback penalty (§7)
    http.py        # HttpConfig — connect/read timeouts, truststore opt-in
  logger/
    setup.py       # package logging setup (setup_logger), driven by LoggerConfig
  network/         # organizational namespace; the surfaces live in the subpackages
    client/        # HTTP transport, retry policy, limiter consultation; consumes the pagination abstraction
      transport.py   # TransportClient — the assembled fetch loop and per-attempt pipeline
      profile.py     # ProviderProfile — per-provider auth + classifier bundle
      runtime.py     # ClientRuntime — process-global configs, limiter registry, jitter, sleeper
      page.py        # FetchedPage — the emit type (envelope + durable_progress)
    tls/           # SSL-context construction
      truststore_context.py  # SSLContext factory backed by the OS trust store (Zscaler-class proxies)
    auth/
      models.py    # AuthenticationResult, GeotabSession (frozen dataclasses)
      manager.py   # GeotabSessionManager — single-flight session lifecycle (§8)
      authenticate.py  # build_geotab_authenticator — the real Authenticate call (§8); the one network/auth/ module that imports httpx
      strategies.py  # StaticHeaderAuth, GeotabSessionAuth — the AuthStrategy implementations (§8)
    contract/
      request.py   # HttpMethod, RequestSpec, JSON type aliases; params is
                   #   single-valued by design — widen to accept sequences when
                   #   a real endpoint demands repeated query keys
      outcome.py   # ClassifiedResponse (the carrier; ResponseCategory lives in vocabulary/)
      classifier.py  # ResponseClassifier ABC + shared transport-exception mapping
      auth.py      # AuthStrategy protocol only (implementations live in network/auth/strategies.py)
      envelopes.py   # validated_envelope_slice — shared validate-or-raise for wire slices (§8)
      pagination.py  # PageAdvance, PaginationStrategy (§8)
    classifiers/   # per-provider classifiers (peers of contract/; import its face): motive.py, samsara.py, geotab.py
    paginators/    # per-provider strategies (peers of contract/; import its face): single_page.py, motive.py, samsara.py, geotab.py
    limits/
      config.py        # RateLimitConfig (frozen Pydantic)
      bucket_math.py   # pure token-bucket arithmetic (stateless functions)
      limiter.py       # QuotaScopeLimiter
      registry.py      # RateLimiterRegistry
    retry/
      decision.py  # RetryDecision, RandomFractionGenerator, decide_retry — pure retry policy (§7)
  timing/
    clock.py       # injectable Clock Protocol; SystemClock and FrozenClock implementations
    sleeper.py     # injectable Sleeper Protocol; SystemSleeper backing TRANSIENT backoff waits
    codec.py       # pure UTC datetime <-> ISO-8601/date-string conversions (stdlib-only leaf)
  incremental/     # per-endpoint resume cursors; pure dependency-free leaf (§4)
    cursor.py      # DateWatermark, FeedToken, IncrementalCursor tagged union
  endpoints/
    base.py        # EndpointDefinition ABC: auth, pagination style, quota_scope,
                   #   incremental strategy (watermark | feed_token), storage strategy
    motive.py
    samsara.py
    geotab.py      # stub until access lands
  models/          # response Pydantic models per provider (largely ported from fleet-telemetry-hub)
  records/         # flattening; Pydantic → Polars schema derivation + overrides + validation
  storage/         # Polars merge/write: delete-by-window + append; single vs partitioned
  state/           # SQLite: watermarks/cursors, run ledger, work units
  orchestrator/    # sync planner: builds work units, per-provider executors, per-endpoint writer threads
  cli.py           # fetch, sync
```

The package root holds user-facing modules only; internal code lives in
subpackages. Settled: ALL Pydantic models parsing user-provided YAML
centralize in `config/` — including `RateLimitConfig`, which currently lives
in `network/limits/config.py` and migrates to `config/` in the prompt that
builds the YAML loader. Placement for everything else is settled the same
way: the client is transport plumbing and lives at `network/client/`,
alongside the limiter, contract, and auth it consumes; `records`, `storage`,
`state`, and `orchestrator` are internal by the same test (consumers call
the public API, never these) and each receives its own subpackage home when
its prompt builds it — a single-module subpackage is the blessed shape.
`exceptions.py` and `cli.py` are user-facing and stay at the root: consumers
catch the exceptions and invoke the CLI. The hierarchy itself — members,
consumer actions, and stances — is recorded in §8.

Boundary rules:

- `storage` knows nothing about state; `state` knows nothing about parquet. The orchestrator sequences them (parquet-then-watermark ordering, §5).
- `network/client/` consumes the pagination abstraction (`network/contract/pagination.py`). Retry and limiter consultation stay interleaved per-request concerns inside the client — splitting them away from the request loop is how the token-per-attempt / token-per-page rules get violated.
- The orchestrator never touches the limiter (§7).

---

## 12. House Code Standards (carried into this package)

- Asserts only in tests, never production code
- Annotated locals; explicit type hints everywhere
- No real VINs / internal fleet identifiers in committed files
- Docstrings with Args/Returns/Raises/Side Effects
- `logging.getLogger(__name__)`; no `print` in production code
- Explicit timeouts on all network calls; specific exception handling
- Blast-radius minimization over DRY where coupling risk is real
- `StrEnum` for enums

---

## 13. Open Questions

- GeoTab specifics pending API access: `GetFeed` semantics in practice, real rate limits, which entities map to which storage strategies (the auth model is settled — session-based, §8)
- Real rate-limit values for Motive/Samsara (YAML numbers above are placeholders)
- Whether any endpoint actually warrants the flattening opt-out
- Per-endpoint quota scopes for Samsara (config-only change when needed)

## 14. Next Steps

1. Review/amend this document
2. Build in dependency order: `network/limits/` (done) → auth session manager (done, `network/auth/`) → request contract (done, `network/contract/`: `RequestSpec`, `AuthStrategy` + implementations, `ResponseCategory`/`ClassifiedResponse`/`ResponseClassifier`; `ProviderProfile` deliberately deferred to the client prompt — the bundle rule triggers at three traveling parameters and only two exist) → exception hierarchy (done, `exceptions.py`) → retry policy (done, `config/retry.py` + `network/retry/`) → pagination abstraction (done, `network/contract/pagination.py` + `paginators/`) → HTTP config + the real GeoTab authenticator (done, `config/http.py` + `network/auth/authenticate.py`) → `network/client/` (done) → `endpoints/base.py` → `records` → `storage` → `state` → `orchestrator` → `cli.py`

The `network/client/` step inherits a recorded agenda: classify
prepare-time transport exceptions (the authenticator propagates
`httpx.TransportError` raw and loop-free by design — whether a transport
failure during auth/prepare is retried is the client's call), wire the
exception-hierarchy raise sites (FATAL → `ProviderResponseError`, exhausted
budgets → `RetriesExhaustedError`, auth paths → `AuthenticationError`), and
bundle the two per-provider dependencies that share a session lifetime
(auth strategy, classifier) into `ProviderProfile`, leaving the per-endpoint
pagination strategy and quota scope to arrive on each `fetch_pages` call.
3. Port Motive/Samsara models and endpoint definitions onto the new base
4. GeoTab integration when access lands
