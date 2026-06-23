# fleetpull — Design Document

**Status:** Design settled through module layout; implementation well underway — the `vehicles` snapshot vertical is complete end-to-end and the `vehicle_locations` date-partitioned/watermark vertical is in progress (see §14 for build progress).
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

**Storage layout is declared on the endpoint definition, not inferred — and it is *layout only*.** What a merge *does* to the data — full-replace, delete-by-window-then-append, or append-plus-dedup — follows the endpoint's `SyncMode` (§4), an orthogonal axis the storage layer combines with the layout.

- `single` — one parquet file; a merge reads the whole file, applies the `SyncMode`'s write semantics, and rewrites it. Fine for low-volume endpoints (on the order of 10–15k rows/day or less). Snapshot endpoints are always `single` — a current-state snapshot has no event-time dimension to partition on.
- `date_partitioned` — hive-style `date=YYYY-MM-DD` partitions; a merge touches only the partitions the fetch window overlaps. Required for breadcrumb-scale endpoints. Hive layout is read natively by BigQuery external tables and `pl.scan_parquet`.

`metadata.json` is a **generated human-readable snapshot**, written from SQLite
contents at the end of each successful run. It is never read by the program.
SQLite is the single source of truth (see §5) — no dual-write divergence.

**Realized structure (`snapshot`+`single` and `watermark`+`date_partitioned` built;
the feed cells next).** Each `(StorageKind, SyncMode)` cell is its own
`DatasetWriter` — fused per cell, not composed from an injected merge, because the
write semantic depends on both axes at once (a floored watermark write *replaces*
under date partitioning but *clears and appends* under a single file). `select_writer`
is the single routing point: it resolves the endpoint directory and returns the
cell's writer, constructed with the runtime resume `window` an incremental cell
needs. The orchestrator drives every endpoint identically — `write` per fetched
piece, `finalize` once — and `finalize` returns a `WriteResult`. The exact-duplicate
dedup (§6) runs inside each writer's finalize, on the frame it is about to write.
Storage is stateless — parquet only, no SQLite, no watermark commit, no
`metadata.json` (the orchestrator sequences those after a successful `finalize`,
§5). The single-file family (`SingleFileWriter` → `SnapshotWriter`) and the
date-partitioned watermark cell (`PartitionedWriter` → `WatermarkPartitionedWriter`)
are built; the feed cells (single and partitioned) fill with GeoTab. The leaf
primitives the writers compose: `split_by_date` (`storage/partition.py`: a frame →
per-UTC-date sub-frames), `date_partition_segment` / `parse_date_partition_segment`
(`paths/partitions.py`: the `date=YYYY-MM-DD` segment and its strict inverse),
`partition_part_file` (`storage/files.py`), `in_window` (`storage/frames.py`: the
half-open `[start, end)` row predicate, for the single-file combine cells),
`render_url_path_template` (`endpoints/shared/url_paths.py`: the per-vehicle URL
fan-out), `latest_event_time` (`records/event_time.py`: the watermark candidate),
`stage_shard` / `compact_partition` (`storage/staging.py`: the date-partitioned
write half), and `prune_window_partitions` (`storage/partitioning.py`: the delete
half). `vehicle_locations` is fully bound. Its page decoder is
`MotiveWrappedSinglePageDecoder` (§8) — the wrapped-list unwrap with a terminal
verdict, net-new because neither existing decoder fit: `SinglePageDecoder` does not
strip the per-item wrapper, and `MotiveWrappedListPageDecoder` requires a
`pagination` block this unpaginated endpoint lacks.
`build_vehicle_locations_endpoint` composes the spec-builder and decoder with
`DATE_PARTITIONED`, `WatermarkMode` (its lookback from the provider config), and
`event_time_column='located_at'`. The per-vehicle fan-out over the vehicle list is
the orchestrator's, next.

**There is no merge function — the combine lives in each writer.** The earlier
design injected a `MergeFn` per `SyncMode` and applied it inside a `Layout`; both
are gone. Each cell's writer owns its own combine: a snapshot returns this run's
frame, a feed concatenates-and-dedups against the prior file, a watermark single-file
clears the window (`~in_window`) and appends, a watermark date-partitioned replaces
each covered partition and prunes the empty ones. Window-clearing is therefore not a
row operation a merge performs but a property of the cell's write mechanism — and
which mechanism applies turns on whether the partition grain equals the window grain,
the matrix below.

**Window-clearing is a write-mechanism concern, and which mechanism applies turns on
whether the partition grain equals the window grain.** The full mechanism matrix,
`StorageKind` × `SyncMode`:

| layout × mode | clear-and-write mechanism | reads parquet? | status |
|---|---|---|---|
| `snapshot` / `single` | overwrite the file | no | built |
| date-window / `single` | lazy `scan_parquet` + `~in_window` filter + concat + rewrite | yes (`in_window` here) | not built |
| date-window / `date_partitioned` | delete covered `date=` folders + write the fetched partitions | **no parquet reads** | building now (`vehicle_locations`) |
| feed / `date_partitioned` | append to the partition: read + concat + dedup + rewrite | yes | not built (GeoTab) |
| feed / `single` | `scan_parquet` + concat + dedup + rewrite | yes | not built |

**The date-partitioned date-window cell touches no parquet bytes.** Because the
partition grain *equals* the window dimension — date partitions, a date window —
every `date=` partition is wholly inside or wholly outside `[start, end)`; there is
no sub-partition row-filtering. Clearing the window is therefore a *directory*
operation (delete whole `date=` folders), not a data operation — which is why
`in_window` (the row-level predicate) is used only in the single-file date-window
and feed cells, never in this one.

**The write+delete for that cell is two steps.** (1) Write every
`split_by_date(new_frame)` partition through `atomic_write_parquet` —
overwrite-or-create, the prior existence of the folder/file being irrelevant since
the result is identical either way. (2) Delete any on-disk `date=` folder the window
*covers* that received no fetched partition — the empty-refetch dates. Step (2) is
**mandatory, not optional**: the window's contract is "`[start, end)` is
authoritatively replaced," and a covered date can legitimately return empty while
stale rows sit on disk (a provider that deletes or edits records). We do not assume
immutability for this whole code path — one immutable provider does not license the
assumption for every endpoint that travels it — so the delete is mandatory
insurance, the directory-grain analogue of §4's delete-by-window.

**The delete step iterates from the window, never from disk.** Generate the covered
date segments from `window_dates(window)` (cost O(window)), `stat` / `is_dir()` only
those specific paths under the endpoint directory, subtract the dates just written,
and delete the remainder — the set arithmetic is `{covered date folders that exist
on disk} − {date folders just written} → delete`. Never list the full endpoint
directory: a dataset spanning years would make that an O(dataset) scan, the exact
cost partitioning exists to avoid.

**`window_dates(window) -> list[date]` is the half-open rule (§4) lifted from
instants to dates.** A partition `date=d` is covered iff some instant of that day
lies in `[start, end)`, i.e. the dates `start.date()` through `(end - 1µs).date()`
inclusive. The load-bearing consequence: a window ending exactly at midnight does
**not** cover that date — `end = June 8 00:00` covers through `date=June 7`, because
`June 8 00:00` *is* `end` and `end` is excluded; a mid-day `end` (`June 8 14:00`)
*does* cover `date=June 8`. The one-microsecond epsilon is valid because datetimes
are `us`-precision end to end (enforced upstream). Reopening trigger: if precision
ever becomes uncertain, switch to the precision-independent branch form
(`last = end.date()`, stepped back one day when `end.time()` is exactly midnight).

```python
def window_dates(window: DateWindow) -> list[date]:
    first = window.start.date()
    last = (window.end - timedelta(microseconds=1)).date()
    return [first + timedelta(days=n) for n in range((last - first).days + 1)]
```

**`persist` grows a keyword-only `window`.** The signature becomes
`persist(definition, new_frame, dataset_root, *, window: DateWindow | None = None)`.
The window is computed per-run by the driver (via `compute_resume`, §4) and passed
in — never read off the definition — and `persist` validates the pairing against the
sync mode: a `WatermarkMode` endpoint with `window is None` is a wiring bug and
raises, and a `SnapshotMode` endpoint handed a non-`None` window also raises.

**How the fleet's rows for one date are assembled across the per-vehicle fan-out** —
`vehicle_locations` fetches per vehicle (~1,459 `GET .../{vehicle_id}` calls) but a
single `date=` partition holds the whole fleet's rows for that date — is the central
open question for this write path (§13), as is `DatePartitionedLayout`'s exact
interface, which is contingent on it.

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

**Resume value vs. cursor — and the pure function between them.** The stored
cursor (`DateWatermark` / `FeedToken`) is not what a request is built from; the
resume value is. For a watermark endpoint that value is a `DateWindow` — the
half-open `[start, end)` window the spec-builder fetches, a frozen carrier in
`incremental/` beside the cursors, so an endpoint names it without importing
`state/`. For a feed endpoint the resume value is the stored `FeedToken` itself,
used directly — no transformation, so the feed arm needs no function. The
watermark arm does: `compute_resume(watermark, lookback, now) -> DateWindow | None`
is a pure function in `incremental/` mapping a `DateWatermark` to
`DateWindow(watermark - lookback, now)` and `None` to `None`. It is watermark-only
by design — the feed arm has neither a lookback nor a window to compute — and pure
(no clock; `now` is injected), so it is a function, not a method on the endpoint
definition and not a strategy. A returned `None` means only "no committed
watermark"; the caller then resolves the start through the resume precedence below,
so `compute_resume` is precedence arm (1) as code, with arms (2)–(3) remaining the
caller's. The `DateWindow` carrier enforces its one structural invariant —
`start < end`, well-ordered — and defers UTC validity to the codec boundary exactly
as `DateWatermark` does. The half-open convention is what lets the delete-by-window
predicate and the start-anchored append-filter share one boundary rule and never
double-count at a window edge.

**The window is cooked per-run by the driver, not stored.** `compute_resume` is
precedence arm (1); the driver composes it with the end-cutoff and the resume
precedence (§5) to produce each run's `DateWindow` fresh and hands it to `persist`
(§3) — the frozen `EndpointDefinition` never carries a window. The window's `end` is
`today - cutoff` rather than the literal `now`: `cutoff` is a config value (e.g. 1
day, or 0 for "up to now") that holds the trailing edge back so a still-arriving day
is not frozen prematurely — `compute_resume`'s injected `now` *is* this cutoff
instant. `default_start_date` (config) is only the first-backfill anchor — arm (3) —
and goes inert the moment observed data or completed coverage exists; thereafter the
start is `max(observed) - lookback`. `lookback` and `cutoff` both sit on
`WatermarkMode`, sourced per-provider from the provider config (`lookback_days` /
`cutoff_days`) — the two ends of one provider-latency concern. The cold-start
`default_start_date` — arm (3) — is sync-wide rather than per-endpoint, so it lives
on the sync-level `SyncConfig`, not on every `WatermarkMode`.

Each endpoint definition declares which strategy it uses. This is the single
biggest architectural improvement over fleet-telemetry-hub, whose
`latest_data_date - lookback` assumption cannot represent GeoTab.

**Merge semantics (watermark endpoints): delete-by-window, then append.**

1. Fetch the window `[start, end)` from the API — half-open is the canonical internal form
   (the `DateWindow` carrier), which the spec-builder maps to the provider's own request
   convention.
2. In existing storage, delete every row whose event timestamp falls in `[start, end)` —
   start inclusive, end exclusive.
3. Append the fresh fetch.

The window is the unit of truth: whatever the API returns for a window
replaces what was held for that window. This handles late-arriving records and
payload-drift updates (providers have been observed returning the same event
with end timestamps drifting by milliseconds-to-seconds across fetches) with
no event-id logic.

No merge function performs this clearing — there is no merge function (§3). The clear
is a property of the cell's writer: a row-level `~in_window` rewrite for `single`, a
whole-`date=`-folder delete for `date_partitioned` — the write-mechanism matrix in §3.

**Precondition — the incoming frame must be anchored to the window on the same
field the delete keys on.** Delete-by-window is idempotent and dup-free only
when the rows appended for a window are exactly the rows the delete would remove
on the next run. For a **start-anchored** provider (the API returns only records
whose anchor falls in `[start, end]`) this holds automatically. For an
**overlap-anchored** provider it does not: Samsara `/v1/fleet/trips` returns any
trip *intersecting* the window, including trips that started before `start`
(verified by live probes). Appended as-is, those pre-`start` trips are never
deleted on a later run — their prior copy lives under the earlier window that
owns their start — so they accumulate as leading-edge duplicates, and exact
dedup cannot remove them because the re-emitted copy carries drifted timestamps
(the same payload drift noted above). The fix is **start-anchored
normalization**: filter the incoming frame to records whose start falls in the
window before appending, so each cross-boundary event is anchored to the single
window that owns its start and is never double-counted at a window's leading
edge. This is the mechanism carried over from fleet-telemetry-hub.

**Consequence — coverage may bleed slightly before `start`.** A trip that
started before `start` but was returned by an overlap fetch is dropped from the
incoming frame, because its one authoritative copy already lives under the
earlier window that owns its start. File coverage therefore extends slightly
before a given window's `start`. This is intended. Do **not** "fix" it by
clamping start times to the window — that would discard the authoritative copy.

**Merge semantics (feed-token endpoints): append-only + exact dedup.** No
window exists to delete; the token stream is the unit of truth, and only
byte-identical rows (from our own pagination or a crash refetch) are dropped.

GeoTab `GetFeed` entities are *active* or *calculated* (the provider's terms).
Active feeds (e.g. `LogRecord`, `StatusData`) emit only new, static records —
append-only is trivially complete. Calculated feeds (`Trip`, `ExceptionEvent`,
`FillUp`, `FuelUsed`, `FuelAndEnergyUsed`, `FuelTaxDetail`, `ChargeEvent`)
re-emit past records on reprocessing: the same `id` reappears with a higher
`version` and changed fields. Append-only therefore stores *every emitted
version*. This is deliberate and consistent with §6 — collapsing versions to the
latest is same-key-different-payload dedup, the consumer's concern, not ours; the
consumer reconciles by `(id, max version)`.

*Open question (resolve empirically against the live feed — access is
available):* calculated records can also be removed by the system. Whether the
feed signals a removal as an emitted record (a tombstone the consumer can act on)
or simply stops re-emitting it is unconfirmed. If removals are unsignaled, a
removed record persists in append-only storage until the consumer reconciles
against the live system — handling it any other way would require the event-id
logic §6 places out of scope. Confirm the removal mechanism empirically before
building the GeoTab merge.

**Never** overwrite storage with only the current window. Incremental means the
dataset stays complete and current.

---

## 5. SQLite Operational State

One SQLite database lives at the resolved state database path — runtime config resolves it from `state.database_path`, defaulting to `<dataset_root>/.fleetpull/state.sqlite3`. Keeping it separable from `storage.dataset_root` lets SQLite stay on local disk when parquet sits on a network filesystem (WAL requires local disk). WAL mode. Short `busy_timeout`. Owns:

- **Watermarks/cursors** per (provider, endpoint) — the tagged-union state from §4
- **Run ledger** — run id, provider, endpoint, sync mode, window/cursor range, status, row counts, duration
- **Work units** — backfill decomposes into (endpoint, date-chunk) or (endpoint, vehicle, date-chunk) units; threads claim and complete them; a crash mid-backfill resumes from unclaimed/failed units instead of refetching everything

Rules:

- Transactions are tiny: claim unit → commit; finish unit → commit. **Never hold a transaction across an HTTP call.**
- SQLite is local-disk only; not designed for network filesystems.

**Status: the `state/` layer is built and tested in full** — `StateDatabase` (WAL,
application-id stamping, integrity check), the v1 forward-only migration (`cursors`
/ `runs` / `work_units`), `CursorStore`, `RunLedger` / `RunStatus`, and
`WorkUnitStore` with its claim queue. What remains unbuilt is the *orchestrator*
that sequences these against fetch and storage (§14), not the state layer itself.

**Crash-safety ordering:** write parquet first (temp file + atomic rename),
commit watermark/cursor second. A crash between the two causes a refetch on the
next run. For watermark endpoints, delete-by-window merge makes that refetch
idempotent. For feed-token endpoints, resuming from the last-committed token
refetches from there and exact dedup drops the byte-identical rows — and a
calculated record reprocessed in the interim simply reappears as a new version,
a normal §4 update rather than a duplication. At-least-once fetching + idempotent
merge = exactly-once data, with no transactional coupling between SQLite and the
filesystem.

**Writer discipline:** fetch workers run in parallel, but parquet merge per
endpoint is **single-writer**. Fetch workers produce record batches into a
queue; one writer per endpoint drains and merges. Date-partitioned endpoints
may parallelize writes *across* partitions (each partition is an independent
file), never within one.

**Per-fetch memory is bounded by the write unit, never the endpoint.** The unit
buffered in memory and written to disk is one bounded batch — for a watermark
backfill, one work-unit chunk (one date-chunk, optionally one partition; the
work-units decomposition above), and for a feed sweep, one bounded run of pages —
never a whole endpoint's accumulated window. A chunk's pages stream through the
client and accumulate into a single Polars frame for that chunk only; that frame
is merged (delete-by-window + append) and the chunk's state advanced, and the
frame is released before the next chunk begins. Memory is therefore bounded by
the chunk, and chunk size is the caller's planning lever — granular, high-volume
data (per-vehicle breadcrumbs over months) is kept in bounds by smaller date
windows, not by a streaming rewrite of the merge. The page loop streams; the
write unit is the buffer boundary. Accumulating an entire endpoint's window into
one frame before writing is forbidden: it is the unbounded-memory failure
fleet-telemetry-hub avoided for its single-file utilization output by pushing the
read-delete-append-write into DuckDB, and fleetpull achieves the same bound
structurally by making the chunk the write unit.

**Schema versioning:** the database schema is versioned with SQLite's
`user_version`. A forward-only migration runner (`state/migrations.py`) brings a
database to head at startup, after `initialize`, applying each step's DDL and its
version bump in one atomic transaction; a database at a version newer than the
running code is refused. Today head is v1: the cursors, runs, and work_units
tables.

**Cursor persistence (the cursor store, `state/cursors.py`).** The store
translates between the `IncrementalCursor` union (§4) and `cursors`-table rows; it
owns the serialization the cursor leaf and the migration runner deliberately
don't. A `DateWatermark` serializes its `watermark` to ISO-8601 UTC text via the
timing codec; a `FeedToken` stores its opaque token verbatim (fleetpull never
parses it). The CHECK-constrained `kind` column discriminates the arm on read;
`updated_at` is written from the injected `Clock`. A row read with an unrecognized
`kind`, or a `date_watermark` value that is not parseable ISO-8601, is state-store
corruption and raises `ConfigurationError`, consistent with the other §5
corruption stances.

`get_cursor` returns `IncrementalCursor | None`; `None` means exactly "no cursor
has been persisted for this (provider, endpoint)" — nothing more. The store never
fabricates a cursor and never interprets absence; the resume-on-absence decision
lives above it (see resume precedence below). `set_cursor` is an unconditional
single-row upsert; the advance discipline lives in the caller, not the store.

**Watermark semantics: observed-data-only and monotonic.** A `DateWatermark` is
the maximum event timestamp actually seen; it is set only from observed data and
only ever moves forward (the caller invokes `set_cursor` only when
`current is None or new_max > current`). An empty fetch — or one returning nothing
newer than the current watermark — writes no cursor. A watermark is never
synthesized from a window boundary; doing so would assert coverage backed by zero
observations and silently abandon the historical window the moment it went
momentarily empty.

**Feed-token semantics: persist on every successful fetch.** The feed token is
provider-issued (GeoTab's `toVersion`), not fleetpull-computed; GetFeed returns a
`toVersion` on every page, including an empty one. The caller persists it after
every successful page-through, empty or not — versions are append-only sequential,
so persisting the latest never skips a future record. The empty-window/no-cursor
problem is exclusively a `DateWatermark` concern; the feed arm always has a cursor
to write.

**Resume precedence (no committed cursor).** When `get_cursor` returns `None`
(only reachable for a watermark endpoint that has never committed a watermark),
resume is driven by coverage, not by a synthesized cursor: (1) the data watermark,
`- lookback`, when one exists; else (2) the high-water mark of completed coverage
from the run ledger / work-units (max successful window-end) — a backfill chunk
that completes empty is still completed, so this never re-scans empty history every
run; else (3) the configured `default_start_date`. The `cursors` table only ever
holds (1). Arm (2) is implemented by the run ledger's `coverage_frontier` (below).

**Run ledger (`state/run_ledger.py`).** One row per run — one fetch of one
(provider, endpoint) in one of three sync modes: a snapshot (no range — a full
current-state refetch), a watermark window, or a feed version range; a `mode`
column records which. A sync invocation produces many runs; incremental and
backfill-chunk fetches alike record a run, so the run ledger is the single
coverage source for the work-units backfill too — work-units add claim/resume
mechanics but each unit's execution still records a run, so no second coverage
query is needed. Lifecycle is two-phase: one of `start_snapshot_run` /
`start_window_run` / `start_feed_run` inserts a `running` row (timestamped from
the injected `Clock`, with the range shape its mode requires — three
single-shape entry points, so an impossible arm combination cannot be
expressed); `complete_run` closes it `succeeded` with the row count (and, for a
feed run, the end `toVersion`); `fail_run` closes it `failed` with an error
detail. The range is mode-keyed, mirroring the cursor union: a snapshot run
carries no range, a watermark run carries `window_start`/`window_end`, a feed
run carries `from_version`/`to_version`. That shape — plus a non-negative row
count and a well-ordered window — is enforced both by the per-mode entry points
and by CHECK constraints on the table, so neither a mismatched range shape nor a
malformed window can persist.

**Coverage frontier — resume arm (2).** `coverage_frontier(provider, endpoint)`
returns `max(window_end)` over that endpoint's `succeeded` runs, or `None`. This
is the implementation of resume arm (2) recorded above: a backfill chunk that
completed empty is still `succeeded`, so its window is counted and the history is
never re-scanned. The frontier is watermark-only — feed and snapshot endpoints
never reach this arm (a feed endpoint holds a committed cursor; a snapshot has no
resume). A `window_end` that fails to parse is state-store corruption and raises
`ConfigurationError`, consistent with the other §5 stances.

**Stale `running` rows are diagnostic.** A run whose process crashed leaves a
`running` row; nothing depends on it (the frontier filters `succeeded`, and
resume correctness rests on the cursor and the work-units queue). Reconciling or
reaping stale `running` rows is deferred.

**Work units (`state/work_units.py`).** A backfill decomposes a
(provider, endpoint) range into chunks — `(endpoint, chunk)`, or
`(endpoint, partition-key, chunk)` for endpoints partitioned by an entity (a
vehicle id, a driver id, or any per-endpoint key) — and the work-units store is
the claim queue over them. The caller plans the decomposition (chunk size,
range, partition list) and drives the queue; the store only persists units,
hands them out, and records outcomes — it knows nothing about HTTP, parquet,
chunking, or what a partition key represents (that is the endpoint definition's
concern). The date-window dimension is intrinsic: this queue is the
parallelizable backfill mechanism, i.e. the watermark endpoints; feed endpoints
sweep the version-token stream sequentially and do not use it. Each unit's
execution records a run in the run ledger, so coverage stays single-sourced
there; per-partition completeness, however, lives here (each
`(partition-key, chunk)` is its own unit), while the ledger's coverage frontier
stays date-only. Enqueue is idempotent (`INSERT OR IGNORE` on the natural key,
with partial unique indexes so a null partition key still dedups), so re-running
a backfill plan never duplicates units. A worker claims the next unit atomically
— a single `UPDATE ... WHERE unit_id = (SELECT ... LIMIT 1) RETURNING ...`, safe
under concurrency because WAL serializes writers (no app-level lock) — runs it,
and marks it `done` or `failed`. Lifecycle: `pending → claimed → done | failed`;
`failed` units are re-served on a later pass, and `attempt_count` (incremented at
claim, so crashes count too) caps retries at `max_attempts` so a poison unit lets
the backfill terminate rather than loop. Crash recovery is a startup reset: a
single `fleetpull` invocation runs the whole backfill — many endpoints, each
optionally fanned across many partition keys — as one process, so at startup any
`claimed` row is stale (its worker is gone) and reverts to `pending`; no lease or
heartbeat, sound because the only constraint is not running two invocations
against one state database at once (concurrent invocations are out of scope).

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
scope membership is the closed `QuotaScope` enum (a `str` subclass, so the
registry stays string-keyed) from day one — a future scope split is a new enum
member plus its config limits and one endpoint field, not a redesign.

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
`classifier.py` (the ABC), `auth.py` (the protocol), `page_decoder.py`,
`envelopes.py`, `request.py`; the `ResponseCategory` vocabulary they all
speak lives in `fleetpull/vocabulary/`. The **provider implementations** —
the classifiers, the decoders, the auth strategies — are **peers** of the
contract surface (`network/classifiers/`, `network/decoders/`,
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

### Page-decoder contract (implemented: `network/contract/page_decoder.py`, `decoders/`)

The client owns the page loop and stays blind to its mechanics — pagination
and record extraction alike — the way it is auth-blind and classification-blind:
one loop shape for every provider and for unpaginated endpoints alike. A
per-provider `PageDecoder` (Protocol — the implementations share zero concrete
behavior) supplies `first_request(spec)` (decorate the base spec for page one)
and `decode_page(sent, envelope) -> DecodedPage` (the page's records and its
pagination verdict, read from one validated view of the envelope). Decoders are
frozen dataclasses holding configuration fields only; the client threads the
loop, the decoder interprets each page. Implemented decoders: `SinglePageDecoder`
(unpaginated endpoints — replaces any is-paginated flag),
`MotiveWrappedListPageDecoder` (page-numbered, wrapped-list records),
`MotiveWrappedSinglePageDecoder` (unpaginated, wrapped-list records),
`SamsaraCursorPageDecoder` (cursor, top-level-list records),
`GeotabFeedPageDecoder` (GetFeed `toVersion` feed).

This supersedes the former split `PaginationStrategy` + `RecordExtractor`: the
raw envelope was interpreted twice — once for pagination metadata, once for
records, across two layers — letting it escape the network layer to be re-parsed
downstream. A decoder parses it once: `decode_page` validates the
provider-uniform pagination slice and the record-bearing shape together and
returns both, so the client emits records (a `FetchedPage` carries `records`,
not the raw envelope) and the envelope never leaves the loop. Per-record field
validation — each record object into the `response_model` — remains the
downstream records layer's concern; the decoder owns wire shape, the model owns
field shape.

**Verdict versus raise:** components return verdicts when the consumer must
choose among actions; they raise when only one action exists. `decode_page`
returns a verdict (`DecodedPage.advance` — continue/complete, the client
dispatches). A structurally violating envelope has exactly one action — raise
`ProviderResponseError` ("contract-violating" covers it) — so decoders raise it
directly. A malformed SENT request reaching a decoder is a caller bug and stays
stdlib `ValueError`. Samsara's continuation-without-cursor is the canonical
single-action case: silently finishing would truncate data, the one failure mode
a fetch library must never have.

**Durable progress:** `PageAdvance.durable_progress` carries cursor progress that
must outlive the fetch — GeoTab's `toVersion`, the state layer's FeedToken commit
value — on EVERY page including the terminal one (the terminal page's value is
the resume point; per-page progress is what makes a crash mid-feed resumable).
None for providers whose cursors are fetch-private (Motive, Samsara).

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
out of `page_decoder.py` at its second consumer — the GeoTab
authenticator — because the composition is contract-layer semantics,
not page-decoder semantics. These private slices are not endpoint mirrors
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
| Motive | `/v3/vehicle_locations/{vehicle_id}` verified live: envelope `{"vehicle_locations": [{"vehicle_location": {...}}]}`, `located_at` is UTC ISO-8601 (`Z`-suffixed), one non-paginated page per fetch (so `SinglePageDecoder` fits), and a single per-vehicle fetch spans multiple calendar dates (the sample crossed two) — confirming `split_by_date`'s multi-partition output is load-bearing in production, not a theoretical edge: one fetch genuinely fans into several partitions. |

---

## 9. Records, Flattening, and Schema Derivation

**Models stay pure API mirrors.** Ported Pydantic models carry no use-case
logic. Flattening and schema derivation are generic transforms in
`records/`, written once against Pydantic introspection — this is what
makes GeoTab cheap: define models + endpoints, get flattening and schema
derivation for free.

Flattening: default ON, double-underscore-joined. Nested objects flatten to double-underscore-joined columns (`parent__child`, `parent__child__leaf`); a top-level field keeps its bare name. The join is double because field names themselves contain single underscores — a single separator is ambiguous about the level boundary and would let a top-level field collide with a nested one — and the prefix is applied uniformly (never conditionally on collision), so a column name is a stable function of the access path rather than something that can silently rename when an unrelated field is added. Arrays cannot flatten without exploding rows; default representation is `pl.List` of the inner scalar, overridable per endpoint. The line is structural, never semantic.

Schema pipeline (`records/`): Schema derivation and flattening share one field walk (`records/fields.py`), so a column's name (type side) and its value (value side) cannot drift. Auto-derivation maps the closed scalar set, enums (→`pl.String` — the model already enforces membership), and `list[scalar]` (→`pl.List`), and recurses into nested models to flatten them. A leaf the deriver cannot place — an `Any`, a `dict`, a `list` of models, a multi-arm union — raises (fail fast); the per-endpoint `schema_overrides` escape hatch remains the planned answer for genuine derivation gaps but is unbuilt until a real consumer needs it, at which point it is built complete (the dtype side and the value-serialization side together — a schema-only override is a half-built hatch that errors at construction). There is no runtime required-column check: Pydantic guarantees every validated record carries every declared field, and constructing the frame with the explicit derived schema makes every column present by construction — the guarantee is a test invariant, not a runtime step. Value-level wire-cleaning (a stringly value Pydantic's lax mode cannot coerce) is not a records concern either; it lives on the model as a `field_validator(mode='before')`, under the rule that recovering the declared type is structural (allowed on the mirror) while reshaping meaning is semantic (kept off it). Empty strings normalize to null at the DataFrame boundary, while the models preserve `""` faithfully from the wire.

---

## 10. Public API and CLI

**Programmatic:**

- `iter_records(endpoint, **params)` — typed iterator of Pydantic models, pagination transparent. (Renamed from fleet-telemetry-hub's `fetch_all`, whose "all" misleadingly suggested all endpoints rather than all pages.) This is the escape hatch for consumers who don't want Polars; the dataframe path is built on top of it.
- DataFrame retrieval per endpoint (built on `iter_records` + `records/`)
- Read path over managed storage (single or partitioned) returning a dataframe

**CLI — two verbs, no more:**

- `fetch` — one provider/endpoint/window → parquet or stdout dataframe
- `sync` — config-driven, multi-endpoint, incremental (work units, executors, writers)

---

## 11. Module Layout and the Endpoints Layer

```
fleetpull/
  exceptions.py    # package exception hierarchy (§8) — user-facing: consumers catch these
  vocabulary/      # shared, dependency-free package vocabulary (imports nothing internal)
    response_category.py  # ResponseCategory (§8) — spoken by exceptions, retry, classification
    provider.py    # Provider (§8) — the second vocabulary enum; provider identity, homed in the
                   #   leaf for the same cycle-free reason as ResponseCategory
  config/          # Pydantic models for user-provided YAML, one module per section; the YAML loader joins in a later prompt
    logger.py      # LoggerConfig
    geotab.py      # GeotabAuthConfig (server validated as a bare hostname, §8)
    retry.py       # RetryConfig — attempt budgets, backoff shape, fallback penalty (§7)
    http.py        # HttpConfig — connect/read timeouts, truststore opt-in
    motive.py      # MotiveConfig (base_url, records_per_page, lookback_days, cutoff_days)
    sync.py        # SyncConfig (default_start_date) — the cold-start backfill anchor
  logger/
    setup.py       # package logging setup (setup_logger), driven by LoggerConfig
  network/         # organizational namespace; the surfaces live in the subpackages
    client/        # HTTP transport, retry policy, limiter consultation; consumes the page-decoder abstraction
      transport.py   # TransportClient — the assembled fetch loop and per-attempt pipeline
      profile.py     # ProviderProfile — per-provider auth + classifier bundle
      runtime.py     # ClientRuntime — process-global configs, limiter registry, jitter, sleeper
      page.py        # FetchedPage — the emit type (records + durable_progress)
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
      page_decoder.py  # PageAdvance, DecodedPage, PageDecoder (§8)
    classifiers/   # per-provider classifiers (peers of contract/; import its face): motive.py, samsara.py, geotab.py
    decoders/      # per-provider page decoders (peers of contract/; import its face): single_page.py, motive.py, samsara.py, geotab.py
    limits/
      config.py        # RateLimitConfig (frozen Pydantic)
      bucket_math.py   # pure token-bucket arithmetic (stateless functions)
      limiter.py       # QuotaScopeLimiter
      registry.py      # RateLimiterRegistry
    retry/
      decision.py  # RetryDecision, RandomFractionGenerator, decide_retry — pure retry policy (§7)
  paths/           # filesystem path expansion + dataset-layout utilities (pure leaf)
    resolution.py  # resolve_path + PathInput: lexical absolute-path normalization
    datasets.py    # endpoint_directory: the shared, filesystem-neutral endpoint-dir
                   #   atom ({root}/{provider}/{endpoint}/), used by storage and the
                   #   future metadata layer
    partitions.py  # date_partition_segment + parse_date_partition_segment: the hive
                   #   date=YYYY-MM-DD segment and its strict inverse
  timing/
    clock.py       # injectable Clock Protocol; SystemClock and FrozenClock implementations
    sleeper.py     # injectable Sleeper Protocol; SystemSleeper backing TRANSIENT backoff waits
    codec.py       # pure UTC datetime <-> ISO-8601/date-string conversions (stdlib-only leaf)
  incremental/     # per-endpoint resume state: cursors + window + deriving fn; pure leaf (§4)
    cursor.py      # DateWatermark, FeedToken, IncrementalCursor tagged union
    window.py      # DateWindow — the half-open [start, end) watermark resume window (§4)
    resume.py      # compute_resume — pure DateWatermark -> DateWindow resume function (§4)
  endpoints/       # per-endpoint bindings (the endpoints layer, below) — new fleetpull code
    shared/        # shared binding machinery (no auth here — auth is per-provider
                   #   ProviderProfile, resolved at the composition root)
      base.py      # EndpointDefinition: frozen kw-only dataclass generic over its
                   #   response model (spec_builder, page_decoder, response_model,
                   #   quota_scope, storage_kind, sync_mode, event_time_column) + the
                   #   SpecBuilder Protocol, the SyncMode union (SnapshotMode /
                   #   WatermarkMode / FeedMode), ResumeValue, and StorageKind
      spec_builders.py  # StaticGetSpecBuilder — the shared snapshot spec-builder
      url_paths.py  # render_url_path_template — strict {placeholder} URL-path rendering (fan-out)
    motive/
      vehicles.py  # build_vehicles_endpoint — the Motive vehicles snapshot factory
      vehicle_locations.py  # MotiveVehicleLocationsSpecBuilder + build_vehicle_locations_endpoint — the watermark binding
    samsara/       # net-new when its endpoints land
    geotab/        # net-new; follows the GeoTab removals probe
  polars_typing/   # quarantined re-export boundary for Polars type aliases with no public
                   #   equivalent (e.g. ParquetCompression) — the sole importer of polars._typing
    __init__.py    # re-exports ParquetCompression
  model_contract/  # pure dependency-free leaf: the response-model config policy
    response.py    # ResponseModel config-policy base (frozen, extra=ignore, populate_by_name, strip)
  models/          # pure API mirrors per provider (Motive/Samsara ported from fleet-telemetry-hub)
    motive/        # the Motive model package — a directory per provider (§11 prose below)
      shared.py    # DriverSummary, EldDeviceInfo — embedded shapes shared across endpoints
      vehicles.py  # Vehicle snapshot record (+ AvailabilityDetails / AvailabilityStatus / VehicleStatus)
      vehicle_locations.py # VehicleLocation breadcrumb record (/v3/vehicle_locations)
    samsara/       # net-new when its endpoints land
    geotab/        # net-new
  records/         # the records stage: models -> typed Polars DataFrame
    fields.py      # the shared field walk: classify + enumerate flat leaf columns
    schema.py      # Pydantic model -> {column: Polars dtype}
    flatten.py     # model instance -> flat {column: value} row (None-safe)
    dataframe.py   # build-with-schema + empty-string -> null normalization
    convert.py     # models_to_dataframe: the schema/flatten/build/normalize composition
    validation.py  # raw dicts -> validated models, fail-fast and loud
    event_time.py  # latest_event_time: the max event-time watermark candidate (raw datetime)
  storage/         # the storage layer: a records DataFrame -> parquet
    files.py       # storage path construction: data_file, partition_dir, partition_part_file, temp_sibling_path
    atomic.py      # atomic_write_parquet: the temp-then-rename durability primitive
    read.py        # read_parquet_if_exists: existence-tolerant parquet read (the write's read sibling)
    partition.py   # split_by_date: a frame -> per-UTC-date sub-frames (the date_partitioned write unit)
    partitioning.py # the date-partition prune (delete half): window_dates + existing_partition_dates + delete_partition + prune_window_partitions (§3)
    staging.py     # the date-partition write half: stage_shard + compact_partition (§3)
    frames.py      # frame ops the writers compose: exact dedup + the half-open window predicate
    result.py      # WriteResult: the write report
    writers.py     # DatasetWriter protocol + SingleFile/Partitioned ABCs + Snapshot/WatermarkPartitioned writers + select_writer (feed cells next, §3)
  state/           # SQLite operational state (§5)
    database.py    # StateDatabase shell + DB primitives (connect, verify, WAL)
    migrations.py  # forward-only migration runner (user_version); v1 = cursors + runs + work_units
    cursors.py     # CursorStore + CursorKind: IncrementalCursor <-> cursors rows
    run_ledger.py  # RunLedger + RunStatus: per-run records + the coverage frontier
    work_units.py  # WorkUnitStore: the backfill claim queue (enqueue/claim/complete/recover)
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
- `network/client/` consumes the page-decoder abstraction (`network/contract/page_decoder.py`). Retry and limiter consultation stay interleaved per-request concerns inside the client — splitting them away from the request loop is how the token-per-attempt / token-per-page rules get violated.
- The orchestrator never touches the limiter (§7).

### The endpoints layer

**A thin declarative binding, not a fat base class.** fleet-telemetry-hub's
`EndpointDefinition` carried auth, pagination, request-building, and
response-parsing on one class hierarchy. In fleetpull the network layer already
owns those as separate strategies — auth as a per-provider `ProviderProfile`
(auth + classifier) resolved at the composition root, pagination and record
extraction together as a `PageDecoder`, classification as a `ResponseClassifier`,
per-record validation as the records layer over a response model — so none of
that work remains on the endpoint. An `EndpointDefinition` is a declaration: it composes one
implementation per behavioral axis and states the per-endpoint facts the generic
machinery reads. It executes nothing itself except its spec-builder.

**`EndpointDefinition` is a single concrete frozen dataclass, generic over its
response model; the variation lives in the strategies it holds.** Its fields are
data — provider and name; the `SpecBuilder`; the `PageDecoder` (which yields each
page's records and its pagination verdict from one validated view of the
envelope); the per-record response model; the `quota_scope`; the `SyncMode` (a
marker `SnapshotMode`, a `WatermarkMode` carrying its lookback, or a marker
`FeedMode`); the storage kind; and — settled with `vehicle_locations` — the
`event_time_column` the watermark and date-partitioning read (§3/§5), `'located_at'`
for `vehicle_locations`. Constructed keyword-only, it is the single source of truth
per endpoint, and each tier reads only its slice — the client reads spec-builder,
page-decoder, and quota and emits the decoded records; the caller reads the sync
mode and storage kind and validates the records into the model; records reads the
model. The definition carries only the *static recipe* — the strategies, the
response model, the quota/storage axes, the sync-mode config (the `lookback` on
`WatermarkMode`, the end-cutoff), and the event-time column — all built once from
config; the per-run `DateWindow` is cooked fresh each run by the driver (§4) and is
never on the frozen definition. The one remaining excluded concern is the records
`schema_overrides` hatch (§9), attaching when that layer needs it.

**The spec-builder is the only genuine per-endpoint behavior.** A `SpecBuilder`
is a Protocol with one method, `build_spec(resume, path_values) -> RequestSpec`,
where `resume` is a `ResumeValue` (`DateWindow | FeedToken | None`, §4) and `path_values` carries
a partition key for URL-path fan-out (for example, a per-vehicle locations
endpoint). It builds only the first request — URL, base params, and the resume
injection; the page decoder produces every request after it.

A snapshot's spec-builder is shared, and bindings are factories over config. A
snapshot endpoint translates no resume value (`SnapshotMode` always passes
`resume=None`) and fans out over no path, so its first request is a fixed
`GET base_url + path` carrying no provider- or endpoint-specific logic. That
builder — `StaticGetSpecBuilder` in `endpoints/shared/spec_builders.py` — is
shared across every snapshot binding; per-provider resume translation (watermark
windows, feed tokens) stays in dedicated builders beside their bindings. The first
such dedicated builder is `vehicle_locations`'s watermark spec-builder
(`StaticGetSpecBuilder` is snapshot-only, with no resume and no fan-out): it renders
the per-vehicle path via `render_url_path_template` and injects the run's
`DateWindow` as the provider's window query parameters. The base
URL and page size are provider configuration: a `MotiveConfig` (in `config/`)
carries them, the URL defaulting to Motive's documented host and normalized to
drop a trailing slash, the page size defaulting to Motive's documented maximum.
Because those values are known only after config loads, a binding cannot be a
module-level constant — capturing config at import would freeze a default and
module-level mutable state is forbidden — so each endpoint is a factory
(`build_vehicles_endpoint(MotiveConfig)`) returning the frozen `EndpointDefinition`
the composition root builds for the enabled endpoints and hands to the client.

**Dataclass for the binding, Protocols for the slots — and never a per-provider
subclass.** The behavioral axes differ per provider and sometimes per endpoint,
which is exactly why each is a Protocol with swappable implementations; the
binding that composes them does not itself differ, which is why it is one
concrete dataclass and not an ABC. Subclassing `EndpointDefinition` per provider
is prohibited — it re-braids the per-provider variation back into a class
hierarchy and recreates the predecessor's tangle. Per-provider or per-endpoint
behavior goes into a strategy implementation — a new `SpecBuilder`, a new
`PageDecoder` — never into a field the generic client branches on. The
failure signature is an `if endpoint.name == ...` (or
`if endpoint.provider == ...`) inside the client; the remedy is always a
strategy, never a branch. (Reopen condition: if a per-endpoint fact ever needs to
vary structurally and cannot be expressed as a swapped-in strategy, stop and
revisit — none is known.)

**This is composition polymorphism replacing inheritance polymorphism — more
independent variation, not less.** The four axes now vary freely instead of being
braided into one subclass, and the genericity of the client, records, and storage
layers is the payoff of that isolation, not a cost paid against it: those layers
are written once precisely because the variation is sealed in strategies. The
discipline above is what keeps the trade real rather than a flattening of
polymorphism into config.

Models and bindings are separate packages, each a directory per provider.
`models/` holds pure API mirrors (`models/<provider>/<endpoint>.py` over a shared
config-policy base in `model_contract/response.py`); `endpoints/` holds the
bindings the same way — `endpoints/<provider>/<endpoint>.py`, with the shared
binding machinery (the `EndpointDefinition`, the `SpecBuilder` protocol, the
provider-agnostic spec-builders, and the sync/storage/resume declaration types)
in `endpoints/shared/`. The split keeps models a clean block-lift and the "models
are pure mirrors" invariant crisp, and lets records import the model package
generically. A directory per provider — rather than one module per provider —
keeps each endpoint a small file (one model plus a short binding factory), matches
the file-per-responsibility house rule, and makes a provider package's face the
gather point for exactly that provider's factories when the orchestrator enables
endpoints.

**Fetch assembly: the endpoint declares, the machinery is generic, the caller
sequences.** For one fetch the caller looks up the `EndpointDefinition`, turns the
stored cursor into a resume value (`compute_resume`, §4) and a `path_values`,
calls `build_spec` for the first `RequestSpec`, and hands that spec plus the
endpoint's `PageDecoder`, the provider's `ProviderProfile`, and the
`quota_scope` to the client. The client streams
`FetchedPage(records, durable_progress)` — `AuthStrategy.prepare` per attempt,
the limiter consulted per attempt by `quota_scope`, `ResponseClassifier` per
response, `PageDecoder.decode_page` per page. The caller validates each page's
records into the response model, hands them to records for
generic flattening to Polars, to storage for the merge, and to state for the
advance (cursor after parquet, §5). No layer below the caller holds endpoint
knowledge.

**State is concentrated; almost everything is stateless.** Stateless: the
`EndpointDefinition`, the `SpecBuilder`, the `PageDecoder` (pagination
position rides in the spec's params, not in the decoder), the
`ResponseClassifier`, the response models, records, storage (its "state" is files
on disk), and the per-fetch client. Stateful, and only these: the GeoTab
`AuthStrategy` (it wraps the session token — the one stateful strategy, forced by
GeoTab's protocol), the `RateLimiterRegistry` (token buckets; a process-global
injected dependency), the `state/` layer (the durable operational memory), and
the caller (it conducts the run and owns the thread pools, but its durable state
lives in `state/`). The per-chunk DataFrame is a value, not a stateful component.

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
- Per-endpoint quota scopes for Samsara: a provider metering one endpoint apart adds a `QuotaScope` member (code), while that scope's limits stay config — a code-plus-config change, not config-only.
- **How `date_partitioned` partitions are assembled across the per-vehicle fan-out — the central open question for the `vehicle_locations` write path (§3).** The fan-out is per-vehicle (~1,459 separate `GET .../{vehicle_id}` calls), but a single `date=` partition holds the *whole fleet's* rows for that date, assembled across all those fetches. Options (A) and (B) produce the *same* output — one clean `part.parquet` per date — and differ only in the memory mechanism (RAM buffer vs. disk shards + coalesce); the real question is whether disk-spill is needed or whether small backfill chunks plus a RAM buffer suffice. Deciding factor: backfill chunk sizing (next item) — if backfill is chunked small, each chunk is bounded and a buffer may be enough (A); staging (B) is robust to any volume regardless of chunk size. The tradeoff is staging complexity vs. chunk-sizing discipline atop the existing `work_units` queue.
    - **(A) RAM buffer.** Accumulate one window's fleet data in memory, `split_by_date`, write one `part.parquet` per date at the end. Bounded by window size in steady state; breaks for backfill (e.g. 2024→today is not bounded by `lookback`).
    - **(B) Staging / spill-to-disk (tentative lean).** On run start create a staging area; append each vehicle to a buffer; at a row threshold (e.g. ~500k) split by date and flush shards (`shard-000001.parquet`, …) into per-date staging; at the end `pl.scan_parquet` each date's shards and coalesce (streamed via sink) to the final `part.parquet`, then delete staging. Peak memory is the threshold knob, independent of total volume — handles backfill and avoids the small-files problem in one mechanism. Cost: shard lifecycle, the coalesce step, and staging crash-recovery (clear stale staging on restart; the final `part.parquet`, written atomically at coalesce, is the only durable artifact).
    - **(REJECTED) Per-vehicle multi-part** (`part-{uuid}.parquet`, no coalesce): ~1,459 vehicles × ~7 window-days ≈ 10k tiny files per refresh, compounding every refresh — the small-files problem partitioning exists to prevent. Tens of thousands of few-KB files degrade BigQuery external tables and `scan_parquet` badly. Not viable at breadcrumb scale.
- **Backfill chunking as a config value.** Splitting one large window (e.g. 2024→today) into sub-window units of N days (e.g. 7) does not exist yet and would be user config. It maps onto the `work_units` queue (built): each sub-window is a work unit, claimed and executed in turn. Tied to the deciding factor above.
- **`DatePartitionedLayout`'s exact interface, contingent on the partition-assembly question above.** How it slots against the `Layout` protocol, where the delete step (§3) sits relative to the writes, and whether it receives an accumulated frame or coordinates staging are all unresolved until that settles.

## 14. Next Steps

1. Review/amend this document
2. Build in dependency order: `network/limits/` (done) → auth session manager (done, `network/auth/`) → request contract (done, `network/contract/`: `RequestSpec`, `AuthStrategy` + implementations, `ResponseCategory`/`ClassifiedResponse`/`ResponseClassifier`; `ProviderProfile` deliberately deferred to the client prompt — the bundle rule triggers at three traveling parameters and only two exist) → exception hierarchy (done, `exceptions.py`) → retry policy (done, `config/retry.py` + `network/retry/`) → page-decoder abstraction (done, `network/contract/page_decoder.py` + `decoders/`) → HTTP config + the real GeoTab authenticator (done, `config/http.py` + `network/auth/authenticate.py`) → `network/client/` (done) → `endpoints/shared/base.py` (done) → `records` (done) → `storage` (done: `snapshot`+`single` plus the date-partitioned/watermark leaf primitives; `DatePartitionedLayout` pending — §3/§13) → `state` (done in full — §5) → `orchestrator` → `cli.py`

The `network/client/` step inherits a recorded agenda: classify
prepare-time transport exceptions (the authenticator propagates
`httpx.TransportError` raw and loop-free by design — whether a transport
failure during auth/prepare is retried is the client's call), wire the
exception-hierarchy raise sites (FATAL → `ProviderResponseError`, exhausted
budgets → `RetriesExhaustedError`, auth paths → `AuthenticationError`), and
bundle the two per-provider dependencies that share a session lifetime
(auth strategy, classifier) into `ProviderProfile`, leaving the per-endpoint
page decoder and quota scope to arrive on each `fetch_pages` call.

**Vertical progress.** The Motive `vehicles` snapshot vertical is complete
end-to-end (`client → validate_records → models_to_dataframe → persist`, exercised
by a throwaway hand-run driver). The Motive `vehicle_locations`
date-partitioned/watermark vertical is in progress: the leaf primitives are built
(§3), and what remains is `DatePartitionedLayout` (its interface open, §13), the
net-new watermark spec-builder, the `persist` window parameter and the
`event_time_column` field, and the trivial `VehicleLocation` model port — the last
step of the vertical.

**Deliberately deferred — not blockers for the `vehicle_locations` port.** The YAML
config loader (hardcoded config stands in meanwhile); the full `work_units` backfill
orchestrator (per-provider executor and per-endpoint writer threads — the
`work_units` *store* is built, the orchestrator that drives it is not); and
`metadata.json` generation (cosmetic, projected from SQLite, never read by the
program). The `state/` layer (§5) is built in full; only the orchestrator that
sequences it against fetch and storage remains.

3. Port Motive/Samsara models and endpoint definitions onto the new base
4. GeoTab integration when access lands
