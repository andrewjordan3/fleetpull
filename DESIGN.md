# fleetpull — Design Document

**Status:** Design settled through the current public API and sync orchestration verticals. Built and tested today: the `fetch` public API; configuration-driven `Sync(config_path).run()`; snapshot, watermark, and LogRecord feed orchestration; work-unit planning and crash-resume; per-provider fan-out executors; Motive `vehicles`; Motive `vehicle_locations`; GeoTab `devices`; GeoTab `trips`; and GeoTab `log_records`. Deferred: single-file feed storage cells, a separate CLI wrapper, metadata snapshot generation, and schema overrides for unsupported model shapes. See §15 for the roadmap and deliberately deferred work.
**Name:** `fleetpull` — final. Describes exactly what the package does and nothing more (PyPI availability confirmed 2026-06-10).
**Relationship to fleet-telemetry-hub:** New package, not a rewrite. fleet-telemetry-hub remains in production untouched while fleetpull is built.

---

## 1. Purpose and Scope

fleetpull retrieves fleet telematics data from provider APIs and delivers it as
typed, dtype-coerced, lightly normalized tabular output that stays as close to
the raw API responses as is reasonable.

**In scope**

- Fetching from provider APIs (Motive and GeoTab endpoints are implemented; Samsara support remains represented in shared abstractions and models as it ports; extensible to others)
- Dtype coercion and structural normalization (flattening) so output is predictable
- DataFrame output via the programmatic `fetch` API; config-driven parquet/state output via `Sync`
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
| IANA tz database | **tzdata** declared as a runtime dependency | Windows and slim Linux images ship no system IANA database, and stdlib `zoneinfo` then requires the `tzdata` package — without it the first tz-aware Polars extraction fails with `ZoneInfoNotFoundError`. Surfaced by the live Windows run; declared explicitly rather than trusted to the platform. |

---

## 3. Output Contract and Storage Layout

One schema per (provider, endpoint). No exceptions, no cross-endpoint merging.

Each endpoint gets its own folder containing its data. A future metadata snapshot
may be generated beside the data, but current production runner/storage paths do not
write `metadata.json`:

```
data/
  motive/
    vehicles/              # single-parquet strategy
      data.parquet
      # metadata.json  # deferred, generated from SQLite in a future doc snapshot pass
    vehicle_locations/     # date-partitioned strategy (breadcrumb-scale)
      date=2026-06-01/part.parquet
      date=2026-06-02/part.parquet
      # metadata.json  # deferred, generated from SQLite in a future doc snapshot pass
  samsara/
    trips/
      ...
```

**Storage layout is declared on the endpoint definition, not inferred — and it is *layout only*.** What a merge *does* to the data — full-replace, delete-by-window-then-append, or append-plus-dedup — follows the endpoint's `SyncMode` (§4), an orthogonal axis the storage layer combines with the layout.

- `single` — one parquet file; a merge reads the whole file, applies the `SyncMode`'s write semantics, and rewrites it. Fine for low-volume endpoints (on the order of 10–15k rows/day or less). Snapshot endpoints are always `single` — a current-state snapshot has no event-time dimension to partition on.
- `date_partitioned` — hive-style `date=YYYY-MM-DD` partitions; a merge touches only the partitions the fetch window overlaps. Required for breadcrumb-scale endpoints. Hive layout is read natively by BigQuery external tables and `pl.scan_parquet`.

`metadata.json` generation is **deferred**. When built, it will be a generated
human-readable snapshot projected from SQLite after successful runs and never read
by the program. Today, SQLite is still the single source of truth (see §5), and
production writers/runners emit parquet plus SQLite state only — no dual-write
divergence.

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
Storage is stateless — parquet only, no SQLite, no watermark commit, and no
metadata snapshot. The orchestrator sequences state advancement after a successful
`finalize` (§5); metadata projection remains deferred. The single-file family (`SingleFileWriter` → `SnapshotWriter`) and the
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
`build_endpoint` composes the spec-builder and decoder with
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
every `date=` partition the cell writes is wholly inside or wholly outside
`[start, end)`; there is no sub-partition row-filtering in the cell, so clearing the
window is a *directory* operation (delete whole `date=` folders), not a data
operation. That every written partition is in-window is upheld one layer up, not by
the cell: an overlap-anchored provider can return records whose single
`event_time_column` falls outside `[start, end)`, so the watermark arm filters each
fetched frame to `in_window` before `split_by_date` — only in-window dates are ever
staged, compacted, or pruned — and the fold uses the filtered maximum, so the
watermark never advances past the trailing edge. `in_window` (the row-level
predicate) is therefore applied by the watermark arm and by the single-file
date-window and feed cells, but never inside this directory-only cell.

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

**Writer selection carries the runtime `window`.** The old `persist` sketch is
realized as `select_writer(definition, dataset_root, *, window=...)`: the window is
computed per run by the orchestrator/resume resolver (§4) and passed in — never
read off the definition. `select_writer` validates the pairing against the sync
mode: a date-partitioned `WatermarkMode` endpoint without a window is a wiring bug,
and a `SnapshotMode` endpoint handed a non-`None` window also raises.

**How the fleet's rows for one date are assembled across the per-vehicle fan-out** —
`vehicle_locations` fetches per roster member (fleet-scale `GET .../{vehicle_id}` fan-out) but a
single `date=` partition holds the whole fleet's rows for that date — is settled. A
backfill decomposes into per-chunk work units (`partition_key=None`, §5); a chunk
fans the whole roster at execution, so the partition is replaced with every member's
rows; and the write half (`storage/staging.py`) stages each fetched piece to disk on
arrival (`stage_shard`), folds each date's shards into its `part.parquet` at finalize
(`compact_partition`), and clears the staging afterward. Peak memory is bounded by
the chunk, not the endpoint — a high-volume endpoint stays in bounds via a smaller
chunk, not by streaming. Per-vehicle multi-part files (`part-{uuid}.parquet`, no
coalesce) were rejected for the small-files problem partitioning exists to prevent:
at fleet-scale fan-out over a multi-day window, this would create many tiny files
per refresh, compounding every refresh, and those small files degrade
BigQuery external tables and `scan_parquet` badly — so each date folds to one
`part.parquet`.

The fan-out key source is settled: a provider-listed roster in SQLite, not the
feeder parquet. An endpoint that fans out declares a `FanOutBinding`
(`EndpointDefinition.fan_out`, `None` = fetch once) naming a `RosterKey`; the
`RosterRegistry` maps that key to a `RosterDefinition` — the feeder endpoint and
frame column its members come from, plus the staleness and eviction policy. The
consumer carries only the key, never the feeder. Keys are listed from the
feeder, persisted to a `rosters` table keyed by `RosterKey` `(provider, name)` plus
`member`, and the fan-out reads the roster — never the feeder's
output parquet, which is the user's product and not fleetpull's to depend on.
Roster freshness follows two rules. **Rule 1: every execution of a feeder
endpoint updates its rosters and records a run in the ledger.** **Rule 2: the
endpoint's parquet is written only when the user asked for that endpoint.**
Fetch, roster reconcile, and ledger row always travel together; parquet is the
one output gated on request, and a `runs` row certifies *execution of the
endpoint*, never parquet freshness. Three cases fall out: (1) a fan-out
consumer runs with a stale roster — the coordinator harvests the feeder
(fetch, reconcile, run row; no parquet); (2) the user runs the feeder itself —
one execution through the runner writes parquet, reconciles the roster (the
entry's feeder tap, §14), and records one run row, and a failed run touches
neither parquet nor roster; (3) the feeder runs while the roster is fresh —
the roster is reconciled anyway: feeder runs reconcile unconditionally, and
staleness gates only whether the *coordinator initiates* a harvest. Because
the harvest records its run, `RunLedger.last_success_at` is a sound staleness
key in both directions — a harvest is visible to the next staleness check, and
a user-initiated feeder run can never advance the ledger without reconciling
the roster. One refinement: an empty stored roster is stale regardless of the
ledger verdict (ledger history may predate this coupling). Refresh is
best-effort: a failed harvest marks its run failed and falls back to the
existing roster rather than blocking the fan-out; an empty roster
with no prior listing is a loud cold-start failure. A per-key absence counter gives
eviction hysteresis — append-only is the degenerate (never-evict) case, and for
permanent, absent-means-empty keys like vehicle ids the counter is an efficiency
lever (stop fetching long-retired vehicles), not a correctness one. The pure
reconcile/staleness logic, the `RosterStore`, and the roster refresh coordinator
(when to list, the cold-start guard) are built, and the fan-out loop is closed
by the orchestration entry (`run_endpoint`, §14): a declared binding resolves
its key through the `RosterRegistry`, refreshes via the coordinator, reads the
members from the store, and fans out — the caller sees none of it. Roster
registration is explicit construction at the composition root
(`RosterRegistry([VEHICLE_IDS_ROSTER, ...])`); the Motive `vehicle_ids`
declaration lives beside its feeder in `endpoints/motive/vehicles.py`. The
include-inactive guarantee binds at the feeder population — `/v1/vehicles`
lists inactive and retired vehicles — not at eviction policy, so historical
windows cover vehicles that are inactive today.

---

## 4. Incremental Model

Per-endpoint incremental state is an **opaque cursor, not a datetime** — a
tagged union:

- `DateWatermark` — Motive/Samsara style: resume from `watermark - lookback`.
- `FeedToken` — GeoTab `GetFeed` style: resume from `fromVersion`/`toVersion` token. No date windows exist for these endpoints.

*Amendment (2026-07-13, the trips vertical): the provider↔strategy pairing
above is per-endpoint, not per-provider — the earlier working rationale
"GeoTab's incremental story is feeds only" is superseded. Windowed `Get`
(`TripSearch` date bounds riding the seek walk) is GeoTab's history/backfill
and utilization-delivery path today, because the watermark arm is built,
live-proven, and its window-refetch semantics absorb Trip recalculation
within the lookback horizon — while the feed arm (the runner arm, the append
writer cells, token-commit crash ordering, and the calculated-feed
reconcile/tombstone design) is unbuilt with open design questions. Feeds
remain the future incremental mechanism, and they CAN be seeded historically
via `search.fromDate` (June-verified bootstrap; §8's own recorded finding) —
only the bare, unseeded `GetFeed` positions its cursor at now. Accepted
residual: a Trip recalculation arriving beyond the lookback horizon is
missed by the windowed path and will be caught when the feed arm lands.
`GeotabConfig` now carries the watermark knobs (`lookback_days` /
`cutoff_days`).*

These two carriers live in `incremental/` — a pure, dependency-free leaf, so
an endpoint can name a strategy without importing the SQLite layer.
Serialization of a cursor to its SQLite form is owned by `state/`, not the
cursor.

**Resume value vs. cursor — and the pure functions between them.** The stored
cursor (`DateWatermark` / `FeedToken`) is not what a request is built from; the
resume value is. For a watermark endpoint that value is a `DateWindow` — the
half-open `[start, end)` window the spec-builder fetches, a frozen carrier in
`incremental/` beside the cursors, so an endpoint names it without importing
`state/`. For a feed endpoint the resume value is the stored `FeedToken` itself,
used directly — no transformation, so the feed arm needs no resolver. The
watermark arm resolves its window from three pure functions in
`incremental/resolution.py` (`resolve_resume_start`, `resolve_trailing_edge`,
`window_or_none`, below), not from the cursor alone — each pure (no clock, no
I/O; the orchestrator does the SQLite and clock reads and feeds the values in),
so window resolution is a composition of stateless leaves, not a method on the
endpoint definition and not a strategy. The `DateWindow` carrier enforces its one
structural invariant — `start < end`, well-ordered — and defers UTC validity to
the codec boundary exactly as `DateWatermark` does. The half-open convention is
what lets the delete-by-window predicate and the start-anchored append-filter
share one boundary rule and never double-count at a window edge.

**The window is cooked per-run by the orchestrator, not stored.** The
orchestrator resolves each run's `DateWindow` fresh from the three helpers and the
resume precedence (§5) and drives the run's write with it (§3) — the frozen
`EndpointDefinition` never carries a window. The window's `end` is `today - cutoff`
rather than the literal `now`: `cutoff` is a config value (e.g. 1 day, or 0 for "up
to now") that holds the trailing edge back so a still-arriving day is not frozen
prematurely — it is the `now`/`cutoff` pair `resolve_trailing_edge` takes.
`default_start_date` (config) is only the first-backfill anchor — arm (3) — and
goes inert the moment observed data or completed coverage exists; thereafter the
start is `max(observed) - lookback`, floored to the UTC midnight of its date
(flooring below). `lookback` and `cutoff` both sit on
`WatermarkMode`, sourced per-provider from the provider config (`lookback_days` /
`cutoff_days`) — the two ends of one provider-latency concern. The cold-start
`default_start_date` — arm (3) — is sync-wide rather than per-endpoint, so it lives
on the sync-level `SyncConfig`, not on every `WatermarkMode`.

The per-run window is computed by three single-concern pure functions in
`incremental/resolution.py`, composed by the orchestrator (which does the SQLite
and clock reads and feeds the values in): `resolve_trailing_edge(now, cutoff)` —
the end, `now` floored to its UTC midnight less the cutoff;
`resolve_resume_start(watermark_start, frontier, default_start)` — the start by the
resume precedence (arm 1's `watermark - lookback`, else the coverage frontier, else
the cold-start default), the chosen arm **floored to the UTC midnight of its
date**: requests and partitions are day-granular, so the effective window must be
day-aligned on both bounds and every covered date refetched in full — the
floored-window invariant the partitioned writer's wholesale replacement and prune
are safe under (an unfloored `23:59:59` start once covered its boundary date by a
one-second sliver that replacement then destroyed — §15 roadmap item 1). Flooring
makes lookback read as "re-cover N whole days before the watermark's day"; arms
(2)/(3) are midnight-aligned by construction, so the floor binds on arm (1). The
rule is for snapshot/point-event endpoints; duration/span endpoints (e.g. HOS
periods crossing midnight) need their own boundary policy when they arrive. The
arms are passed as pre-resolved datetimes so the helper stays pure datetime math
with no cursor dependency; and
`window_or_none(start, end)` — a `DateWindow` when `start < end`, else `None`. That
`None` is load-bearing and is why the start is resolved as a verdict rather than a
raise: a caught-up window (`start >= end`, e.g. a watermark sitting inside the
still-arriving day) means "no work this run", which the orchestrator dispatches on
— an inverted window is never a valid control-flow value here. The
future-watermark guard — a watermark dated past `now`, which would otherwise stall
the endpoint as a permanent caught-up — is deliberately not baked into these
helpers; it lands in the orchestrator that adopts them.

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
running code is refused. Today head is v2: v1 is the cursors, runs, and
work_units tables, and v2 adds the rosters table.

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

**Work units (`state/work_units.py`).** A backfill decomposes into one unit per
date chunk — `(provider, endpoint, chunk)` with a null `partition_key`; a fan-out
endpoint's chunk fans the whole roster at execution (the unit carries no member).
The `partition_key` column is retained for a genuinely per-entity decomposition
later. The unit is the chunk, not per member, because the date-partitioned writer
replaces each covered date in full, so a unit must cover every member for its dates
— which the chunk run, fanning the whole roster, does. The work-units store is the
claim queue over them. The caller plans the decomposition (chunk size and range)
and drives the queue; the store only persists units,
hands them out, and records outcomes — it knows nothing about HTTP, parquet,
chunking, or what a partition key represents (that is the endpoint definition's
concern). The backfill decomposition aligns chunks to whole UTC days, because
the date-partitioned writer replaces whole date partitions and a mid-day chunk
boundary would drive partial-day replacement and corrupt them. The date-window
dimension is intrinsic: this queue is the
parallelizable backfill mechanism, i.e. the watermark endpoints; feed endpoints
sweep the version-token stream sequentially and do not use it. Each unit's
execution records a run in the run ledger, so coverage stays single-sourced
there; per-chunk completeness lives in the queue (each chunk its own unit), while
the ledger's coverage frontier stays date-only. Enqueue is idempotent (`INSERT OR IGNORE` on the natural key,
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

### Per-provider executors (built: `orchestrator/executors.py` + `orchestrator/fanout.py`)

A single shared thread pool + blocking `request_slot()` would cause
cross-provider starvation: a Motive 429 penalty parks workers holding pool
slots, stalling Samsara/GeoTab work. Therefore: **one executor per enabled
provider, sized `max_workers = max_concurrency`**, created at `Sync.run()`'s
composition root as a context-managed `FetchPoolRegistry` — shut down
deterministically when the run ends, success or failure — and injected down
the collaborator chain (the `Clock` / sleeper / transport seam family) to the
fan-out driver as a `FetchPool` (the executor plus the channel bound below).
Tests inject a synchronous same-thread executor through the same seam. The
limiter's concurrency semaphore is exactly matched, never exceeded, by the
orchestrated path and is kept as belt-and-suspenders — it protects the
invariant for any caller outside the orchestrator. The limiter remains the
only enforcement point on the wire; the pool supplies workers and never
becomes one.

The concurrency grain is the (member × window) piece within one endpoint's
fan-out run. Workers **fetch only** — each runs one member's full request
chain, acquiring tokens and the concurrency semaphore per attempt exactly as
the serial path did; validation, dataframe assembly, and every write stay on
the single consumer thread (§3's single-writer discipline, untouched).

**The bounded-streaming property.** Fetched pieces flow through a bounded
channel (`stream_pieces`: a bounded submission window over futures, drained
in submission order) and are never collected: at most `submission_window + 1`
(= `2 × max_workers + 1`) fetched-but-unwritten pieces exist at any moment —
a function of the pool size, never of the member count. Backpressure reaches
the workers: when the writer lags, no further piece is submitted, so
rate-budget tokens are never burned on results that cannot yet be written.
Submission-order draining keeps the yield order identical to the serial
loop's, so correctness never assumes a completion order anywhere.

**Failure semantics.** The first exception the consumer encounters wins:
pieces beyond the in-flight window are never requested (queued futures are
cancelled; unsubmitted members are never submitted), in-flight requests are
allowed to finish and their results discarded (a discarded piece's own
failure is logged, never raised over the first), and the run fails by
re-raising the first exception — the same loud outcome as the serial loop.

Explicit non-goals of this cut: `Sync`'s endpoint loop stays sequential (the
concurrency grain lives inside one endpoint's fan-out); provider-parallel
activation awaits a second ported provider (the executors are per-provider
by construction, which is all the shaping this cut does); and page-fanning
within one member's chain awaits envelope verification and will be recorded
with its own design when it lands.

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
machinery honors the names-at-composition-root rule. As built, the
composition root reaches this factory through the auth ingress's GeoTab arm,
which passes `QuotaScope.GEOTAB_AUTHENTICATE` as that name.

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

### GeoTab probe-settled decisions (2026-07-09)

Settled as design by the live probe session (the captured rows in the
observed-behaviors table below); the build prompts implement them.

1. **Device harvest mechanism: sort-seek paging by `id` ascending.** The
   initial request carries the `sort` object (`sortBy: "id"`) with a null
   `offset`; each advance sets `offset` to the last returned id; termination
   is the empty result list; `lastId` is never sent with id-sort (docs:
   `ArgumentException`). Rationale: the silent 5,000 cap makes a single
   uncursored `Get` unsound for any cappable entity, so `SinglePageDecoder`
   is ruled out for capped `Get` entities and a seek-paging Get decoder
   joins the PageDecoder family.
2. **Completeness guard: `GetCountOf` beside every Device harvest.** The
   harvest streams unbuffered while a running record count accumulates;
   after the terminal page one `GetCountOf` fires through the same client
   and quota scope, and a mismatch raises `ProviderResponseError` naming
   both counts — the run fails loudly, staging is discarded so the prior
   parquet stands, and the next scheduled run is the retry. Rationale:
   silent truncation must be loud; the same doctrine family as the
   empty-listing reconcile guard (§13). Placement, settled at the build:
   the guard is driver-layer, declared on the definition
   (`EndpointDefinition.completeness_check`, an optional narrow Protocol
   whose GeoTab implementation is `GetCountOfCheck`) and honored by the
   single-fetch driver — the runner, the entry, and every orchestrator
   module stay provider-agnostic. Snapshot-scoped by construction: an
   expected-count comparison is meaningful only against a complete listing
   — a windowed harvest is deliberately partial and a fan-out run is
   per-member, so either pairing is rejected at definition construction.
   (Amended 2026-07-13: the original refetch-once and its buffering were
   dropped — a mismatch fails the run and the next scheduled run is the
   retry; exact-match with no tolerance number is unchanged.)
3. **Rate posture: self-limit at the header-advertised per-class budgets**,
   regardless of whether GeoTab is enforcing yet; GeoTab's rate-limit config
   defaults cite the captured headers, and no ramp probe is needed — the
   headers are the probe. Runtime posture is unchanged: the reactive control
   loop stands and the headers stay unconsumed at runtime (the table row
   below); they informed the configured budgets, nothing else.
4. **Feed semantics: the bootstrap is *ingested-since*.** Date watermarks
   are never derived from feed `dateTime`s — the version-order capture (the
   feed is version-ordered, with `dateTime` dipping before the requested
   `fromDate`) is the empirical justification for `FeedToken` opacity
   (`incremental/cursor.py`, §4/§5): the token is the only sound resume
   coordinate. Date-partitioned feed storage assigns partitions per-record
   and tolerates event-time disorder by construction.
5. **Accepted residual: the exactly-full-final-page feed edge.** When the
   final feed page holds exactly `resultsLimit` records, the decoder issues
   one extra call that returns empty — the worst case is one empty
   `GetFeed` per sync. Accepted, not open; recorded in §13 with this
   rationale.

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
├── RetriesExhaustedError
└── SyncFailuresError
```

| Exception | Consumer action |
|---|---|
| `ConfigurationError` | Fix local config/wiring before rerunning. |
| `AuthenticationError` | Fix credentials / account access. |
| `ProviderResponseError` | Provider response was non-retryable or contract-violating; do not blindly rerun. |
| `RetriesExhaustedError` | The transient/rate-limit budget ran out; rerunning later is reasonable. |
| `SyncFailuresError` | One or more endpoints failed inside a sync run whose siblings continued; inspect `failures` (per-endpoint, in run order) and act per member. |

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

### Observed provider behaviors (verified June–July 2026)

| Provider | Behavior |
|---|---|
| GeoTab | Application errors arrive inside HTTP 200; `error.data.type` is the authoritative discriminator (present in every captured failure). |
| GeoTab | `InvalidUserException` covers BOTH bad credentials and dead sessions — distinguished only by message text; context disambiguates (data call → invalidate + one retry; Authenticate itself → fatal). |
| GeoTab | `OverLimitException` pairs with an integer `Retry-After` header (e.g. `56`). |
| GeoTab | Success responses carry `X-Rate-Limit-*` budget headers; deliberately unconsumed — the reactive control loop (configured budgets plus the 429 penalty) is the v1 design, and a second feed-forward loop is rejected. Re-litigate on sustained 429 churn on GeoTab in production. |
| GeoTab | `toVersion` is a string cursor; `GetFeed` with `search.fromDate` supports historical bootstrap (feeds the state design). |
| GeoTab | `Authenticate` re-run returned `path: "ThisServer"`; the envelope byte-shape matches the June fixture (captured 2026-07-09). |
| GeoTab | `Get` honors `resultsLimit` exactly when at or under the cap (captured 2026-07-09). |
| GeoTab | `Get` hard-caps at 5,000 records **silently** — no continuation signal or metadata of any kind; a `resultsLimit` above the cap still returns exactly 5,000 (captured 2026-07-09). |
| GeoTab | `GetCountOf` returns the true entity count; a captured account returned an entity count above the silent 5,000 cap, proving that records beyond the cap are invisible to a bare `Get` (captured 2026-07-09). |
| GeoTab | Sort-seek paging works as documented: `sort.sortBy: "id"` ascending with `offset` set to the last returned id; page sums matched `GetCountOf` exactly; adjacent page-boundary ids were numerically consecutive — no loss, no overlap at the seam; the terminal signal is an **empty result list**, now captured. `lastId` must not be sent with id-sort — `ArgumentException` (docs, not captured) (captured 2026-07-09). |
| GeoTab | Device schema is polymorphic across device generations and types: at least three shapes observed (GO7-era, GO9-era, and untracked/trailer entries with `deviceType: "None"`, `productId: -1`, and a `tmpTrailerId` field absent from the others); one tracked record lacked `deviceFlags`/`devicePlans` entirely — modeling requires the union of fields with everything optional (captured 2026-07-09). |
| GeoTab | Sentinel values: `activeTo: 2050-01-01` means active, and retired devices remain listed — the full-harvest property holds on GeoTab; VIN fields carry `""` and a literal `"?"`; `ignoreDownloadsUntil` observed at both `1986-01-01` and `0001-01-01` — the year-one value overflows nanosecond-precision timestamp columns, so datetime parsing of such fields requires microsecond precision or exclusion (captured 2026-07-09). |
| GeoTab | A bare `GetFeed` (no `fromVersion`, no `search`) returns empty `data` with the **current** `toVersion`: cursor-at-now, not replay-from-start — which explains the June fixture's empty data (captured 2026-07-09). |
| GeoTab | The `search.fromDate` bootstrap returns records and a walkable token — the June acceptance is now backed by a data-bearing capture (captured 2026-07-09). |
| GeoTab | The feed is **version-ordered, not event-time-ordered**: `dateTime` was non-monotonic within a page and dipped before the requested `fromDate` (buffered device uploads land later versions with earlier event times). Bootstrap semantics are *ingested-since*, not *occurred-since* (captured 2026-07-09). |
| GeoTab | Token advance is strict: records carry versions strictly after the sent `fromVersion`, no overlap; record ids and `toVersion` share one counter space — the docs' own example pair `"b14C3EE"` ↔ `"000000000014c3ee"` shows the mapping (captured 2026-07-09). |
| GeoTab | A short page (`len(data) < resultsLimit`) was observed live as the caught-up shape; the exactly-full-final-page edge remains unobserved (captured 2026-07-09). |
| GeoTab | LogRecord's shape is six fields: `latitude`, `longitude`, `speed`, `dateTime`, `device` (a nested id ref), `id` (captured 2026-07-09). |
| GeoTab | Every response carries `X-Rate-Limit-Limit` (a period), `X-Rate-Limit-Remaining`, and `X-Rate-Limit-Reset`; budgets are per method class — the GetFeed class showed remaining 59 after one call in multiple independent windows (implied 60/min); the Get class showed 649 after one call (implied ~650/min, single datum). The docs state the headers may precede enforcement ("Coming Soon" — docs, not captured). June's captured "Maximum admitted 10 per 1m" OverLimit is consistent with an Authenticate-class budget (captured 2026-07-09). |
| GeoTab | The read-the-type-never-the-message rule's strongest exhibit: an unknown `typeName` returns `MissingMethodException` whose *message* falsely claims the method itself doesn't exist while `error.data.type` stays truthful; a malformed JSON body returns `JsonSerializerException` (code `-32700`) in the same envelope shape (both captured 2026-07-09). |
| GeoTab | Durations are .NET TimeSpan strings, grammar `[d.]hh:mm:ss[.f{1,7}]` — 1–7 fractional digits are 100 ns ticks (every captured seventh digit is zero, so microsecond truncation loses nothing); day-prefixed spans (`"4.16:41:16"`) occur whenever a stop window crosses days (captured 2026-07-13). |
| GeoTab | Trip units, delta-arithmetic confirmed: `engineHours` is SECONDS despite the name (a captured 26.1M "hours" is 7,251 real engine-hours); `odometer` is meters (confirmed against a trip's own km `distance` delta); `distance` fields are km; speeds are km/h (captured 2026-07-13). |
| GeoTab | Trip interval semantics, 12-of-12 captured records: `drivingDuration = stop − start`; `stopDuration = nextTripStart − stop`; `idlingDuration` measures engine-on time WITHIN the post-trip stop window, never within the drive. The zero-distance degenerate shape has `start == stop` and no `averageSpeed` key at all (captured 2026-07-13). |
| GeoTab | Reference fields may arrive as either a bare known-id sentinel string (`driver: "UnknownDriverId"`) or an object (`{"id": ..., "isDriver": true}`) — one field, two wire shapes; modeled by structural flattening (the bare string becomes the reference's `id`, verbatim) (captured 2026-07-13). |
| GeoTab | `sort` and `search` compose on `Get`/`TripSearch`: a windowed, sorted, seeked page pair returned strictly-ascending ids across the boundary with every record inside the window — windowed seek paging works (captured 2026-07-13). |
| GeoTab | An unmatched `search` referent returns an EMPTY result, never an error — the silent-empty hazard: a typo'd search filter reads as "no data", not as a failure (captured 2026-07-13). |
| GeoTab | `Get` `sort` composed with `ExceptionEventSearch.ruleSearch` returned `-32000 GenericException` — open, under discrimination probes; do not assume sort composes with every search type (captured 2026-07-13). |
| Samsara | 429 with fractional `Retry-After` (e.g. `0.40235`); 401 body is `{"message": ...}`; 5xx bodies are plain strings, never JSON. |
| Motive | 401 body is `{"error_message": ...}`; the documented /vehicle_locations limit was not observed to enforce — generic 429 posture. |
| Motive | `/v3/vehicle_locations/{vehicle_id}` verified live: envelope `{"vehicle_locations": [{"vehicle_location": {...}}]}`, `located_at` is UTC ISO-8601 (`Z`-suffixed), one non-paginated page per fetch (so `SinglePageDecoder` fits), and a single per-vehicle fetch spans multiple calendar dates (the sample crossed two) — confirming `split_by_date`'s multi-partition output is load-bearing in production, not a theoretical edge: one fetch genuinely fans into several partitions. |
| Motive | `/v3/vehicle_locations/{vehicle_id}` date bounds pinned by direct probing: day-granular `start_date`/`end_date` are honored inclusively on both bounds — a single-day request returns that full day, a two-day request both complete days. The documented 3-month maximum range is real: long backfills will eventually need request chunking (a range limit, unrelated to the §15 item-1 window defect). |
| Motive | `updated_after` on `/v3/vehicle_locations/{vehicle_id}`: documented as required, observably optional and inert — omitting it and supplying it produced byte-identical responses. It remains a candidate ingestion-time CDC hook for the late-upload gap (§13). |
| Motive | The collection endpoint `/v3/vehicle_locations` (no vehicle id) is a different animal: a last-known-location roster snapshot that ignores date parameters and serves active vehicles only. It is not a history source and must not be conflated with the per-vehicle history endpoint. |

The `updated_after` finding generalizes into a standing rule: **encode probed
provider behavior, never documented behavior alone.** Motive silently
defaults or ignores parameters in both directions — a documented-required
parameter was inert, and a documented rate limit was not observed to enforce
(rows above) — so an endpoint binding's parameters and expectations are
settled against direct probes (or the predecessor's production fetcher),
never against the docs page by itself.

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

## 10. Public API

Two verbs, deliberately not a family. `fetch` is the programmatic convenience
verb for embedding fleetpull inside another program; `sync` is the
config-driven verb for running it as a pipeline. The fluent / method-chaining
surface previously penciled in for this section is **retired** — a decision
from the roadmap item 3 design pass, not drift. Chaining earns its complexity
when a call site composes many options; the settled design's whole point is
that `fetch` has almost no options to compose and `sync`'s options live in a
file, not in code, so the chain had nothing left to chain.

**`fetch` — the programmatic convenience verb.** Minimal arguments: an
endpoint identity from the public catalog, one `auth=` parameter, and little
else — as built (`fleetpull/api/`, the top-tier public-surface package), one
keyword flag: `use_truststore` (default False, named identically to the
`HttpConfig` field it coerces into — the auth-ingress
simple-shape-to-rich-internal pattern applied to transport posture, present
because the `HttpConfig` default is deliberately off yet fetch must still
work behind TLS-intercepting proxies; timeouts and further transport posture
stay config-phase). Returns an eager Polars DataFrame, end-to-end in memory —
it touches no SQLite, no disk, no cursor, no run ledger, no roster. The
governing principle is normative: **fetch is a convenience and deliberately limits options —
anything beyond its minimal surface belongs to the config path.** A user who
wants windows, incremental resume, partitioned storage, or fan-out is not a
`fetch` user with missing parameters; they are a `sync` user.

**Snapshot-only `fetch`, and why.** `fetch` exposes snapshot-mode endpoints
only; incremental retrieval is config/sync territory. The in-memory contract is
only honest for snapshots: a snapshot result is bounded by entity count, while
a incremental result grows with window width and fleet activity — unbounded by
anything the caller controls in memory. The exposed subset is a *type*, not a
runtime allowlist: identity types encode sync mode, and `fetch`'s signature
accepts only snapshot-typed identities, so handing it an incremental identity
fails mypy — backed, as built, by a runtime exposure guard (the first
statement of `fetch` raises `ConfigurationError` naming the endpoint and its
mode, before any client construction), because the convenience verb's
audience includes notebooks where mypy never runs. Starting narrow is the
reversible choice — adding windowed fetch later would be an additive
extension, while shipping it now and retracting it would be a break.

**The `Endpoints` catalog.** Endpoint addressing is a public catalog of inert
typed identities: `from fleetpull import Endpoints`, then
`Endpoints.Motive.vehicles` or `Endpoints.Motive.vehicle_locations` (Samsara
and GeoTab entries join as their endpoints port). Entries are small frozen
public identities carrying the same `(provider, name)` pair the discovery
registry keys on — never the `EndpointDefinition` itself, whose declaration
schema stays private. This is distinct from the ruled-out
provider-namespace-with-methods pattern: the namespaces hold inert data, and
the verb stays flat and provider-agnostic, so the orchestrator-boundary
agnosticism principle (§14) survives — the catalog is organized by provider,
the behavior is not. The module is static and committed, not codegen; the
drift protection is a two-way parity discipline test against the discovery
registry (every exposed identity resolves; every intended-public endpoint
appears), built with the catalog in roadmap item 5. An `available_endpoints`
enumeration rides along as the catalog's manifest.

**Casing.** Provider namespaces are CapWords (`Endpoints.Motive`): they are
class-like public containers, the PEP 8 convention for which is CapWords, and
the shape matches how users think of a provider brand. Endpoint names are
lowercase data attributes. Three casings, one identity: `Provider.MOTIVE`
(the enum member), `Endpoints.Motive` (the public namespace), `'motive'` (the
value string) — and the load-bearing identity everywhere strings live (paths,
wire, YAML keys) is the lowercase value, so the Python-surface casing
introduces no string drift.

**Auth.** One `auth=` parameter. A bare string for single-credential
providers (Motive, Samsara). GeoTab requires named fields — a plain dict or
the existing `GeotabAuthConfig`, the caller's choice — because its credential
is four fields (`username`, `password`, `database`, `server`) and no
positional convention exists past username/password. Tuples are rejected in
both directions: the 1-tuple requires the trailing-comma trap (`('key',)`),
and the 4-tuple invites transposed fields discovered only at auth-failure
time. Ingress immediately coerces every accepted shape into the internal
`SecretStr`-carrying auth, so nothing loggable survives past the boundary —
the same lax-boundary-strict-interior posture `SyncConfig` already takes
coercing string dates and paths. An `auth` whose provider mismatches the
endpoint identity's provider is a `ConfigurationError`. Settled 2026-07-09:
profile construction takes a `ProviderProfileContext` (HTTP config, limiter
registry, clock) alongside the identity and the credential — the union of
composition-root collaborators a provider's auth machinery draws on (GeoTab's
session stack consumes all three; Motive ignores it). The context grows only
when a provider's auth machinery demands a new collaborator; it is not a
general-purpose bag.

**Return contract.** An eager `polars.DataFrame`, dtype-coerced per the
endpoint's model. Column order is deliberately unspecified in the contract:
it falls out of model declaration order deterministically in fact, but is not
promised, so reordering model fields in a later release is not a break —
ordering columns *for* the user would presume a use case, which the scope
line below forbids. An empty result is a zero-row frame carrying the full
typed schema — never `None`, never a schemaless frame — so downstream
`filter`/`select` code behaves identically on empty and populated results.
Polars is the only supported frame library for now; others are out of scope.

**Public exceptions.** The documented `Raises` promise: consumers catch
`FleetpullError` or its public subclasses — `ConfigurationError`,
`AuthenticationError`, `RetriesExhaustedError`, `ProviderResponseError` (§8),
and `SyncFailuresError`, the aggregate a sync run raises after letting
siblings continue: it carries the per-endpoint failures (`EndpointFailure`:
provider, endpoint, the caught exception) in run order, and is deliberately
not an `ExceptionGroup` so the documented `except FleetpullError:` contract
keeps catching it. Every other exception type is internal and renameable.

**`sync` — the config-driven verb.** Constructed on a path to the YAML config
(`Path` or `str`); a `run()` method returning `None`; failure signaled by
raising. Endpoints inside one sync run and commit independently — a sibling's
failure never halts the others. The YAML schema *is* sync's API: designing
sync meant designing the config schema, and that work is now implemented as
`Sync(config_path).run()` plus the validated `FleetpullConfig` schema. `fetch`
was separable and designed first because its vocabulary does not depend on the
schema.

**`Sync`, as built (`fleetpull/api/sync.py`).** Construction is validation
only: the config loads via `from_yaml`, every selected endpoint name is
checked against the public catalog (the validation deliberately absent below
the `api` tier), and zero enabled providers raises — a sync that syncs
nothing is a configuration failure to surface, not a no-op. `run()` applies
the logging section first (`setup_logger`), then composes the whole run from
the validated config: the state database at the resolved
`state.database_path`, the stores, the discovered endpoint and roster
registries, the limiter registry from the precedence-resolved provider
configs, and per-provider client profiles through the auth ingress (which
accepts the config's `SecretStr` directly). Endpoints run sequentially
(concurrency is the next vertical) in feeder-first order derived from the
roster bindings via `sourced_by` — never a user-facing key; config order
stands within ties — and commit independently: an endpoint's
`FleetpullError` is recorded while siblings continue, any other exception is
a bug and propagates immediately, and a run with failures ends by raising
`SyncFailuresError` with every failure in run order. Only the selected set
runs — an unselected feeder is never run on a consumer's behalf; roster
freshness stays the refresh coordinator's job at fan-out time.

**The settled YAML schema (rebuilt — `FleetpullConfig.from_yaml` is the
loading API).** One frozen nested model family IS the schema: the sections
and the models agree exactly, so no loader machinery bridges them (the
vertical-1 masks, injections, and post-validation rewriting are deleted, not
deprecated). Sections: `sync` (`default_start_date` required; optional
package-wide `lookback_days` / `cutoff_days`; optional `backfill_chunk_days`,
default 7 — the whole-day work-unit width every windowed run's plan tiles its
window into, §13), `storage` (`dataset_root`
required — its one and only home; `SyncConfig` no longer carries it), `state`
(`database_path`, defaulting to `<dataset_root>/.fleetpull/state.sqlite3`),
`logging` (`console_level` / `file_level` / `file_path`; either file key
enables file logging and the missing partner is defaulted — level to DEBUG,
path to `<dataset_root>/.fleetpull/fleetpull.log`), `http` and `retry` (the
existing models' fields, wholesale-optional), and `providers.motive`
(`api_key`, `endpoints`, `base_url`, `records_per_page`, `rate_limit`, and
the per-provider `lookback_days` / `cutoff_days` overrides). Window-knob
precedence: a provider's own key stands; else a declared `sync` value fans
in; else the provider model's documented default — resolved by
`mode='before'` validation on the root over the raw document (thin wrappers
over pure functions in `config/resolution.py`), so any `FleetpullConfig`
validated from a raw document is fully resolved, every path field normalized
through `paths.resolve_path`. Unknown keys are `ConfigurationError`s at
every level (`extra='forbid'` throughout). A provider is enabled iff its
credential resolves AND its `endpoints` list is non-empty: endpoints with no
credential raise at validation (direct construction included), naming the
YAML field and the environment variable; a credential with no endpoints logs
one load-time WARNING and the provider stays disabled. Credentials come from
the YAML literal or fall back to the conventional environment variable
(`MOTIVE_API_KEY`, declared per provider in the providers family), the
literal winning and empty counting as unset — environment access lives in
the `from_yaml` path only, never in validators. Values are `SecretStr` from
parse time on. Endpoint names stay unvalidated strings at this tier — the
catalog lives in `api`, above `config`, so name validation happens at `Sync`
construction. The public windowed-bound vocabulary (`start_date` /
`end_date`) appears nowhere in this schema, by design.

**Vocabulary bound now for item 6.** The public names for windowed bounds are
`start_date` / `end_date` — never bare `start`/`end` — matching the package's
existing `_date` vocabulary (`default_start_date`, the
`lookback_days`/`cutoff_days` day-suffix family, and the wire parameters
themselves). Public bounds are inclusive dates, translated once at ingress
into the internal half-open `[start, end)` UTC-midnight window — normalize at
the boundary, strict inside, the established doctrine (§4, §12).

**The scope refusal, explicit.** Retrieval, dtype coercion, and light
structural normalization only: no cross-endpoint joins, no unified schema, no
warehouse loading, no presumption of the user's use case. The column-order
stance above is an instance of this refusal, not a separate policy.

**What does not survive as current public API.** The earlier `iter_records(endpoint,
**params)` idea is not implemented, exported, or tested today. The current public
surface is `fetch(...)` for snapshot DataFrames and `Sync(config_path).run()` for
configuration-driven parquet/state syncs. A typed model iterator can be revisited
as a future design option, but it is not current behavior. A separate CLI wrapper
would likewise serialize the two verbs rather than introduce a third concept.

---

## 11. Module Layout and the Endpoints Layer

```
fleetpull/
  exceptions.py    # package exception hierarchy (§8) — user-facing: consumers catch these
  vocabulary/      # shared, dependency-free package vocabulary (imports nothing internal)
    response_category.py  # ResponseCategory (§8) — spoken by exceptions, retry, classification
    json_types.py  # JsonScalar/JsonValue/JsonObject — generic JSON aliases spoken by
                   #   the network contract, records, and the orchestrator
    provider.py    # Provider (§8) — the second vocabulary enum; provider identity, homed in the
                   #   leaf for the same cycle-free reason as ResponseCategory
  config/          # Pydantic models for user-provided YAML — one model FAMILY per
                   #   file (different families in different files); the schema and
                   #   the models are the same shape, loaded via
                   #   FleetpullConfig.from_yaml (§10)
    base.py        # ConfigModel — the frozen/extra-forbid/validate-default policy,
                   #   stated exactly once and inherited by every config model
    sections.py    # the run-scoped standalone sections: SyncConfig
                   #   (default_start_date + the package-wide window knobs),
                   #   StorageConfig (dataset_root — its only home), StateConfig
                   #   (database_path — AUD-13's landing); path fields normalize
                   #   through paths.resolve_path at validation
    providers.py   # the provider family: ProviderConfig (quota_scope, rate_limit,
                   #   endpoints), MotiveConfig (api_key, base_url, records_per_page,
                   #   per-provider lookback_days/cutoff_days), GeotabConfig (nested
                   #   GeotabAuthConfig, the two method-class budgets: rate_limit for
                   #   the Get class + authenticate_rate_limit, §8), ProvidersConfig,
                   #   the credential env-var convention map, and the enablement checker
    root.py        # FleetpullConfig — the whole-document root; cross-section
                   #   resolution as mode='before' validators (thin wrappers over
                   #   resolution.py) and from_yaml as the loading API
    resolution.py  # pure raw-document resolution: knob precedence, state-path and
                   #   log-path defaults; no I/O, no env, no logging
    loading.py     # from_yaml's steps: read/parse with actionable errors, the env
                   #   credential merge (the only env access in config), validation
                   #   detail shaping, the disabled-provider warning
    logger.py      # LoggerConfig
    geotab.py      # GeotabAuthConfig (server validated as a bare hostname, §8)
    retry.py       # RetryConfig — attempt budgets, backoff shape, fallback penalty (§7)
    http.py        # HttpConfig — connect/read timeouts, truststore opt-in
    rate_limit.py  # RateLimitConfig — one quota scope's token-bucket budget; each
                   #   provider config defaults its own (AUDIT AUD-12)
  logger/
    setup.py       # package logging setup (setup_logger), driven by LoggerConfig
  network/         # organizational namespace; the surfaces live in the subpackages
    client/        # HTTP transport, retry policy, limiter consultation; consumes the page-decoder abstraction
      transport.py   # TransportClient — the assembled fetch loop, the per-attempt pipeline,
                     #   and fetch_envelope (the one-shot non-paging request surface)
      registry.py    # ProviderClientRegistry — provider -> TransportClient, opened/closed as a unit (§14)
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
      request.py   # HttpMethod, RequestSpec (JSON aliases live in vocabulary/); params is
                   #   single-valued by design — widen to accept sequences when
                   #   a real endpoint demands repeated query keys
      outcome.py   # ClassifiedResponse (the carrier; ResponseCategory lives in vocabulary/)
      classifier.py  # ResponseClassifier ABC + shared transport-exception mapping
      auth.py      # AuthStrategy protocol only (implementations live in network/auth/strategies.py)
      envelopes.py   # validated_envelope_slice — shared validate-or-raise for wire slices (§8)
      page_decoder.py  # PageAdvance, DecodedPage, PageDecoder (§8)
    classifiers/   # per-provider classifiers (peers of contract/; import its face): motive.py, samsara.py, geotab.py
    decoders/      # per-provider page decoders (peers of contract/; import its face): single_page.py,
                   #   motive.py, samsara.py, geotab.py (GetFeed toVersion + seek-paging Get, §8)
    limits/
      bucket_math.py   # pure token-bucket arithmetic (stateless functions)
      limiter.py       # QuotaScopeLimiter
      registry.py      # RateLimiterRegistry + rate_limits_from_configs (per-scope
                       #   values derived from provider configs)
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
    codec.py       # pure UTC datetime <-> ISO-8601/date-string conversions (guards via canon)
    canon.py       # the canonical-UTC surface: ensure_utc (ingress normalizes) +
                   #   require_utc (interior/egress requires, identity) — §12 doctrine
  incremental/     # per-endpoint resume state: cursors + window + resolution helpers; pure leaf (§4)
    cursor.py      # DateWatermark, FeedToken, IncrementalCursor tagged union
    window.py      # DateWindow — the half-open [start, end) watermark resume window (§4)
    resolution.py  # resolve_trailing_edge + resolve_resume_start + window_or_none — pure window resolution (§4)
  endpoints/       # per-endpoint bindings (the endpoints layer, below) — new fleetpull code
    shared/        # shared binding machinery (no auth here — auth is per-provider
                   #   ProviderProfile, resolved at the composition root)
      base.py      # EndpointDefinition: frozen kw-only dataclass generic over its
                   #   response model (spec_builder, page_decoder, response_model,
                   #   quota_scope, storage_kind, sync_mode, event_time_column,
                   #   completeness_check) + the SpecBuilder and CompletenessCheck
                   #   Protocols, the SyncMode union (SnapshotMode / WatermarkMode /
                   #   FeedMode), ResumeValue, and StorageKind
      fan_out.py   # FanOutBinding — the per-endpoint fan-out declaration (names a RosterKey)
      spec_builders.py  # StaticGetSpecBuilder — the shared snapshot spec-builder
      url_paths.py  # render_url_path_template — strict {placeholder} URL-path rendering (fan-out)
    motive/
      vehicles.py  # build_endpoint — the Motive vehicles snapshot factory
      vehicle_locations.py  # MotiveVehicleLocationsSpecBuilder + build_endpoint — the watermark binding
    samsara/       # net-new when its endpoints land
    geotab/
      devices.py   # build_endpoint — the devices seek-paged snapshot factory, its
                   #   JSON-RPC spec-builder, and GetCountOfCheck (the completeness
                   #   guard's GeoTab implementation, §8 probe-settled decision 2)
      trips.py     # build_endpoint — the trips windowed (watermark) factory; the
                   #   TripSearch date bounds ride the seek walk (§4's amendment)
    registry.py  # EndpointRegistry + build_endpoint_registry — the (provider, name) catalog; discovers leaves by walking endpoints.<provider>
  polars_typing/   # quarantined re-export boundary for Polars type aliases with no public
                   #   equivalent (e.g. ParquetCompression) — the sole importer of polars._typing
    __init__.py    # re-exports ParquetCompression
  model_contract/  # pure dependency-free leaf: the response-model config policy
    response.py    # ResponseModel config-policy base (frozen, extra=ignore, populate_by_name, strip)
  roster/          # the fan-out roster leaf: identity, declaration, catalog (imports only vocabulary/exceptions)
    key.py         # RosterKey: the opaque (provider, name) handle a consumer references
    definition.py  # RosterDefinition: a key's source endpoint + column + refresh policy
    registry.py    # RosterRegistry: RosterKey -> RosterDefinition (forward lookup)
  models/          # pure API mirrors per provider (Motive/Samsara ported from fleet-telemetry-hub)
    motive/        # the Motive model package — a directory per provider (§11 prose below)
      shared.py    # DriverSummary, EldDeviceInfo — embedded shapes shared across endpoints
      vehicles.py  # Vehicle snapshot record (+ AvailabilityDetails / AvailabilityStatus / VehicleStatus)
      vehicle_locations.py # VehicleLocation breadcrumb record (/v3/vehicle_locations)
    samsara/       # net-new when its endpoints land
    geotab/
      shared.py    # GeotabTimeSpan (.NET TimeSpan ingress) + bare_id_to_reference
                   #   (the sentinel-or-object reference coercion) — shared across entities
      device.py    # Device — the union-of-shapes snapshot record (GO7/GO9/trailer,
                   #   everything optional; year-one and non-derivable fields excluded)
      trip.py      # Trip — the movement-interval record (Duration columns, the
                   #   driver sentinel flattening, the seconds-despite-the-name
                   #   engine_hours trap)
  records/         # the records stage: models -> typed Polars DataFrame
    fields.py      # the shared field walk: classify + enumerate flat leaf columns
    schema.py      # Pydantic model -> {column: Polars dtype}
    flatten.py     # model instance -> flat {column: value} row (None-safe)
    dataframe.py   # build-with-schema + empty-string -> null normalization
    convert.py     # models_to_dataframe: the schema/flatten/build/normalize composition
    validation.py  # raw dicts -> validated models, fail-fast and loud
    event_time.py  # latest_event_time: the max event-time watermark candidate (raw datetime)
    roster_members.py # extract_roster_members: a frame column's distinct values as roster members
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
    migrations.py  # forward-only migration runner (user_version); v1 = cursors + runs + work_units; v2 = rosters
    cursors.py     # CursorStore + CursorKind: IncrementalCursor <-> cursors rows
    run_ledger.py  # RunLedger + RunStatus: per-run records + coverage frontier + last_success_at
    work_units.py  # WorkUnitStore: the backfill claim queue (enqueue/claim/complete/recover)
    rosters.py     # RosterStore + reconcile + is_roster_stale + RosterDelta: the fan-out roster
  orchestrator/    # run executor + request drivers + roster refresh + fan-out coordinators (§14); concurrency executors (§7)
    outcome.py     # RunOutcome: Executed | CaughtUp — the run result carrier (§14)
    drivers.py     # RequestDriver Protocol + SingleRequestDriver + FanOutRequestDriver — yields FetchedPage per batch (§14)
    runner.py      # EndpointRunner — one endpoint's run transaction; snapshot arm built (§14)
    batch.py       # process_batch: per-batch validate/frame/window + fold (§14)
    streaming.py   # stream_processed_batches: a driver's pages, validated and framed per batch (§14)
    roster_harvest.py # harvest_roster_members: a feeder's complete membership as roster members (drives streaming, no write)
    roster_refresh.py # RosterRefreshCoordinator: refresh a roster when stale (staleness -> harvest -> reconcile -> apply); refresh only, not fan-out
    resume.py      # resolve_watermark_start + should_advance_watermark (§14)
    backfill.py    # plan_backfill_units: whole-UTC-day chunk -> WorkUnitSpecs (§5)
  # cli.py         # deferred future wrapper over fetch/sync, absent today
```

The package root holds user-facing modules only; internal code lives in
subpackages. Settled: ALL Pydantic models parsing user-provided YAML
centralize in `config/` — including `RateLimitConfig`, migrated there from
`network/limits/` ahead of the YAML loader (audit fix wave 1, AUD-12):
provider defaults live on the provider configs (`MotiveConfig.rate_limit`),
and `rate_limits_from_configs` derives the limiter registry's per-scope map,
so no composition root invents rate-limit numbers. Placement for everything else is settled the same
way: the client is transport plumbing and lives at `network/client/`,
alongside the limiter, contract, and auth it consumes; `records`, `storage`,
`state`, and `orchestrator` are internal by the same test (consumers call
the public API, never these) and each receives its own subpackage home when
its prompt builds it — a single-module subpackage is the blessed shape.
`exceptions.py` is user-facing and stays at the root: consumers catch the
exceptions exported from the package face. A separate `cli.py` wrapper is deferred
and absent from the current tree. The exception hierarchy itself — members,
consumer actions, and stances — is recorded in §8.

Boundary rules:

- `storage` knows nothing about state; `state` knows nothing about parquet. The orchestrator sequences them (parquet-then-watermark ordering, §5).
- `network/client/` consumes the page-decoder abstraction (`network/contract/page_decoder.py`). Retry and limiter consultation stay interleaved per-request concerns inside the client — splitting them away from the request loop is how the token-per-attempt / token-per-page rules get violated.
- The orchestrator never touches the limiter (§7).
- `state` and `endpoints` are same-tier siblings and never import each other — this is why the run ledger's `RunMode` shadows the endpoints layer's `SyncMode` instead of importing it (§5, `state/run_ledger.py`).

**Import-linter coverage.** The rules above are mechanically enforced, not
just documented. `pyproject.toml`'s `[tool.importlinter]` carries a
package-wide vertical `layers` contract over every top-level module under
`fleetpull/`:

```
fleetpull.api
fleetpull.orchestrator
fleetpull.storage
fleetpull.endpoints | fleetpull.records | fleetpull.state
fleetpull.models | fleetpull.network
fleetpull.logger
fleetpull.config | fleetpull.roster
fleetpull.exceptions
fleetpull.vocabulary | fleetpull.incremental | fleetpull.timing | fleetpull.model_contract | fleetpull.polars_typing | fleetpull.paths
```

Read top-down: a layer may import any layer strictly below it; a lower
layer may never import a higher one; modules joined by `|` share a tier
and may not import each other (this is what keeps `state` off `endpoints`,
and `records`/`endpoints` off each other). `network` and `endpoints` are
each treated as one opaque node here — their internals are layered
separately (the narrower `network` contract already in `pyproject.toml`,
and `tests/test_import_discipline.py`'s clause-3 face-routing check for
`endpoints`'s provider leaves). Alongside the vertical, a `forbidden`
contract bars `orchestrator` from directly importing `network.limits`,
scoped to direct imports only (`allow_indirect_imports = true`) so the
client's own internal limiter consultation — a legitimate transitive path
through `network.client` — doesn't trip it. `uv run lint-imports` runs
these as the fourth of the five verification gates (CLAUDE.md); an
accidental upward edge fails the build there rather than surfacing later
as a design regression no single change would have noticed.

**Temporal discipline.** `tests/test_temporal_discipline.py` (AST-based,
riding the `pytest` gate like the import-discipline test) enforces the
canonical-UTC doctrine (§12) mechanically over `src/fleetpull/`: no direct
wall-clock reads outside `timing/` — `datetime.now(...)` even tz-aware
(legal by ruff's DTZ rules, yet it bypasses the injectable `Clock` seam),
`datetime.today()`, `date.today()`, `datetime.utcnow()` — and no foreign
tzinfo entering the domain (`zoneinfo` imports, `timezone(...)`
construction). Referencing the canonical constant (`datetime.UTC`) stays
legal everywhere; `timing/` itself is the allowlist (it owns `SystemClock`
and the canonicalization surface); tests are exempt (they construct foreign
tzinfo to exercise rejection).

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
(`build_endpoint(MotiveConfig)`) returning the frozen `EndpointDefinition`
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
the file-per-responsibility house rule, and lets each endpoint leaf expose a uniform
`build_endpoint(ProviderConfig)` factory that `build_endpoint_registry` discovers by
walking the provider packages — so adding an endpoint is adding one leaf module, with
no provider face or manifest to update.

**Fetch assembly: the endpoint declares, the machinery is generic, the caller
sequences.** For one fetch the caller looks up the `EndpointDefinition`, turns the
stored cursor into a resume value (the resume resolver, §4) and a `path_values`,
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

**The endpoint catalog: discovery, not a manifest.** `EndpointRegistry` is a
dumb immutable map from `(provider, name)` to an `EndpointDefinition`, answering
`get(provider, name)` and rejecting a duplicate key at construction with a
`ConfigurationError`. `build_endpoint_registry(configs)` is the one place
endpoints are enumerated: it discovers every leaf by walking the
`endpoints.<provider>` packages for modules exposing the uniform `build_endpoint`
factory, injects each factory's provider config by matching its annotated config
type against the supplied configs (exact-type keying), and indexes the results.
Adding an endpoint is adding one leaf module — no provider list, no registration,
no manifest. The walk reaches leaf modules dynamically rather than through
provider faces; this is a named, deliberate exception to the clause-3
face-routing rule the import-discipline test enforces, justified because the walk
depends only on the `build_endpoint` contract, not on any specific module. Its
replacement guardrail is the structural contract test, which fails loudly if any
leaf lacks a well-formed `build_endpoint`. A new `ProviderConfig` base in
`config/` carries the shared config-model policy (frozen, `extra='forbid'`,
validate-default) and types that config bag; each provider config subclasses it.

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

**Canonical UTC (the temporal doctrine).** The interior temporal form is
exactly one: a timezone-aware `datetime` whose `tzinfo is datetime.UTC` —
identity, not offset-equality. `datetime.date` serves calendar concepts
(timezone-free by nature); strings exist only at wire/storage edges via
`timing/codec.py`. The `timing/canon.py` surface enforces the form in two
verbs: **ingress normalizes** (`ensure_utc` — any function bringing a
temporal value into the domain converts it to canonical form, rejecting only
the genuinely ambiguous naive value, never assumed UTC; `from_iso8601` is
the string-ingress twin), and **interior and egress require** (`require_utc`
— the strict identity guard, never loosened; a strict failure in the
interior means an ingress was missed, and the fix is adding the missing
ingress, not weakening the guard). Identity rather than offset-equality is
deliberate: a zero-offset foreign tzinfo (Polars materializes
`zoneinfo.ZoneInfo('UTC')` out of a frame; pydantic-core tags parsed
datetimes with its own `TzInfo`) is the fingerprint of a value that entered
without normalizing — offset-equality would mask the missed ingress, and
identity is what caught the live watermark-serialization crash. Known
ingress boundaries: `from_iso8601` (strings), `records/event_time.py`'s
`latest_event_time` (the sole site materializing a `datetime` out of a
Polars frame), and `SystemClock` (the wall clock). The
`tests/test_temporal_discipline.py` check (§11) enforces the wall-clock and
tzinfo-construction rules mechanically.

---

## 13. Open Questions

- GeoTab specifics pending API access: `GetFeed` semantics in practice, real rate limits, which entities map to which storage strategies (the auth model is settled — session-based, §8). *Update (2026-07-09): the live probe session closed the `GetFeed`-semantics and rate-limit halves — see §8's observed-behaviors table and probe-settled decisions. Still open: which remaining entities map to which storage strategies, and the calculated-feed questions (version re-emission shape, tombstones — the bullet below), deferred to Trip's port.* *Update (2026-07-13): Trip's mapping is settled — watermark / `DATE_PARTITIONED` on `start` (the trips vertical; §4's amendment carries the rationale and the accepted recalculation residual). Still open per entity: ExceptionEvent pends the sort-failure discrimination (§8's `GenericException` row), and User pends the driver-visibility question — a scope anomaly where trips reference driver ids invisible to the probing account, under investigation with the subsidiary.*

- **Accepted residual (2026-07-09): the exactly-full-final-page feed edge.**
  When a feed's final page holds exactly `resultsLimit` records, the
  short-page termination rule issues one extra call that returns empty
  `data` (with the current `toVersion`). Worst case is one empty
  `GetFeed` per sync per feed endpoint — accepted with that rationale
  rather than left implicitly unresolved; the empty-page terminal shape is
  captured (§8's table). Not an open question.
- Real rate-limit values for Motive/Samsara (YAML numbers above are placeholders)
- Whether any endpoint actually warrants the flattening opt-out
- Per-endpoint quota scopes for Samsara: a provider metering one endpoint apart adds a `QuotaScope` member (code), while that scope's limits stay config — a code-plus-config change, not config-only. (GeoTab's method-class scopes — `GEOTAB_GET` and `GEOTAB_AUTHENTICATE`, emitted from one `GeotabConfig`'s two budget fields — are the first shipped instance of this pattern.)

- **`updated_after` as an ingestion-time CDC hook (open — for the
  incremental-strategy design conversation, not a current commitment).** The
  late-upload gap: a vehicle offline for days uploads old-`located_at`
  records later, and a `located_at` watermark with a fixed lookback will
  never fetch them. Probing showed Motive accepts `updated_after` on the
  per-vehicle history endpoint (though inert as observed, §8), making it a
  candidate for closing that gap at ingestion time rather than by widening
  the lookback.

- **Staging-clear robustness on synced/scanned filesystems (parked for the
  polish phase, §15 roadmap item 8).** A live run on a OneDrive-synced
  `dataset_root` failed in `clear_partition_staging` (`shutil.rmtree` →
  `PermissionError` / `WinError 5`): the sync handler held the staging
  directory during cleanup. Not a correctness bug — at the clear point the
  finalized partition is expected to be already promoted, which would make
  leftover staging cosmetic, not corrupting. The intended fix, pending
  confirmation of that expectation: best-effort removal with a short
  retry/backoff, degrading to a logged warning rather than crashing a run
  whose data landed correctly. Deterministic regression test: hold an
  external handle on the staging directory and assert retry-then-warn — no
  OneDrive required. This is Windows reality, not OneDrive-specific: endpoint
  antivirus scan-on-write produces the same `WinError 5` on fresh writes, in
  exactly the corporate Windows environments fleet telematics runs in. Ships
  with a docs note: `dataset_root` should be a real filesystem path, not a
  live cloud-synced folder.

- **The work-unit transaction boundary (settled — the unified plan-and-drive
  loop; its empty-roster twin was settled earlier).** *Empty roster — settled
  and implemented:* the orchestration entry
  reads the roster (the driver does not, §14) and short-circuits before
  `runner.run()` — an empty roster after refresh raises `ConfigurationError`.
  Error-by-default because a feeder that silently returned nothing is a failure
  to surface, not an empty dataset to emit; this also keeps the writer's
  "`write` called ≥1 time" precondition intact without a separate "tolerate
  zero writes" path (a snapshot always yields ≥1 page-batch, a fan-out with ≥1
  member yields ≥1 batch, and the only zero-batch case never reaches the
  runner). The `FanOutBinding.allow_empty_roster` escape is deliberately not
  built — it joins the binding when an endpoint genuinely needs it, not before.
  *Transaction boundary — settled and implemented:* neither of the two
  candidates, but the third the `WorkUnitStore` was built for — per **unit**
  (a date chunk of the whole roster), not per run and not per member. Every
  windowed run plans its window into `sync.backfill_chunk_days`-wide units (a
  daily window degenerates to one unit) and drives them serially in ascending
  window order; each unit is its own transaction — fetch the unit's window,
  finalize its partitions, advance the watermark on a strictly-forward
  observation, record its ledger row, mark it done. Ascending completion
  keeps completed units a contiguous prefix, so every persisted watermark is
  true at every instant — the truth invariant, and the reason unit order is
  not a free choice. Resume precedence: incomplete units outrank the
  watermark — a run re-claims and drives them first (an in-progress unit
  found at run start is by definition orphaned, because fleetpull assumes a
  **single driver per state database**), then plans the residual, resolved
  exactly as the resume chain always has (watermark less lookback, floored;
  else frontier; else anchor; cutoff trailing edge), as new units at the
  current chunk size — persisted unit boundaries are honored on resume even
  when `backfill_chunk_days` changed. The first failed unit fail-fasts the
  endpoint: it returns to a claimable state with nothing committed, the
  completed prefix stands, and the next run re-claims what remains.
  Completed unit rows are kept, not pruned — the runs-ledger provenance
  doctrine. One emergent consequence, deliberate: a re-invocation whose
  residual window exactly matches an already-done unit's bounds drives
  nothing (the idempotent enqueue collapses onto the kept `done` row), so a
  same-day identical-window re-run is a no-op; the late-arrival margin
  refetch still happens whenever the window shifts.

- **Logging policy (pinned during the concurrency vertical — open questions,
  deliberately unanswered there).** Three decisions, recorded so no scoped
  task preempts them with ad-hoc narration:
  - *Log-line timestamps: UTC vs local-with-offset.* The user leans local
    (the operator reading a console lives in local time); the counterpoint
    is that everything the lines describe — windows, watermarks, partitions
    — is UTC, so mixed clocks invite off-by-a-timezone misreadings.
  - *Handler scope:* configure the root logger or the `fleetpull` package
    logger only — and whether third-party verbosity (httpx and kin) becomes
    a deliberate opt-in rather than an accident of root configuration.
  - *What `Sync` narrates at INFO during long fan-outs:* long fleet-scale live runs
    can currently produce very sparse INFO output. Progress narration (members
    completed, pages fetched, penalties waited out) is the open surface; the
    concurrency vertical added no narration pending this policy (its one new
    log call is the error-path record of a discarded in-flight failure).

---

## 14. Orchestration: the run executor, the request driver, and the client registry

The layer that sequences fetch, records, storage (§3), and state (§5) into one
endpoint's run. Snapshot execution is implemented; watermark execution is
implemented; windowed execution uses planned and persisted work units; fan-out is
implemented; and feed execution remains unsupported and fails closed.

**The orchestrator-boundary principle: higher-level orchestrators and tools are
polymorphic — provider-agnostic and endpoint-agnostic.** A caller invoking an
endpoint never knows, or branches on, the provider, whether the endpoint fans
out, its sync mode, its storage cell, or its record identity; every dispatch
keys off `EndpointDefinition` declarations. `FanOutBinding` (fan-out is a
declared fact, never an identity branch), `select_writer` (the declared
storage/sync cell routes), and the runner's `sync_mode` match all state this
for their seams; `run_endpoint` (`orchestrator/entry.py`) extends it to driver
resolution — the caller boundary that resolves a definition's declared driver
(fan-out via the roster machinery, else single-fetch) and runs. Driver
resolution is module-private inside the entry: exposing a resolve-driver step
to callers would leak exactly the fan-out/single-fetch distinction the
declarations hide. The entry never reasons about roster freshness (the refresh
coordinator owns that policy whole), and an empty roster after refresh raises
`ConfigurationError` — error-by-default (§13): a feeder that listed nothing is
a failure to surface, and the short-circuit keeps the writer's
write-called-at-least-once precondition intact. The entry also owns the feeder
tap (§3's Rule 1): it reverse-looks-up the rosters the definition sources
(`RosterRegistry.sourced_by`); when any exist it installs a generic batch
observer on the run — the runner hands each post-validation frame to an
opaque `BatchObserver` hook and stays roster-blind — collecting each sourced
roster's distinct `source_column` values (values only, never frames), and
after the run returns `Executed` it hands each collected listing to the
coordinator's `apply_listing` — the reconcile choke point, whose guard means
reconciliation is no longer unconditional: **a roster is never reconciled to
empty**. An empty listing (the provider returned nothing, or every member
value filtered out) is a failed refresh, not a membership fact — reconciling
it would mass-increment absence counts and, with an eviction threshold,
evict the entire roster through systematic provider garbage. The prior
membership stands; the harvest route degrades exactly like a failed HTTP
refresh (run marked failed, staleness unadvanced), and the tap route
propagates, failing the endpoint loudly. A failed run applies nothing; a
`CaughtUp` run executed nothing, so there is no listing to apply. A sourced definition that is not snapshot-mode is rejected with
`ConfigurationError` before anything runs — `reconcile` is only correct over
a complete listing, which only a snapshot feeder produces — mirroring the
coordinator's harvest-route guard, with the same rule enforced at build time
by `tests/endpoints/test_roster_discipline.py`.

**The carve: a run executor under which a request driver owns cardinality.** The
orchestration splits into three nested layers, by concern:

- **`EndpointRunner`** (`orchestrator/runner.py`) owns one endpoint's run
  *transaction*: open the run (`RunLedger`), construct the writer (`select_writer`,
  §3), call the request driver, consume each record batch the driver yields
  (validate -> frame -> guard -> `writer.write`), then `finalize` once, advance the
  cursor once, and complete the run once. It is cardinality-blind — it never knows,
  or branches on, how many requests a run makes.
- **`RequestDriver`** (`orchestrator/drivers.py`) owns request *cardinality* and
  yields the run's fetched pages (records and durable progress) as a stream of
  batches — the run executor reads the records to validate/frame/write and the
  durable progress to advance a feed cursor. `SingleRequestDriver` issues one
  request chain (`path_values={}`) and yields its pages a page at a time;
  `FanOutRequestDriver` issues one request chain per member
  (`path_values={path_placeholder: member}`), yielding each member's pages — the
  member list the caller's, one member per backfill unit, the whole roster per
  incremental run. Both drivers yield one page per batch; the runner consumes
  batches uniformly. A driver touches only the client (from the registry) and the
  endpoint's `SpecBuilder`, and yields whole fetched pages; it does no validation,
  framing, or writing. **`path_values` live only in the driver** — the runner never
  writes them and the coordinator never supplies them.

**`stream_processed_batches`** (`orchestrator/streaming.py`) is the fetch-and-frame
pipe between the driver and the writer: it drives the request driver's pages and
runs each through `process_batch`, yielding one `ProcessedBatch` per page as a lazy
generator — each page framed and handed off before the next is fetched, preserving
the partitioned writer's per-page memory bound. Both run-executor arms drive it, the
snapshot arm with `context=None` and the watermark arm with a `WindowContext`, and
`harvest_roster_members` (`orchestrator/roster_harvest.py`) drives it to list a
feeder's complete membership without writing. It
owns no state and resolves no client; the conductor opens the run, picks the client,
and consumes the stream.

**The roster refresh coordinator** (`RosterRefreshCoordinator`) makes a stale roster
current on demand: `last_success_at` -> `is_roster_stale` -> harvest the feeder ->
`reconcile` -> `RosterStore.apply` (§5), guarding that the feeder is a snapshot
endpoint and degrading to the existing roster when a refresh attempt fails
(re-raising only on cold start, where there is no roster to fall back to). It is
handed the resolved `RosterDefinition`, the way the runner is handed a resolved
`EndpointDefinition`. **The orchestration entry** (`run_endpoint`,
`orchestrator/entry.py`) is the consume half: it reads the refreshed members,
builds a `FanOutRequestDriver` from them, and hands it to the runner.
`EndpointDefinition.fan_out` is read in exactly one place: there.

The driver is the missing adapter between one endpoint run and one-or-many request
chains, and it matches grain the existing layers already have: a `SpecBuilder`
builds one first request from `path_values`, `TransportClient.fetch_pages` drives
one chain from one first spec, and a `DatasetWriter` accepts one-or-many frames and
finalizes once. This resolves the §13 question on how a date partition's rows
assemble across the per-vehicle fan-out: the driver yields per vehicle, the runner
writes per vehicle, and `stage_shard` lands each piece to disk immediately (§3), so
the fleet's rows for a date assemble across per-vehicle `write` calls bounded by one
chain's records — never a RAM buffer holding the fleet. Backfill chunk sizing is
implemented: `sync.backfill_chunk_days` controls planned work-unit width, a
smaller-than-chunk residual window becomes one unit, units are driven serially in
ascending order, completed units are not refetched during crash recovery, and
persisted unfinished unit boundaries survive later configuration changes.

**The run is constructed, not self-assembling.** The `EndpointRunner` is injected
with a client source (the `ProviderClientRegistry` surface that answers
`client_for(provider)`), a bundled `RunStateAccess`, the shared `Clock`, and the
root `FleetpullConfig`. `RunStateAccess` groups the state responsibilities the
runner needs: the run recorder/ledger, cursor access, and work-unit queue. The
runner reads the root config for `sync.default_start_datetime` (the cold-start
anchor), `sync.backfill_chunk_days` (the planned work-unit width),
`storage.dataset_root` (where writers land), and `storage.drop_exact_duplicates`
(the writer dedup policy). The `EndpointDefinition` and the driver are `run()`
arguments, not constructor fields, so one runner instance can run multiple
endpoints with the driver each caller resolved. The runner constructs no clients
and reads no credentials. One `Clock` instance is shared by the runner, state
stores, and the limiter inside the registry's runtime — otherwise run timestamps,
window resolution, and future guards skew apart.

**`ProviderClientRegistry`** (`network/client/registry.py`) owns
`{provider: TransportClient}` and answers `client_for(provider)`. It is a
resource-owning context manager: handed the per-provider `ProviderProfile`s and the
one shared `ClientRuntime` (it builds neither — credential and config composition is
the composition root's job, not the registry's), it opens every enabled provider's
client on enter, closes every connection pool on exit, and raises
`ConfigurationError` for an un-enabled provider. The single shared `ClientRuntime` is
what keeps cross-provider quota enforced — every page attempt routes through its one
`RateLimiterRegistry` (§7). This separates transport identity (which provider's
auth/classifier/pool) from endpoint execution (decoder, quota scope, model, storage,
sync mode — all carried by the `EndpointDefinition`): the runner asks the registry
for `definition.provider`'s client, and the endpoint supplies the rest.

**The watermark arm resolves its window from pure functions, then orchestrates.**
Each run resolves a fresh window: `resolve_trailing_edge(now, cutoff)` floors `now`
to its UTC midnight and holds it back by the cutoff (the end); the start is the
resume precedence — `resolve_watermark_start` turns the stored cursor into arm 1
(`watermark - lookback`, carrying Guard A and the cross-mode feed-cursor rejection),
else the coverage frontier (arm 2), else `default_start_date` (arm 3), composed by
`resolve_resume_start`, which floors the chosen arm to its UTC midnight (the
floored-window invariant, §4); `window_or_none(start, end)` yields the `DateWindow`
or `None` (caught up → `CaughtUp`, no run opened). The partitioned writer carries
the matching interior tripwire: a staged partition date outside
`window_dates(window)` fails the run loudly — an upstream window filter missed
rows (the require-inside half of the normalize-at-boundary doctrine, §12). These decisions are pure and live in
`orchestrator/resume.py` (cursor interpretation and its guards) and
`incremental/resolution.py` (cursor-free date math); the runner reads the cursor,
the clock, and the frontier, calls them, and writes — no resume logic on the class,
the same split as `process_batch` in `orchestrator/batch.py`. The per-unit transaction
— open the run, drive/write/finalize, advance the cursor, complete — is the
shared spine `_execute_window`, in the parquet -> cursor -> ledger order below.
`_run_watermark` is the plan-and-drive loop (§13's settled record): it drives
every claimable work unit through the spine, each with the freshly read prior
cursor; the cursor advances only when `should_advance_watermark` confirms the
unit's folded in-window maximum is strictly past that prior (the monotonicity
the cursor store omits) and only when the unit observed at least one in-window
event; the `set_cursor` write is inline, between `finalize` and `complete_run`.
Serial ascending units make the per-unit advance sound — the out-of-order
concern that once suppressed the advance on backfill chunks is gone with
out-of-order execution itself. A unit fans the whole roster, so each partition
is replaced with every member's rows, exactly the in-full refetch the
partitioned writer already assumes — the writer is unchanged.

**Crash-safety ordering — parquet, then cursor, then ledger.** §5 fixes
parquet-before-cursor; the run executor adds a second ordering, cursor before run
completion. A succeeded watermark run feeds `coverage_frontier` (resume arm 2, §4),
and arm 2 applies no lookback. So if `complete_run` landed before `set_cursor` and
the cursor write then failed or crashed, the next run would find no cursor but a
frontier, resume from the frontier without lookback, and skip late arrivals inside
the window just written. The order is therefore **parquet -> `set_cursor` ->
`complete_run`**: a crash between the cursor and the completion leaves the watermark
advanced (arm 1, lookback applies) and the run merely `running` (diagnostic-only —
the frontier filters `succeeded`), which is the protective state. Snapshots are
unaffected: they hold no cursor and never reach the frontier.

**Two future-time guards, one rule applied where it can surface.** The `CursorStore`
enforces no advance discipline by design (§5), so the watermark arm owns the
future-time checks — both in the pure helpers it calls. *Guard A*, in
`resolve_watermark_start` before window resolution: a persisted `watermark > now` is
corruption and raises `ConfigurationError` — otherwise a future-dated cursor becomes
a permanent "caught up". *Guard B*, inside `process_batch` on the raw frame **before**
the window filter: an observed event-time maximum past the run's captured `now` raises
`ProviderResponseError` before any parquet is written — because a future-dated row
would `split_by_date` into a `date=<future>` partition outside the resume window,
which the window-bounded `prune_window_partitions` (§3) never reaches, leaving an
orphan partition beyond the prune horizon. Guarding the *raw* frame is what surfaces
the anomaly: the window's end is held back to at or before `now`, so a future-dated
record falls outside `[start, end)` and the window filter would otherwise drop it
silently. Both guards are one rule ("no event-time after now") at the two points it
can surface.

**What the runner tracks and returns.** The ledger's row count is `records_fetched`
— the summed `len(models)` across batches — not `WriteResult.rows_written`; the write
report's counts (dedup, pruning, partitions touched) are a different quantity, kept
for logging. The watermark candidate is folded incrementally —
`latest_event_time(frame, event_time_column)` per batch, combined with a
None-tolerant `max` — never by retaining frames. `run()` returns a `RunOutcome`,
never `None`: a frozen tagged union `Executed` (carrying `records_fetched` and the
`WriteResult`) or `CaughtUp` (the window resolved to nothing — no fetch, no writer,
no ledger row). The high-level surface dispatches on it.

**Date-partition staging is crash-cleaned at writer construction.**
`compact_partition` folds *every* `.shard` in a date's staging directory (§3), so a
run that fails after `stage_shard` but before `finalize` leaves shards a later run
would fold in — re-injecting a superseded row's old version (one that exact dedup
will not collapse against the corrected version), which defeats the watermark cell's
replace semantics. The `WatermarkPartitionedWriter` therefore clears the staging
directories for its window's covered dates at construction, before staging anything:
this both sweeps a prior crash's shards and guarantees a clean start, since the run
that compacts a date always covers it and so always clears it first. The `.shard`
extension keeps a half-staged partition out of any hive `*.parquet` read in the
interim.

**Build order (historical, now complete for snapshot and watermark).** The
`ProviderClientRegistry`, `EndpointRunner`, request drivers, `RunOutcome`, staging
crash-clean, watermark arm, `FanOutRequestDriver`, and roster coordinator have all
shipped for the snapshot and watermark paths. Feed execution is the remaining
runner arm.

## 15. Next Steps

1. Review/amend this document
2. Build in dependency order: `network/limits/` (done) → auth session manager (done, `network/auth/`) → request contract (done, `network/contract/`: `RequestSpec`, `AuthStrategy` + implementations, `ResponseCategory`/`ClassifiedResponse`/`ResponseClassifier`; `ProviderProfile` deliberately deferred to the client prompt — the bundle rule triggers at three traveling parameters and only two exist) → exception hierarchy (done, `exceptions.py`) → retry policy (done, `config/retry.py` + `network/retry/`) → page-decoder abstraction (done, `network/contract/page_decoder.py` + `decoders/`) → HTTP config + the real GeoTab authenticator (done, `config/http.py` + `network/auth/authenticate.py`) → `network/client/` (done) → `endpoints/shared/base.py` (done) → `records` (done) → `storage` (done: `snapshot`+`single` plus the date-partitioned/watermark writer, §3) → `state` (done in full — §5) → `orchestrator` (built in full: the run executor's snapshot arm and plan-and-drive watermark arm, the request drivers, the fan-out machinery, the unit loop, and the roster refresh — §7/§13/§14). The chain's original endpoint, `cli.py`, is superseded by the build roadmap below — the public API (§10) precedes any YAML/CLI surface.

The `network/client/` step inherits a recorded agenda: classify
prepare-time transport exceptions (the authenticator propagates
`httpx.TransportError` raw and loop-free by design — whether a transport
failure during auth/prepare is retried is the client's call), wire the
exception-hierarchy raise sites (FATAL → `ProviderResponseError`, exhausted
budgets → `RetriesExhaustedError`, auth paths → `AuthenticationError`), and
bundle the two per-provider dependencies that share a session lifetime
(auth strategy, classifier) into `ProviderProfile`, leaving the per-endpoint
page decoder and quota scope to arrive on each `fetch_pages` call.

**Vertical progress.** The Motive `vehicles` snapshot vertical was proven
complete end-to-end in June 2026 (`client → validate_records →
models_to_dataframe → persist`, exercised by a throwaway hand-run driver).
That driver has since been re-pointed through the public `fetch` surface
(roadmap item 5) and no longer persists — the persist-ending trace is the
June proof, not the script's current shape. The Motive `vehicle_locations`
date-partitioned/watermark vertical has run end-to-end live
(`scripts/run_vehicle_locations.py`, on local disk): the incremental loop
mechanics are live-proven — cold backfill, watermark persistence and resume
(through the canonical-UTC serialization path), wholesale date-partition
replacement idempotency, compaction dedup over more than one million combined rows, and fan-out over a
script-harvested roster. The same run
surfaced a window-granularity defect in the resume machinery (roadmap item 1
below); the defect was internal to fleetpull — the provider was exonerated by
direct probing — and has since been fixed by flooring the resume window at
resolution (item 1, done).

### The build roadmap (settled order)

1. **Window-granularity fix (done — the floored resume window, §4).** Root
   cause: the effective resume window start was computed at datetime
   granularity (the persisted watermark, e.g. `23:59:59`, minus the lookback
   margin) while requests and partitions are day-granular. The day-granular
   request floored to `start_date` and fetched the full boundary day; the
   datetime-granular post-fetch filter then kept only a seconds-wide sliver
   of it; wholesale date-partition replacement would then overwrite the
   boundary day's complete partition with that sliver on every steady-state
   daily sync — silent, cumulative data destruction. The live run
   materialized a severely truncated boundary partition via exactly this
   mechanism. The fix, delivered:
   `resolve_resume_start` floors the chosen start to the UTC midnight of its
   date, so the window `in_window` filters against equals the requested date
   range and the filter's residual job is guarding provider overshoot.
   Watermark semantics unchanged (max `located_at` of kept records). Both
   riders shipped: the writer-side tripwire (every staged partition date must
   lie in `window_dates(window)` — the require-inside half of the
   normalize-at-boundary doctrine, §12), and the scope note that the rule
   holds for snapshot/point-event endpoints only: duration/span endpoints
   (e.g. HOS periods crossing midnight) need their own boundary policy when
   they arrive and must not blindly reuse it (recorded on
   `resolve_resume_start` and in §4).
2. **Fan-out composition root (done — the orchestration entry, §14).**
   `run_endpoint` (`orchestrator/entry.py`) resolves a definition's declared
   `fan_out` through the roster machinery — registry → coordinator refresh →
   store members → `FanOutRequestDriver` — or hands a `fan_out=None`
   definition the single-fetch driver; the caller never sees the distinction
   (the orchestrator-boundary principle, §14). The `vehicle_ids` roster is
   declared beside its feeder (`endpoints/motive/vehicles.py`,
   `VEHICLE_IDS_ROSTER`), vehicle_locations declares its binding, and the
   diagnostic script composes the entry instead of hand-harvesting. The
   settled constraint holds at the feeder population: `/v1/vehicles` lists
   inactive and retired vehicles, so the roster covers vehicles active during
   historical windows even if inactive today. The vehicle_locations module
   docstring now describes the real wiring. A live-run follow-up closed the
   roster-freshness defect this composition surfaced: the staleness signal
   (`last_success_at`) and the refresh events were decoupled — a coordinator
   harvest was invisible to the ledger, and a runner-driven feeder run never
   touched the roster — so now every feeder execution records a run and
   reconciles its rosters (§3's Rules 1 and 2: the harvest records a run
   directly, and the entry's feeder tap reconciles from the run's own
   batches).
3. **Public API design (done — settled per §10).** The design conversation
   concluded: `fetch` as the snapshot-only in-memory convenience verb over a
   typed `Endpoints` catalog, `sync` as the config-driven verb whose full
   vocabulary is item 6's schema work, and the fluent/method-chaining
   pattern retired.
4. **Pre-API audit, anchored to that design (done — `AUDIT.md`,
   2026-07-06).** The audit swept the composition path the API will sit on,
   produced the wiring inventory, the state-free fetch trace (clean — the
   item-5 build map), and sixteen verdicted findings; audit fix wave 1
   cleared everything pre-item-5 (the SUCCESS-path parse escape, the
   rate-limit config migration and runtime defaults, the roster feeder-mode
   guards, the empty-member filter, the `JsonObject` relocation to
   `vocabulary/`, the `ResponseModel` bound, the carrier-contract rename,
   and the script comment drifts). The item-6-owned findings (roster
   discovery, the state DB path key, `runs.row_count` semantics, the
   rate-limit YAML key) were closed by the config/Sync vertical and recorded in
   the implementation docs/tests.
5. **Build the fetch side of the public API (§10), after the audit (done —
   `fleetpull/api/`, 2026-07-07):** the `Endpoints` catalog (a static
   committed module plus the two-way parity discipline test against the
   discovery registry), the typed endpoint identities, `fetch` itself, and
   the auth ingress coercion. The snapshot script re-pointed through the
   verb is the audit's consumer-cost evidence, closed.
6. **Config-YAML framework and `Sync(config_path).run()` (done); CLI wrapper
   deferred.** The YAML is a serialization of the API surface, so it followed
   the public API design. The schema, loader, endpoint selection validation, and
   programmatic `Sync(config_path).run()` shell are built and tested (§10). A
   separate command-line wrapper over the same verbs remains deferred; it must
   serialize the existing `fetch`/`Sync` concepts rather than invent a third
   surface.
7. **GeoTab verticals.** GeoTab is the architectural stress test (different
   auth, pagination, decode). The `devices` snapshot vertical is built
   end-to-end — `GeotabConfig` and the two method-class scopes, the ingress
   `ProviderProfileContext` seam, the seek-paging Get decoder, the `GetCountOf`
   completeness guard (probe-settled decisions 1 and 2), the union-of-shapes
   `Device` model, `Endpoints.Geotab.devices`, and both public verbs. The
   `trips` vertical is also built as a watermark/date-partitioned endpoint over
   windowed `Get` (`TripSearch` date bounds plus seek paging) with
   `event_time_column='start'`. The `log_records` feed vertical is built over
   `GetFeed`/`LogRecord`, using the global cold-start anchor and persisted feed
   tokens. The abstractions held — no driver, runner, or entry change carried a
   provider branch.
8. **Polish phase, gated on a stable public surface:** full-tree ceremony
   audit, test-coverage audit, documentation audit, the real usage-driven
   README, multi-platform CI (a Windows leg would have caught the
   missing-`tzdata` failure automatically), and the parked staging
   robustness (§13).

**The `work_units` orchestration — built in full (no longer deferred).** The
unified plan-and-drive loop is the only windowed path: the store, the chunk
planner (`plan_backfill_units`), and the claim-and-drive loop
(`orchestrator/unit_loop.py` composed by the runner's watermark arm) plan
every windowed run as units and drive them serially ascending with per-unit
commits (§13's settled transaction-boundary record). The whole-window
watermark arm and the never-wired no-advance per-chunk arm are deleted — the
single-unit degenerate case is the daily run. (The old
deferred-inventory line here read "per-provider executor and per-endpoint
writer threads" — a conflation: the per-provider executor shipped with the
concurrency vertical (§7) as fan-out machinery, orthogonal to backfill, and
per-endpoint writer threads were never part of the settled design; the
single writer per endpoint stands, §3.)

**Deliberately deferred — off the roadmap's critical path.**
`metadata.json` generation (cosmetic, projected from SQLite, never read by
the program), a separate CLI wrapper, single-file feed storage, calculated-feed
tombstone handling, and schema overrides for unsupported model shapes.


## 2026-07-14 LogRecord feed update

The GeoTab `log_records` endpoint is the first built `GetFeed` vertical. Cold feed starts use non-persisted `FeedBootstrap(sync.default_start_datetime)`, subsequent starts use persisted opaque `FeedToken`, and each API page is its own durable storage transaction: date-partitioned parquet finalization, then feed-token cursor commit, then the run ledger. Feed output stages temporary `.shard` files under the endpoint `.staging/` root and compacts to exactly one production `part.parquet` per touched date with append-plus-exact-dedup semantics. Feed single-file storage and calculated-feed tombstone handling remain deferred. The public non-snapshot identity is now `IncrementalEndpoint`. Pre-release SQLite state installs the consolidated head schema version 3; development databases at versions 1 or 2 must be recreated.
