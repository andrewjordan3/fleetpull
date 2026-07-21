# fleetpull — Design Document

**Status:** Design settled through the two-verb public API (§10) and config-driven sync. Shipped end-to-end: the `fetch` API; `Sync(config_path).run()`; the yaml-run CLI (`fleetpull sync <config>`) and the per-endpoint `metadata.json` projection (both 2026-07-17); work-unit planning with crash resume and the per-provider fan-out executor; Motive `vehicles` (snapshot), `vehicle_locations` (date-partitioned watermark, live-run), and the fleet-wide event pair `driving_periods` / `idle_events` (windowed watermark); GeoTab `devices` (snapshot, live-run), `users` (snapshot), `trips` (windowed watermark, stop-anchored), and `exception_events` (windowed watermark, bisected). The Samsara foundation is wired (2026-07-17): `SamsaraConfig` with the `SAMSARA_API_KEY` fallback, the bearer auth-ingress arm, and full `Sync` dispatch; Samsara `vehicles` (snapshot, cursor walk) is the first Samsara vertical, built from the same-day probe session. The GeoTab feed arm (`GetFeed` runner, storage cells, token commit) remains unbuilt — trips ships windowed until it lands (§8). See §15 for run status and the build roadmap.
**Name:** `fleetpull` — final. Describes exactly what the package does and nothing more (PyPI availability confirmed 2026-06-10).
**Relationship to fleet-telemetry-hub:** New package, not a rewrite. fleet-telemetry-hub remains in production untouched while fleetpull is built.

---

## 1. Purpose and Scope

fleetpull retrieves fleet telematics data from provider APIs and delivers it as
typed, dtype-coerced, lightly normalized tabular output that stays as close to
the raw API responses as is reasonable.

**In scope**

- Fetching from provider APIs (Motive, GeoTab, Samsara; extensible to others)
- Broad endpoint coverage per provider — as many endpoints as practical, built
  out over time into a large default library. "No assumed end use" includes
  not assuming which endpoints are useful: an endpoint is never excluded
  because no known consumer wants it, only deferred until its vertical is
  built (or excluded for a documented practicality reason, e.g. a shape the
  schema pipeline cannot yet honestly type). fleet-telemetry-hub seeds the
  port order; it is a bootstrap aid, not the scope ceiling — fleetpull
  implements more endpoints than its predecessor ever did. (Scope principle
  settled 2026-07-17.)
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
| IANA tz database | **tzdata** declared as a runtime dependency | Windows and slim Linux images ship no system IANA database, and stdlib `zoneinfo` then requires the `tzdata` package — without it the first tz-aware Polars extraction fails with `ZoneInfoNotFoundError`. Surfaced by the live Windows run; declared explicitly rather than trusted to the platform. |

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

`metadata.json` is a **generated human-readable snapshot** (shipped
2026-07-17): after each successful endpoint run, the runner projects the run's
committed facts — its counts and resolved window, plus a cursor read-back from
the store — into a `MetadataSnapshot` that `storage/metadata.py` renders and
atomically writes as `<endpoint>/metadata.json`. It is never read by the
program; SQLite remains the single source of truth (see §5) — no dual-write
divergence. The write is post-commit and best-effort: an `OSError` logs at
ERROR and the committed run stands — a stale file the next successful run
rewrites beats failing a committed run over a cosmetic projection.

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
Storage is stateless — parquet only, no SQLite, no watermark commit (the
orchestrator sequences those after a successful `finalize`, §5); the
`metadata.json` render/write primitive lives in `storage/metadata.py`, but the
facts it projects are the orchestrator's. The single-file family (`SingleFileWriter` → `SnapshotWriter`) and the
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

Build obligation for the date-window / `single` cell: it shares one file across
work units, so it must serialize its units or reject
`sync.backfill_unit_workers > 1` — the §5 prefix-advance record's parallel-unit
legality holds only for the date-partitioned cell's disjoint whole-day
partitions.

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

**`persist` grows a keyword-only `window`.** The signature becomes
`persist(definition, new_frame, dataset_root, *, window: DateWindow | None = None)`.
The window is computed per-run by the driver (the resume resolver, §4) and passed
in — never read off the definition — and `persist` validates the pairing against the
sync mode: a `WatermarkMode` endpoint with `window is None` is a wiring bug and
raises, and a `SnapshotMode` endpoint handed a non-`None` window also raises.

**How the fleet's rows for one date are assembled across the per-vehicle fan-out** —
`vehicle_locations` fetches per vehicle (~1,459 `GET .../{vehicle_id}` calls) but a
single `date=` partition holds the whole fleet's rows for that date — is settled. A
backfill decomposes into per-chunk work units (`partition_key=None`, §5); a chunk
fans the whole roster at execution, so the partition is replaced with every member's
rows; and the write half (`storage/staging.py`) stages each fetched piece to disk on
arrival (`stage_shard`), folds each date's shards into its `part.parquet` at finalize
(`compact_partition`), and clears the staging afterward. Peak memory is bounded by
the chunk, not the endpoint — a high-volume endpoint stays in bounds via a smaller
chunk, not by streaming. Per-vehicle multi-part files (`part-{uuid}.parquet`, no
coalesce) were rejected for the small-files problem partitioning exists to prevent:
at this fleet's ~1,459 vehicles × ~7 window-days that is ≈ 10k tiny files per
refresh, compounding every refresh, and tens of thousands of few-KB files degrade
BigQuery external tables and `scan_parquet` badly — so each date folds to one
`part.parquet`.

The fan-out key source is settled: a provider-listed roster in SQLite, not the
feeder parquet. An endpoint that fans out over a roster declares the
`RosterFanOut` request shape (`EndpointDefinition.request_shape`; the default
`SingleFetch` fetches once) naming a `RosterKey`; the
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
(verified by live probes; live-reconfirmed per-type 2026-07-20 — a 60-second
window strictly inside a trip's span returned it, start- and end-anchoring
falsified). Appended as-is, those pre-`start` trips are never
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
`WorkUnitStore` with its claim queue. The orchestrator that sequences these
against fetch and storage (§14) is likewise built; only the feed arm remains.

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
running code is refused. Today head is v3: v1 is the cursors, runs, and
work_units tables, v2 adds the rosters table, and v3 adds
`work_units.observed_max` (the prefix-advance watermark rule's datum — the
dated record below).

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
lives above it (see resume precedence below). Two writes, two stances:
`set_cursor` is the unconditional single-row upsert with no advance
discipline — the general write the feed arm will use when it lands (no
production caller today) — and `advance_watermark_forward` (added 2026-07-20
with the prefix-advance rule below) is the watermark arm's write, carrying the
strictly-forward monotonicity guard and the cursor-kind guard *inside* its
statement. That in-statement guard is the recorded exception to the
dumb-store stance: the prefix rule's concurrent per-completion commits cannot
enforce monotonicity race-free from outside the statement (a read-compare-write
in the caller can interleave a stale read into a backward write).

**Watermark semantics: observed-data-only and monotonic.** A `DateWatermark` is
the maximum event timestamp actually seen; it is set only from observed data and
only ever moves forward — since 2026-07-20 enforced by
`advance_watermark_forward`'s in-statement strictly-forward guard (the
watermark arm's only write path). An empty fetch — or one returning nothing
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
claim, so crashes count too) caps retries at `max_attempts`. The cap is store
mechanism, not shipped policy (updated 2026-07-20): the orchestrator passes a
deliberately never-binding cap (`2**31 - 1`, predating the prefix rule), because
a finite cap would convert a persistent failure into a silently skipped unit — a
coverage hole behind an advancing watermark — where an always-claimable poison
unit instead fails the endpoint loudly on every invocation, the fail-loud
posture. The never-binding cap is additionally load-bearing for the
prefix-advance rule's gap-unreachability argument (invariant 2 in the record
below: a drained claim loop implies every existing unit is done); the
`max_attempts` machinery is retained for a future bounded retry policy, which
must re-derive that argument before binding the cap. Crash recovery is a startup reset: a
single `fleetpull` invocation runs the whole backfill — many endpoints, each
optionally fanned across many partition keys — as one process, so at startup any
`claimed` row is stale (its worker is gone) and reverts to `pending`; no lease or
heartbeat, sound because the only constraint is not running two invocations
against one state database at once (concurrent invocations are out of scope).

**The prefix-advance watermark rule — parallel unit execution (decided
2026-07-20).** A windowed endpoint's work units now drive
`sync.backfill_unit_workers` at a time (default 4; `1` is the serial path,
inline with no pool). Activation rationale: the per-unit transactions were
already independent by construction — each unit owns its run-ledger row, its
disjoint whole-day partitions, and its atomic claim — so the serial constraint
existed *solely* to protect the per-unit watermark advance (completed units had
to stay a contiguous prefix for a per-unit `set_cursor` to be truthful).
Replacing that advance removed the constraint's only reason. The default is
active (4), not opt-in: the provider's rate budget still governs at the
transport boundary (§7), so workers beyond a provider's budget simply pace on
rate-limit tokens — parallel units spend nothing the endpoint could not
already spend, they only stop leaving the budget idle between units. The replacement,
piece by piece:

- **`work_units.observed_max` (migration v3).** `mark_done(unit_id,
  observed_max=...)` records the unit's folded in-window maximum event time
  (`to_iso8601` form; NULL for an empty unit or a pre-v3 completion) — the
  per-unit datum the rule reads.
- **`done_prefix_observation(provider, endpoint)`.** One SQL read returning
  `MAX(observed_max)` over the *contiguous done-prefix*: every `done` unit whose
  `chunk_start` precedes the earliest not-done unit's (all done units when none
  remains). Done units beyond a pending/claimed/failed gap contribute nothing
  until the gap closes.
- **`CursorStore.advance_watermark_forward(provider, endpoint, observed)`.**
  The guarded upsert whose monotonicity and cursor-kind guards live *inside*
  the statement (the recorded exception to the dumb-store stance above):
  inserts when absent, advances when strictly forward, changes nothing
  otherwise, and surfaces a stored feed cursor as `ConfigurationError`.
  In-SQL because the commits race — a caller-side read-compare-write can
  interleave a stale prefix read into a backward write; the statement cannot.
- **The commit choreography.** After every unit completion the unit loop
  invokes the runner's prefix commit (`done_prefix_observation` →
  `advance_watermark_forward`); `_run_watermark` also invokes it **once at run
  start** — the crash-heal for a crash landing between a unit's done-mark and
  its prefix commit (the read is cheap; an up-to-date cursor makes it a no-op).
  Completions may land in any order and every persisted watermark is true at
  every instant: everything at or before it has been fetched and committed.

**The four load-bearing invariants (the prefix read's gap-blindness).** The
prefix SQL sees only *rows*: a hole no row represents — a never-enqueued window
between done units — would not gate the prefix, and the far observation would
be returned. Such a hole is unreachable today because, and only because:

1. **One enqueue site, running only after the claim loop drains.**
   `_run_watermark` drives leftover claimables to drain, *then* resolves and
   enqueues the residual — so the claimable set is always one contiguous
   tiling, and no second enqueue site can interleave a plan mid-drive.
2. **The never-binding attempt cap.** A drained claim loop implies every
   existing unit is done — no capped-out unit can be silently left not-done
   behind an advancing prefix (the fail-loud record above).
3. **`work_units` rows are never deleted** (the §13 provenance doctrine) — a
   pruned done row can never fake contiguity across what it used to gate.
4. **Planner windows tile without holes from the resume arms.** The residual
   window starts at the resume precedence (watermark less lookback, floored;
   else frontier; else anchor), which never leaps past coverage — consecutive
   plans leave no un-enqueued day between units.

Plainly: **any future change that violates one of these — row pruning, a second
enqueue site, a real (binding) attempt cap — must re-derive the prefix rule's
safety before shipping.** The state-layer gap-blindness test
(`tests/state/test_work_units.py`, the never-enqueued-hole case) is the
tripwire: it pins the query's gap-blind behavior so any "fix" or new reliance
is a conscious decision.

**Failure semantics and the accepted retry residual.** A failing unit stops
further claiming (in-flight siblings finish and commit — each is an
independent transaction), is logged with its unit id, is marked `failed`
(claimable again next invocation), and the first failure re-raises after all
workers join. The stop signal deliberately lands *before* the failed-mark,
which narrows but does **not** close a same-invocation retry window: a sibling
already past its stop check when the mark lands can claim the just-failed unit
once more — bounded at one extra claim per already-running sibling, each
logged and each incrementing `attempt_count`. Accepted as recorded.

**Scope: `DATE_PARTITIONED` watermark cells only.** Parallel units are legal
under the single-writer invariant on two legs, both load-bearing: every
shipped watermark cell is date-partitioned with disjoint whole-day unit
windows (midnight-aligned, contiguous tiling — no two units share a
partition), and the concurrently claimable set is one contiguous plan
(enqueue-after-drain, invariant 1). A future (`SINGLE`, `WatermarkMode`) cell
shares one file across units, so it **must serialize its units or reject
`backfill_unit_workers > 1`** — recorded here as that cell's build obligation
(§3's mechanism matrix, the unbuilt date-window/`single` row).

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

Non-goals of this cut, revised: provider-parallel activation was recorded
here as deferred ("awaits a second ported provider" — the executors were
per-provider by construction, which was all the shaping this cut did); with
three providers shipped it activated 2026-07-20 at the queue-per-provider
grain — the record below. The non-goal still standing: page-fanning within
one member's chain awaits envelope verification and will be recorded with
its own design when it lands.

**Provider-parallel `Sync` (activated 2026-07-20).** `Sync.run` carves the
selection into one queue per enabled provider — feeders then consumers,
config order within each, so queue order equals the retired serial order
(feeders never cross providers) — and runs each queue on its own worker in
a context-managed `ThreadPoolExecutor` sized to the enabled-provider count;
each worker renames its thread to the provider so stack traces attribute
cleanly, and a single enabled provider degenerates to one worker with the
serial loop's exact behavior (amended 2026-07-20: endpoints within a queue
now run staged-concurrent — the intra-provider record below).
Activation is unconditional — no config knob:
the queues were already independent by construction (per-provider fetch
pools and clients, per-scope limiters, provider-scoped rosters,
`(provider, endpoint)`-scoped claim recovery, connection-per-operation
SQLite under WAL with a busy timeout), so a knob would be configurability
nobody asked for. Failure semantics: within a queue, unchanged — a
`FleetpullError` records an `EndpointFailure` and the queue continues, and
any other exception is a bug that stops that queue's remaining endpoints;
across queues, independent — one provider's failures never touch another's
queue. After all workers join, the first bug by provider order re-raises;
otherwise failures raise `SyncFailuresError` in the two-level order — run
order within each provider, provider config order across providers (§10's
"in run order" contract, updated; the intra-provider record below restates
the within-provider half as queue order). The `sync finished` elapsed time
stays the run's wall clock — now the slowest queue, not the queues' sum.

Accepted residual (recorded at activation): a KeyboardInterrupt landing while the main thread joins the queue workers abandons the remaining joins, so the client/pool context managers can close while a worker still fetches -- noisy worker tracebacks and bounded exit latency, never corruption (the per-unit parquet -> ledger -> done-mark -> prefix-commit ordering with unit-gating keeps state sound, exactly the crash story §5/§14 tell). The same window existed under the serial loop; signal hardening is polish-phase work (§15 item 8).

**Intra-provider endpoint concurrency (activated 2026-07-20).** The third
grain completes the concurrency ladder: providers run concurrently (the
record above), endpoints run concurrently *within* a provider — feeders
barriered ahead of consumers — and units / fan-out members run
concurrently within one endpoint (the per-provider pool above). Each
provider's queue worker carves its selection into two stages by
**feeder-hood** (`sourced_by` non-empty), never snapshot-hood — a snapshot
endpoint that feeds no roster (e.g. Samsara `drivers`) has no dependents
and rides with the consumers. Stage 1 runs the selected feeders
concurrently among themselves (they reconcile distinct roster keys, so
they cannot interfere; the set is usually 0 or 1), the stage join is the
barrier — no consumer fans out while a selected sibling feeder is still
reconciling the roster it is about to read — and stage 2 runs the
consumers concurrently. Each non-empty stage is a short-lived
`ThreadPoolExecutor(max_workers = stage size)`; an empty stage spawns
nothing, and a single-endpoint stage degenerates to the serial path's
exact semantics — the one visible difference is attribution: the
endpoint's log records carry the task thread's
`fleetpull-sync-<provider>-<endpoint>` name rather than the queue
worker's. The selected-set doctrine is untouched: an unselected
feeder is never enlisted on a consumer's behalf — roster freshness stays
the refresh coordinator's job at fan-out time.

- *The queue-order failure contract.* Failure reporting within a provider
  is **queue order** — feeders then consumers, config order within each:
  the same sequence the serial queue executed in, but now a contract about
  reporting, never execution (this replaces the earlier "run order within
  provider" phrasing). After each stage's pool joins, its futures drain in
  submission (queue) order: operational failures (`FleetpullError` —
  recorded, the queue continues) collect in queue order with no locks and
  no re-sort, and the first bug by queue order re-raises
  deterministically, never by completion timing. A bug sets the queue's
  stop event before escaping its future — in-flight siblings finish and
  commit; unstarted tasks skip without running (the unit loop's stop
  semantics) — and a stage-1 bug skips stage 2 entirely. Cross-provider,
  nothing changed: `SyncFailuresError` carries queue order within each
  provider, providers in config order.
- *Zero new config — deliberate.* No endpoint-workers knob:
  `rate_limit.max_concurrency` remains the one per-provider load dial. It
  sizes the fetch pool and the limiter's in-flight semaphore — the real
  throttles; endpoint threads are waiting containers, and a count knob
  would be masked by the limiter, so it would not earn its keep.
- *Fetch-pool sharing.* Concurrent fan-out endpoints share the provider's
  one fetch pool: a submission window is per-`stream_pieces`-call local
  state, so each concurrent stream keeps its own window over the shared
  executor. Execution parallelism stays capped by the pool's `max_workers`
  and in-flight requests by the limiter's semaphore — nothing is resized
  for the new grain.
- *The 429 note.* A penalty pauses the whole quota scope, so one
  endpoint's backoff stalls its concurrently running siblings — correct,
  not a defect: the scope is the provider's, and the siblings spend the
  same budget.
- *Single-flight roster refresh.* Concurrent consumers of one stale
  roster — the feeder-unselected case, e.g. Samsara `trips` +
  `idling_events` both cold-starting — would each harvest: wasted quota,
  duplicate ledger snapshot runs. `RosterRefreshCoordinator` therefore
  holds a per-`RosterKey` lock across `refresh_if_stale`'s whole body (an
  outer lock guards get-or-create of the per-key locks), so the second
  entrant re-runs the freshness check under the lock and returns early on
  the now-fresh roster. Holding the lock across the harvest network call
  is intended — the waiting consumer needs the membership before it can
  fan out. No lock nests inside (the store and ledger are
  connection-per-operation), so there is no deadlock surface; locks are
  per-key, so distinct rosters refresh concurrently. The feeder tap's
  `apply_listing` takes the same per-key lock (the harvest route, already
  holding it, reconciles through the shared body directly), so **every**
  reconcile for one key serializes whichever route wrote it — the staging
  fact that feeders and consumers never share a stage stays a freshness
  optimization, never a correctness dependency.
- *Duplicate-name validation.* `ProviderConfig.endpoints` rejects
  duplicated names at validation, naming them: a duplicated endpoint
  would now run twice — concurrently, against itself.

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

### GeoTab `exception_events` design decisions (2026-07-15)

Settled conversationally from the 2026-07-13 ExceptionEvent capture set;
the paging arm was selected by the 2026-07-15 discrimination probes (the
observed-behaviors rows below).

1. **`exception_events` ships as the second windowed GeoTab endpoint on the
   trips template** — windowed watermark, date-partitioned, the shared
   provider `lookback_days`/`cutoff_days` knobs. The captured
   mutation-after-creation envelope (observed-behaviors row below) sits
   well inside a one-day lookback. **Paging resolved (2026-07-15):**
   the discrimination probes found id-sort unsupported on this type
   outright (the observed-behaviors row), so the seek template is
   structurally unavailable and the bisection fallback (decision 3)
   ships. Version/date sortability, which the ArgumentException
   revealed, is banked as feed-design input, not a paging arm here.
2. **Version one ships the unfiltered stream — no rule-filter config
   knob.** Three reasons, in force order: the captured silent-empty hazard
   means a typo'd rule id reads as a permanently empty dataset, never an
   error; validating a filter without an extra API call reduces to a
   closed proven-set (one rule today), which serves nobody; and rule
   selection on the delivered stream is a one-expression consumer-side
   operation, which is where selection belongs (§1's no-presumption
   stance). If server-side filtering later shows real need, the knob
   arrives with proven-set config validation — fail-fast at config load,
   no referent-validation API call.
3. **The bisection fallback, designed and banked.** If sort is unusable
   for this type: fetch the unit's window at the endpoint's
   `resultsLimit`; a page of exactly the requested limit is the overflow
   signal (a return-type condition) — discard it, halve the window,
   recurse left-to-right; a minimum-width window still returning a full
   page raises `ProviderResponseError`. Placement: a
   `BisectingWindowDriver` beside the single-fetch driver at the
   request-driver seam (§14) — fetch grain decouples from write grain, so
   work units and the delete-by-window merge stay whole-day and
   storage/state are untouched. Rejected placements: a bisecting page
   decoder (decoders are stateless; the recursion's pending-halves stack
   has nowhere to live) and work-unit splitting (breaks the
   FIFO-by-unit-id watermark-truth invariant and the midnight-alignment
   guard).
4. **Bisection is cap-gated per entity type.** The silent `Get` cap is
   Captured on Device only and appears in no GeoTab documentation. Seek
   paging never depends on the cap — it terminates on the empty page, so
   the requested limit is merely a page-size preference — but bisection's
   overflow signal is sound only where the endpoint honors requests up to
   the asked size: a lower silent cap would make every page look partial
   and overflow undetectable. Bisection therefore ships for a type only
   after that type's cap behavior is captured (done for ExceptionEvent —
   the observed-behaviors row below).
   Corollary: `resultsLimit` values stay per-leaf constants carrying
   per-type provenance comments — two leaves declaring 5,000 are two
   per-type facts that currently coincide, not one fact stated twice —
   and the value is flagged in-code as a strong candidate for a user
   config knob.
5. **The event-time column is resolved (2026-07-15): `active_from`.**
   The window-matching pair found OVERLAP matching (the
   observed-behaviors row) — retrieval supersets start-anchored
   ownership, so every record whose `activeFrom` falls in a chunk's UTC
   window is guaranteed returned by that chunk's fetch, the existing
   post-fetch window filter assigns single ownership, and no wire-window
   pad is needed. Edge records fetched by a neighboring chunk are
   filtered out there and kept by their owner — the Samsara-settled
   normalization pattern, at zero new machinery.

### Motive `driving_periods` / `idle_events` probe-settled decisions (2026-07-15)

Settled by the 2026-07-15 live probe session (the captured rows above); the
port build prompt implements them.

1. **Both port as windowed watermark, date-partitioned, fleet-wide** (no
   fan-out), offset-paginated through the existing Motive wrapped-list
   decoder family; `event_time_column='start_time'` on both — start was
   never observed null (in-progress records null their *end* fields), and
   for `driving_periods` the retrieval anchor and the routing anchor
   coincide natively.
2. **`idle_events` pads its wire window one day on each side and the true
   UTC window does the trimming.** The leaf's spec builder writes
   `start_date − 1` / `end_date + 1` onto the wire; the resume window, the
   post-fetch window filter, and the writer-side partition tripwire all
   keep the true UTC window, so every record lands in exactly one chunk
   (the §4 start-anchored normalization rule, generalized
   timezone-agnostic — a one-day pad covers any account timezone on
   earth). Rejected: deriving the account zone from `/v1/companies`
   `time_zone` — the field-to-window linkage is unverified (Motive's
   rollup-timezone documentation covers returned timestamps on rollup
   endpoints, not window interpretation on raw-event endpoints), the
   Rails-style display name needs a maintained mapping plus DST handling,
   and overlap-matching makes the client-side filter mandatory anyway, so
   precision buys nothing over the pad.
3. **Backfill chunking bounds at a ≤ 30-day date delta on both endpoints.**
   `driving_periods` enforces it loudly; applying the same bound to
   `idle_events` keeps wide-window latency (observed 12–18 s) inside the
   configured HTTP read timeout (`HttpConfig.read_timeout_seconds`,
   default 30 s — global, not per-endpoint; a user running wide backfill
   chunks raises it in config).
4. **The rollup endpoints stay unported** (`vehicle_utilization`,
   `driver_utilization`): superseded in the legacy package, and their
   documented company-local rollup timestamps make them a modeling hazard
   with no consumer. *Amended 2026-07-17: reframed from excluded to
   deferred under the endpoint-breadth scope principle (§1) — "no
   consumer" is no longer a scope argument. The company-local timestamp
   semantics become a documentation obligation on the mirror (verbatim
   timestamps, the timezone caveat in the model docstring) plus a
   window-matching probe question, not an exclusion. Same reframe for
   the legacy `groups` and `users` endpoints. All queue behind the
   Samsara wave (§15 item 7).*

### Samsara `drivers` probe-settled decisions (2026-07-20)

Settled by the 2026-07-20 live probe session (the captured rows below); the
drivers build implements them.

1. **One dataset: the complete listing is active ∪ deactivated, and the
   `driverActivationStatus` column carries the split.** The default listing
   is only the active set (45% of the population invisible to it), so
   completeness requires both sweeps. Rejected: splitting into two
   endpoints (`drivers_active` / `drivers_deactivated`) — the partition is
   a provider filter quirk over one entity, not two entities; two datasets
   would push the union onto every consumer and double the state surface
   for no semantic gain.
2. **The sweep is DECLARED, not implemented: `request_shape=ParamSweep`,
   resolved to the existing fan-out driver at the shared seam.** The
   member-agnostic `FanOutRequestDriver` fans one cursor chain per declared
   value with the sweep's param as the member key — no new decoder, no new
   driver. Rejected: a sweeping *decoder* (a Samsara decoder that would
   iterate statuses inside the page walk) — request cardinality belongs to
   the driver seam (§14's unification), the decoder sees exactly one chain
   by contract, and a per-provider sweeping decoder would not generalize
   to the next provider's partitioned listing, where the fan-out driver
   already does.
3. **Model = union-of-observed with the Device/User list-block exclusion.**
   The `tags` list (441/460) and `eldSettings.rulesets` (190/460) are
   list-of-object blocks the §9 pipeline does not represent — excluded and
   recorded in the model docstring; `externalIds` was never observed in
   832 records and is unmodeled as *unobserved*, not excluded. Only `id`
   is required: per-key presence was fully enumerated on the active sweep
   only (the deactivated sweep matched structurally), so the
   always-present-in-capture keys stay optional.
4. **No completeness check.** Continuation is explicit per page (the
   cursor contract, proven per-type) and the sweep vocabulary is
   API-enforced — every malformed `driverActivationStatus` value returns a
   loud HTTP 400, never a silent empty listing — so there is nothing a
   provider-side count would guard that the walk and the enum closure do
   not already make loud.

### Samsara `trips` probe-settled decisions (2026-07-20)

Settled by the 2026-07-20 live probe session (the captured rows below); the
trips build implements them.

1. **The legacy v1 surface is the only surface, and it is structurally
   per-vehicle — the roster machinery's first cross-provider consumer.**
   `GET /v1/fleet/trips` is the one live path (the modern candidates 404:
   `/fleet/trips`, `/beta/fleet/trips`, `/preview/fleet/trips`) and
   `vehicleId` is REQUIRED (a loud HTTP 400 when omitted), so the binding
   ships windowed watermark / `DATE_PARTITIONED` / SAMSARA scope /
   `event_time_column='start_time'` with `request_shape=RosterFanOut(
   roster=RosterKey(SAMSARA, 'vehicle_ids'), member_key='vehicleId')` — the
   Motive vehicle_locations template, crossed to a second provider with no
   machinery change.
2. **A new `VEHICLE_IDS_ROSTER` declared beside its feeder**
   (`endpoints/samsara/vehicles.py`, the Motive precedent verbatim:
   `source_endpoint='vehicles'`, `source_column='id'` — the vehicles
   frame's flattened top-level id — 1-day max age, eviction threshold 3;
   discovered by the registry walk, no registration). On inactive
   coverage, honestly: the 2026-07-17 capture proves the feeder lists
   unplugged units, so present-but-inactive vehicles stay fanned over;
   whether Samsara ever delists a removed vehicle was not probed, and the
   eviction hysteresis is what retires a member the listing stops
   returning.
3. **The leaf builder merges the fan-out member as a QUERY parameter**
   (the drivers-leaf precedent, not the Motive path-template one) plus the
   resume window as `startMs`/`endMs` epoch milliseconds —
   `int(timestamp * 1000)` of the tz-aware bound, `require_utc`-guarded at
   the serialization point. The half-open `[start, end)` to inclusive-wire
   mismatch at the millisecond boundary is absorbed by
   overlap-supersets-ownership plus the runner's post-fetch start filter
   (documented on the builder). The 90-day cap needs no builder guard:
   default 7-day backfill chunks sit far inside, and a
   `backfill_chunk_days` raised past 90 earns the provider's own loud 400
   — the Motive driving_periods stance.
4. **Model = the full-census mirror with epoch-ms type recovery.**
   `start_time`/`end_time` are tz-aware UTC datetimes RECOVERED from the
   wire's epoch-millisecond ints by a mode='before' validator — type
   recovery is structural and belongs on the mirror (the GeotabTimeSpan
   precedent); aliases `startMs`/`endMs`, the field names dropping the
   unit suffix because the recovered type carries it (naming ownership).
   Everything else mirrors verbatim under unit-suffixed names; `driverId`
   0 is the UNASSIGNED sentinel, mirrored untouched;
   `assetIds`/`codriverIds` (observed ONLY EMPTY across all 725) are typed
   `list[int]` — the int-id family in the `list[scalar]` form the §9
   derivation represents (it has no tuple form). Address blocks and the
   geocoded strings are the PII-adjacent fields — capture fixtures are
   fully synthetic.
5. **No completeness check** — windowed, deliberately partial (the
   standing snapshot-only rule).

### Samsara `idling_events` probe-settled decisions (2026-07-20)

Settled by the 2026-07-20 live probe session (the captured rows below); the
idling_events build implements them.

1. **The Motive driving_periods template on the Samsara cursor decoder —
   the first windowed+cursor pairing, composed entirely from existing
   parts.** Windowed watermark / `DATE_PARTITIONED` / SAMSARA scope /
   `event_time_column='start_time'`, with the fleet-wide default
   `SingleFetch` shape (events carry per-record asset attribution —
   `asset: {id: int}` on every record — so there is no fan-out and the
   binding declares nothing). Retrieval is START-anchored on UTC (the
   captured row), so the retrieval anchor and the routing anchor coincide
   natively: no wire pad, and the runner's post-fetch window filter is
   pure hygiene (the driving_periods situation).
2. **The leaf spec builder renders the resume window as RFC3339
   `startTime`/`endTime`** via `require_date_window` and the timing
   codec's `to_iso8601`; the decoder owns `limit`/`after` — its
   `first_request` merges `limit=200` (the probed per-endpoint cap, a
   per-leaf `Final` with provenance), and because the `after` advance
   merges onto the SENT spec, the window parameters persist across every
   page of the walk (the mechanism proven live on the drivers sweep, now
   carrying a window instead of a status). The sub-3-months range cap
   needs no builder guard: default 7-day backfill chunks sit far inside,
   and a `backfill_chunk_days` raised past the cap earns the provider's
   own loud JSON 400 — the Motive driving_periods stance.
3. **Duration stays `duration_milliseconds: int`** — a verbatim
   unit-suffixed mirror. No timedelta recovery: the value is directly
   consumable, and recovery would presume a use (the wire has NO end key
   — the interval is start plus duration).
4. **No completeness check** (windowed, deliberately partial — the
   standing snapshot-only rule); **no enum for `ptoState`** — only
   `'inactive'` was observed in 2,200 records, but the value set is not
   closed by evidence (unlike `driverActivationStatus`'s 400-proven
   closure), so the field is a plain `str` with the openness documented
   on the model.

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
| `SyncFailuresError` | One or more endpoints failed inside a sync run whose siblings continued; inspect `failures` (per-endpoint, queue order within each provider — feeders then consumers, config order within each — providers in config order) and act per member. |

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
| GeoTab | `GetCountOf` returns the true entity count: 5,666 captured against the capped 5,000 — 666 records invisible to a bare `Get` (captured 2026-07-09). |
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
| GeoTab | `ExceptionEventSearch` composes cleanly with `ruleSearch` + `fromDate`/`toDate` when `sort` is absent — the identical body that crashes with sort succeeds without it, a one-member delta; this is the request shape window bisection needs, so bisection's feasibility is Captured while seek paging's remains open (captured 2026-07-13). |
| GeoTab | ExceptionEvent records mutate after creation (`lastModifiedDateTime` observed ~17 min past `createdDateTime`; creation observed up to ~1 h after the interval begins — the basis for the one-day lookback), and `duration = activeTo − activeFrom` holds exactly on every captured record, including a fractional-second span reproducing a fractional `activeFrom` (captured 2026-07-13). |
| GeoTab | **`TripSearch` matches trips by their STOP time — prediction-confirmed.** The original discriminating capture (2026-07-06) falsified start-, overlap-, and containment-matching; the confirmation pair sealed stop-matching: widening the window's end to 13:30 pulled in the trip stopping 13:27:56 exactly as predicted, and moving the start to 13:03 dropped exactly the trip stopping 13:02:20. The trips binding anchors `event_time_column='stop'`; retrieval and routing coincide, and the watermark advances on stop — record-materialization order (captured 2026-07-15). |
| GeoTab | ExceptionEvent does not support id-sort at all: `sort.sortBy: "id"` with no search returns `ArgumentException` — "Can not sort by id. Supported sortable fields are version, date." Composed with any search (rule or dates-only) the same request degrades to the `-32000 GenericException`, reproduced deterministically on exact retry. The trips seek template is structurally unavailable for this type; version/date sortability is a feed-design datum, unprobed for seek composition (captured 2026-07-15). |
| GeoTab | `ExceptionEventSearch` window matching is OVERLAP-anchored: a window catching only an event's activeTo returned it, and a window catching only an event's activeFrom returned it too — both discriminating bodies agree; activeFrom-, activeTo-, and containment-matching are all falsified. Third distinct window-matching rule observed across providers (Trip: stop; Motive driving_periods: start; this: overlap) — never assume the rule, per type, ever. Overlap retrieval supersets start-anchored ownership, so `active_from` routing needs no fetch pad (captured 2026-07-15). |
| GeoTab | The silent 5,000 `Get` cap is confirmed on ExceptionEvent: `GetCountOf` returned 304,716 against a bare `Get` returning exactly 5,000 — the bisection overflow signal's per-type precondition, now Captured for this type (captured 2026-07-15). |
| GeoTab | The User visibility anomaly was scope, not nonexistence: after the account fix, `GetCountOf User` returned 157 (previously 2), `isDriver: true` search returns driver records, and a by-id fetch of a trip-referenced driver succeeds. The driver-variant User shape is captured (60+ fields; carries list-of-nested-object fields like `mapViews`, excluded from the shipped model per the Device exclusion precedent) (captured 2026-07-15). |
| GeoTab | User supports id-sort seek paging — proven live, never assumed from Device: a `resultsLimit: 3` first page returned ascending ids, and the offset advance continued past the boundary with no overlap or loss. Third per-type sortability datum (Device yes, ExceptionEvent no, User yes) — sortability is probed per type, every time (captured 2026-07-16). |
| GeoTab | The full User population sweep (bare `Get`): 157 records, equal to `GetCountOf` exactly; no null value and no type variance on any of the 75 observed keys — GeoTab omits keys rather than sending nulls, so User optionality is absence-shaped. The driver-only block (`licenseNumber`, `licenseProvince`, `viewDriversOwnDataOnly`, plus the `driverGroups`/`keys` lists) sits at exactly the 129-driver count; `maxPCDistancePerDay` (126/157) does NOT align with the driver split; `accessGroupFilter` appears on exactly one record; `iAMMetadata` on 42/157 (captured 2026-07-16). |
| Samsara | 429 with fractional `Retry-After` (e.g. `0.40235`); 401 body is `{"message": ...}`; 5xx bodies are plain strings, never JSON. |
| Samsara | Success responses carry NO rate-limit headers (`Date`/`Content-Type`/`Content-Length`/`Connection`/`Request-Id`/`Strict-Transport-Security` only) — the Motive posture: the real budget is unobservable outside a 429, so the config's self-limiting default carries the load. `Request-Id` is the support correlation handle (captured 2026-07-17). |
| Samsara | `/fleet/vehicles` cursor mechanics proven live: the `after` advance continued across a real page boundary with no overlap or loss (ids ascend numerically straight across it), a fresh `endCursor` per page, and the TERMINAL page carries `hasNextPage: false` beside an EMPTY-STRING `endCursor` — not absent, not null — the shape the decoder's promised-continuation guard is calibrated against. The documented 512 `limit` maximum was honored exactly (608-vehicle fleet: 512 + 96) (captured 2026-07-17). |
| Samsara | Vehicle-record optionality is absence-shaped: no null value and no type variance across all 608 swept records (20-key union) — Samsara omits keys, like GeoTab, while also using empty strings (`notes: ""` everywhere). Partial-presence keys omit blockwise (the minimal 7-key shape is an unplugged unit with a serial-shaped default name). `externalIds` is an OPEN user-definable map with dotted namespace keys (`samsara.serial`, `samsara.vin`), each mirroring its top-level sibling; `year` is a quoted integer (captured 2026-07-17). |
| Samsara | `/fleet/drivers` carries the vehicles envelope (`data` list + `pagination {endCursor, hasNextPage}`) and honors `limit` 512; the terminal page is `hasNextPage: false` beside an EMPTY-STRING `endCursor`. Cursor mechanics proven per-type: a `limit=5` walk of 92 pages returned 460/460 unique ascending ids with no boundary overlap or loss (captured 2026-07-20). |
| Samsara | The default `/fleet/drivers` listing IS the active set exactly — 460 records, identical ids to `driverActivationStatus=active`. The deactivated sweep returns 372 fully disjoint records INVISIBLE to the default listing: 45% of the 832-driver population. The complete listing is the union of both sweeps (captured 2026-07-20). |
| Samsara | `driverActivationStatus` is a STRICT CLOSED ENUM (`'active'` \| `'deactivated'`): case variants, comma-joined values, repeated keys, and bogus values ALL return HTTP 400 `{"message": "Invalid value for driverActivationStatus. Can only be 'active' or 'deactivated'", "requestId": ...}` — loud, never silent-empty, so a typo'd sweep value can never read as an empty partition (captured 2026-07-20). |
| Samsara | `after` composes with the status param: a `limit=50` deactivated walk ran 8 pages (50×7+22), 372/372 unique, every record deactivated, a fresh cursor per page, the standard terminal — which is why the existing `SamsaraCursorPageDecoder` needed NO change for the sweep: its advance merges `after` onto the SENT spec, so a first-request query parameter persists across the whole walk (captured 2026-07-20). |
| Samsara | `/fleet/drivers` success responses carry no rate-limit headers either — the provider-wide posture confirmed on a second endpoint; the request id is the correlation handle (captured 2026-07-20). |
| Samsara | Driver-record census (union over 832; presence counts per the 460 active; deactivated matches structurally): absence-shaped optionality with ZERO nulls anywhere. Always present: `id`, `name`, `username`, `driverActivationStatus`, `timezone` (IANA), `createdAtTime`/`updatedAtTime` (ISO-8601 `Z` millis), `hasVehicleUnpinningEnabled`, `carrierSettings` (`carrierName`, `dotNumber` a BARE INT, `mainOfficeAddress`, plus `homeTerminalName`/`homeTerminalAddress` empty-string on 204/268 of 460), `hosSetting {heavyHaulExemptionToggleEnabled}`. Partial: `staticAssignedVehicle {id, name}` 102/460; `peerGroupTag` 4 and `vehicleGroupTag` 8, each `{id, name, parentTagId}`; `licenseNumber` 172, `licenseState` 269, `phone` 7, `locale` 1, `notes` 1, `profileImageUrl` 1; booleans `eldExempt` 270, `eldExemptReason` 282 (a free-text reason string, despite sitting in the flag family — the model and capture mirror it as `str`), `eldAdverseWeatherExemptionEnabled` 191, `eldBigDayExemptionEnabled` 186, `eldPcEnabled` 77, `eldYmEnabled` 100, `waitingTimeDutyStatusEnabled` 8. List-of-object blocks `tags` (441/460) and `eldSettings.rulesets` (190/460) are model-excluded per the Device/User precedent; `externalIds` was NEVER observed — unmodeled as unobserved (captured 2026-07-20). |
| Samsara | The trips surface is the LEGACY v1 API only: `GET /v1/fleet/trips`. The modern candidates 404 (`/fleet/trips`, `/beta/fleet/trips`, `/preview/fleet/trips`). `vehicleId` is REQUIRED — omitting it returns a loud HTTP 400 `rpc error: code = InvalidArgument desc = Missing parameter: vehicleId` in a text/plain body. Window params `startMs`/`endMs` are epoch MILLISECONDS (captured 2026-07-20). |
| Samsara | The `/v1/fleet/trips` envelope is `{"trips": [...]}` — no pagination of any kind; one response per (vehicle, window), so `SinglePageDecoder(records_key='trips')` fits (the exception_events pairing precedent) (captured 2026-07-20). |
| Samsara | `/v1/fleet/trips` retrieval is OVERLAP-anchored, re-verified per-type: a 60-second window strictly inside a trip's span returned that trip (start- and end-anchoring falsified) — §4's historical record for this exact endpoint confirmed live. Overlap retrieval supersets start-anchored ownership, so `start_time` routing needs no wire pad (captured 2026-07-20). |
| Samsara | The `/v1/fleet/trips` window range cap is LOUD and exactly 90 days: 91+ days returns HTTP 400 text/plain `rpc error: code = InvalidArgument desc = requested time range cannot exceed 90 days`; a 90-day window succeeded (702 trips, one page). NOTE the wire posture: v1 400 bodies are TEXT/PLAIN rpc-error strings — the known Samsara plain-string-body posture extends beyond 5xx (captured 2026-07-20). |
| Samsara | Trip-record census (725 trips, 60 vehicles, ZERO nulls anywhere): always present — `startMs`/`endMs` (epoch-ms ints; `endMs` on every observed trip including the two most recent — in-progress trips were never observed and appear to materialize on completion; the lookback absorbs late materialization, an accepted residual), `distanceMeters`, `fuelConsumedMl`, `tollMeters`, `startOdometer`/`endOdometer` (ints, provider units mirrored verbatim), `driverId` (int; 0 is the UNASSIGNED sentinel, 110/725), `startLocation`/`endLocation` (reverse-geocoded strings), `startCoordinates`/`endCoordinates` (`{latitude, longitude}` floats), `assetIds`/`codriverIds` (lists, observed ONLY EMPTY). Partial: `startAddress` 177/725 and `endAddress` 185/725, each `{address: str, id: int, name: str}` — present when the trip endpoint matched a defined address/geofence. The record does NOT echo the requested `vehicleId` (captured 2026-07-20). |
| Samsara | `/v1/fleet/trips` success responses carry no rate-limit headers either — the provider-wide posture confirmed on a third endpoint, and on the legacy v1 surface (captured 2026-07-20). |
| Samsara | `GET /idling/events` is a modern-envelope surface: `data` + `pagination {endCursor, hasNextPage}`, terminal page `hasNextPage: false` beside an EMPTY-STRING `endCursor`; the cursor walk proven live — 11 pages at limit=200, 2,200/2,200 unique. Window params `startTime`/`endTime` are RFC3339 UTC. Events are fleet-wide with per-record asset attribution (`asset: {id: int}` on every record — the vehicle reference), so no fan-out exists (captured 2026-07-20). |
| Samsara | **The `limit` maximum is PER-ENDPOINT: `/idling/events` caps at 200, NOT the 512 of vehicles/drivers** — limit=512 returns a loud JSON 400 `{"message": "limit must be lesser or equal than 200 but got value 512", "requestId": ...}`. The first captured instance of Samsara's per-endpoint limit tiers: never assume a sibling's limit (captured 2026-07-20). |
| Samsara | `/idling/events` retrieval is START-anchored on UTC, proven by a discriminating pair on a 6.5-hour event: a 60s window strictly inside its span does NOT return it; a 60s window straddling only its start DOES. Fourth distinct anchoring datum across providers (GeoTab Trip: stop; Motive driving_periods: start; GeoTab ExceptionEvent / Samsara v1 trips: overlap; this: start) — and notably NOT the company-local overlap behavior of Motive's `idle_events` sibling: never assume the rule, per endpoint, ever. Consequence: `event_time_column='start_time'`, retrieval anchor == routing anchor natively, no wire pad, the runner's window filter is pure hygiene (captured 2026-07-20). |
| Samsara | The `/idling/events` range cap is loud and sub-3-months: 91 days accepted; 180 days returns JSON 400 `{"message": "Total duration must be less than 3 months.", ...}`. Default 7-day chunks sit far inside; no builder guard (the Motive driving_periods stance) (captured 2026-07-20). |
| Samsara | Idling-event census (2,200 events, ZERO real nulls — absence-shaped): always present — `eventUuid` (str, the event id), `startTime` (RFC3339 str), `durationMilliseconds` (int; there is NO end key — the interval is start+duration; events were only ever observed complete, with implied ends in the past even in a last-30-minutes probe: in-progress idles appear to materialize on completion, the lookback absorbs late materialization, accepted residual), `asset {id: int}`, `latitude`/`longitude` (floats), `ptoState` (str; only `'inactive'` observed — the value set is NOT evidence-closed, so modeled plain str, not an enum), `fuelConsumedMilliliters` (MIXED int\|float on the wire — modeled float), `fuelCost {amount: str, currency: str}` (money mirrored verbatim as strings), `gaseousFuelConsumedGrams` (int), `gaseousFuelCost {amount, currency}`. Partial: `operator {id: int}` 1546/2200 (driver attribution when known); `airTemperatureMillicelsius` (int) 1833/2200; `address {id: str, addressTypes: list[str]}` 552/2200 (element `'yard'` observed; `addressTypes` itself absent on ~31/552 blocks). NOTE `address.id` is a STRING while `asset.id`/`operator.id` are BARE INTs — mirrored exactly (captured 2026-07-20). |
| Motive | 401 body is `{"error_message": ...}`; the documented /vehicle_locations limit was not observed to enforce — generic 429 posture. |
| Motive | `/v3/vehicle_locations/{vehicle_id}` verified live: envelope `{"vehicle_locations": [{"vehicle_location": {...}}]}`, `located_at` is UTC ISO-8601 (`Z`-suffixed), one non-paginated page per fetch (so `SinglePageDecoder` fits), and a single per-vehicle fetch spans multiple calendar dates (the sample crossed two) — confirming `split_by_date`'s multi-partition output is load-bearing in production, not a theoretical edge: one fetch genuinely fans into several partitions. |
| Motive | `/v3/vehicle_locations/{vehicle_id}` date bounds pinned by direct probing: day-granular `start_date`/`end_date` are honored inclusively on both bounds — a single-day request returns that full day, a two-day request both complete days. The documented 3-month maximum range is real: long backfills will eventually need request chunking (a range limit, unrelated to the §15 item-1 window defect). |
| Motive | `updated_after` on `/v3/vehicle_locations/{vehicle_id}`: documented as required, observably optional and inert — omitting it and supplying it produced byte-identical responses. It remains a candidate ingestion-time CDC hook for the late-upload gap (§13). |
| Motive | The collection endpoint `/v3/vehicle_locations` (no vehicle id) is a different animal: a last-known-location roster snapshot that ignores date parameters and serves active vehicles only (~1,029 vehicles vs the full harvest's 1,460). It is not a history source and must not be conflated with the per-vehicle history endpoint. |
| Motive | `/v1/driving_periods` window matching is START-anchored on UTC days: across a 10,366-record two-day window, zero returned periods start before the window's UTC midnight while ends freely spill past the window's right edge (62 right-straddlers observed; end-anchoring and overlap both falsified). Retrieval anchor = `start_time` (captured 2026-07-15). |
| Motive | `/v1/idle_events` window matching is OVERLAP-anchored on **company-local** day boundaries, not UTC — `start_date`/`end_date` are interpreted at UTC−5, matching the company-local `time_zone` the account's `/v1/companies` capture carried (a zone at a UTC−5 offset — the linkage behind the rollup-timezone documentation obligation): a single-local-day probe's earliest end landed 05:14:58Z against the predicted ≥ 05:00:00Z boundary with prior-local-evening overlappers present, and two-day windows returned records lying entirely outside the window on UTC terms. Two sibling endpoints, same param names, different anchor AND different timezone semantics (prediction-confirmed, captured 2026-07-15). |
| Motive | The 30-day range cap is real and LOUD on `/v1/driving_periods` — HTTP 400, `{"error_message": "Date range cannot be greater than 30 days"}`, with a 30-day delta accepted exactly (the limit counts the date delta) — and is NOT enforced on `/v1/idle_events`, which honored a 35-day window to its final record. Never generalize a per-endpoint cap across siblings (captured 2026-07-15). |
| Motive | Event-record mechanics, both endpoints: completed `driving_periods` reproduce `duration = end_time − start_time` exactly (float seconds); the in-progress shape carries `status: "in_progress"`, null `end_time`/`end_kilometers`/`distance`, an EMPTY-STRING `destination` beside null destination coordinates, and a fractional running `duration` counter. `start_time` was never observed null. Sort orders differ per endpoint (driving: start desc; idle: end asc); a past-the-end page returns 200 with an empty list and intact pagination echo (`total` = records); success responses carry NO rate-limit headers (the real budget is unobservable outside a 429); wide windows run 12–18 s with one observed 30 s client timeout (captured 2026-07-15). |

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
wants windows, incremental resume, partitioned storage, or roster fan-out is
not a `fetch` user with missing parameters; they are a `sync` user.

**Snapshot-only `fetch`, and why.** `fetch` exposes snapshot-mode endpoints
only; windowed retrieval is config/sync territory. The in-memory contract is
only honest for snapshots: a snapshot result is bounded by entity count, while
a windowed result grows with window width and fleet activity — unbounded by
anything the caller controls in memory. The exposed subset is a *type*, not a
runtime allowlist: identity types encode sync mode, and `fetch`'s signature
accepts only snapshot-typed identities, so handing it a windowed identity
fails mypy — backed, as built, by a runtime exposure guard (the first
statement of `fetch` raises `ConfigurationError` naming the endpoint and its
mode, before any client construction), because the convenience verb's
audience includes notebooks where mypy never runs. Starting narrow is the
reversible choice — adding windowed fetch later would be an additive
extension, while shipping it now and retracting it would be a break. Within
the snapshot subset, `fetch` is shape-polymorphic with a stateless boundary
(2026-07-20): its driver comes from the same `resolve_request_driver` seam
sync uses (§14), called with no roster source, so every stateless
`RequestShape` — `SingleFetch`, `ParamSweep` — serves identically on both
verbs, while a `RosterFanOut` is refused with a loud `ConfigurationError`
(roster membership is durable state, and fetch's contract is no state).

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
provider, endpoint, the caught exception) in queue order within each
provider — feeders then consumers, config order within each — providers in
config order (§7's queue-order failure contract), and is deliberately
not an `ExceptionGroup` so the documented `except FleetpullError:` contract
keeps catching it. Every other exception type is internal and renameable.

**`sync` — the config-driven verb.** Constructed on a path to the YAML config
(`Path` or `str`); a `run()` method returning `None`; failure signaled by
raising. Endpoints inside one sync run and commit independently — a sibling's
failure never halts the others. The YAML schema *is* sync's API: designing
sync means designing the config schema, so sync's full vocabulary is
deliberately deferred to roadmap item 6, where the schema and the programmatic
shell (`Sync(path).run()`) are designed as one unit. `fetch` was separable and
designed first because its vocabulary does not depend on the schema.

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
accepts the config's `SecretStr` directly). Endpoints run as one staged
queue per enabled provider, the queues concurrent (§7's provider-parallel
and intra-provider records, both 2026-07-20): per provider, the selection
carves by feeder-hood — `sourced_by` non-empty, never a user-facing key —
into feeders and consumers, config order within each; stage 1 runs the
feeders concurrently among themselves, the stage join is the barrier, and
stage 2 runs the consumers concurrently. Endpoints commit independently: an
endpoint's `FleetpullError` is recorded while its queue continues, any
other exception is a bug that stops that queue's unstarted endpoints
(in-flight siblings finish and commit) while the other queues finish (the
first bug — queue order within a provider, provider order across — re-raises
after all queues join), and a run with failures ends by raising
`SyncFailuresError` with the failures in queue order within each provider,
providers in config order. Only the selected set runs — an unselected
feeder is never run on a consumer's behalf; roster freshness stays the
refresh coordinator's job at fan-out time, single-flight per roster key.

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

**What survives from the old section.** `iter_records(endpoint, **params)` —
typed iterator of Pydantic models, pagination transparent. (Renamed from
fleet-telemetry-hub's `fetch_all`, whose "all" misleadingly suggested all
endpoints rather than all pages.) This is the escape hatch for consumers who
don't want Polars; the dataframe path is built on top of it. The old CLI
framing is superseded: fleetpull makes no fetch-CLI commitment; the CLI story
is the yaml-run tool over `sync` (item 6).

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
                   #   (database_path); path fields normalize
                   #   through paths.resolve_path at validation
    providers.py   # the provider family: ProviderConfig (quota_scope, rate_limit,
                   #   endpoints, and the shared lookback_days/cutoff_days --
                   #   declared once, per-provider YAML override preserved),
                   #   MotiveConfig (api_key, base_url, records_per_page),
                   #   GeotabConfig (nested
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
                   #   provider config defaults its own
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
    posture/       # transport posture: the one HttpConfig -> httpx-options mapping
      client_options.py  # client_verify + client_timeout, consumed by every
                         #   httpx.Client construction site (the transport pool
                         #   and the GeoTab authenticator's per-call client) so
                         #   the mapping cannot drift between them
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
                   #   atom ({root}/{provider}/{endpoint}/), used by the parquet
                   #   writers and the metadata.json projection
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
                   #   request_shape, completeness_check) + the SpecBuilder and
                   #   CompletenessCheck Protocols, the SyncMode union (SnapshotMode /
                   #   WatermarkMode / FeedMode), ResumeValue, and StorageKind
      request_shape.py  # the RequestShape union — SingleFetch / RosterFanOut /
                   #   BisectedWindowFetch / ParamSweep: request cardinality as one
                   #   closed axis (§14's shape resolution matches over it)
      spec_builders.py  # StaticGetSpecBuilder — the shared snapshot spec-builder
      resume.py    # require_date_window — the shared windowed-resume guard
      url_paths.py  # render_url_path_template — strict {placeholder} URL-path rendering (fan-out)
    motive/
      _spec_builders.py  # MotiveFleetDateRangeSpecBuilder — the shared fleet-wide date-range builder
      vehicles.py  # build_endpoint — the Motive vehicles snapshot factory
      vehicle_locations.py  # MotiveVehicleLocationsSpecBuilder + build_endpoint — the watermark binding
      driving_periods.py  # build_endpoint — the fleet-wide driving-span watermark binding
      idle_events.py  # build_endpoint — the fleet-wide idle-interval watermark binding (padded wire window)
    samsara/
      vehicles.py  # build_endpoint — the vehicles cursor-walk snapshot factory,
                   #   plus VEHICLE_IDS_ROSTER (the trips fan-out's roster,
                   #   declared beside its feeder)
      drivers.py   # SamsaraDriversSpecBuilder + build_endpoint — the ParamSweep
                   #   snapshot factory (active ∪ deactivated, §8's 2026-07-20 block)
      trips.py     # SamsaraTripsSpecBuilder + build_endpoint — the per-vehicle
                   #   windowed fan-out over the v1-only surface (the roster
                   #   machinery's first cross-provider consumer, §8's trips block)
    geotab/
      _get_requests.py  # the shared GeoTab Get request machinery: GeotabGetSpecBuilder
                   #   (the snapshot seek walk), GeotabWindowedGetSpecBuilder (the
                   #   windowed builder with per-leaf id_sort), server_host, and
                   #   GetCountOfCheck (underscore-prefixed so the registry walk
                   #   skips it)
      devices.py   # build_endpoint — the devices seek-paged snapshot factory
      users.py     # build_endpoint — the users seek-paged snapshot factory
      trips.py     # build_endpoint — the trips windowed (watermark) factory; the
                   #   TripSearch date bounds ride the seek walk (§4's amendment)
      exception_events.py  # build_endpoint — the bisected windowed factory; composes
                   #   the windowed builder with id_sort=False (id-sort rejected
                   #   per-type, §8)
    registry.py  # EndpointRegistry + build_endpoint_registry — the (provider, name) catalog; discovers leaves by walking endpoints.<provider>
  polars_typing/   # quarantined re-export boundary for Polars type aliases with no public
                   #   equivalent (e.g. ParquetCompression) — the sole importer of polars._typing
    __init__.py    # re-exports ParquetCompression
  model_contract/  # pure dependency-free leaf: the response-model config policy
    response.py    # ResponseModel config-policy base (frozen, extra=ignore, populate_by_name, strip)
    coercions.py   # empty_str_to_none — the type-recovery before-validator (composed
                   #   per field where "" cannot validate as the declared type, e.g.
                   #   an int; string fields mirror "" — the DataFrame boundary nulls it)
  roster/          # the fan-out roster leaf: identity, declaration, catalog (imports only vocabulary/exceptions)
    key.py         # RosterKey: the opaque (provider, name) handle a consumer references
    definition.py  # RosterDefinition: a key's source endpoint + column + refresh policy
    registry.py    # RosterRegistry: RosterKey -> RosterDefinition (forward lookup)
  models/          # pure API mirrors per provider (Motive/Samsara ported from fleet-telemetry-hub)
    motive/        # the Motive model package — a directory per provider (§11 prose below)
      shared.py    # DriverSummary, EldDeviceInfo — embedded shapes shared across endpoints
      vehicles.py  # Vehicle snapshot record (+ AvailabilityDetails / AvailabilityStatus / VehicleStatus)
      vehicle_locations.py # VehicleLocation breadcrumb record (/v3/vehicle_locations)
      driving_periods.py  # DrivingPeriod span record (/v1/driving_periods)
      idle_events.py  # IdleEvent interval record (/v1/idle_events)
    samsara/
      vehicle.py   # Vehicle (+ the gateway/driver refs and the open-map
                   #   externalIds slice) — the fleet-vehicle snapshot record
      driver.py    # Driver + the carrier/HOS/tag-ref nesteds — the two-sweep
                   #   population record (list-of-object blocks excluded per the
                   #   Device precedent)
      trip.py      # Trip + the coordinates/address nesteds — the per-vehicle
                   #   trip record (epoch-ms wire ints recovered to UTC
                   #   datetimes; the 0 driver sentinel mirrored verbatim)
    geotab/
      shared.py    # GeotabTimeSpan (.NET TimeSpan ingress) + bare_id_to_reference
                   #   (the sentinel-or-object reference coercion) — shared across entities
      device.py    # Device — the union-of-shapes snapshot record (GO7/GO9/trailer,
                   #   everything optional; year-one and non-derivable fields excluded)
      trip.py      # Trip — the movement-interval record (Duration columns, the
                   #   driver sentinel flattening, the seconds-despite-the-name
                   #   engine_hours trap)
      exception_event.py  # ExceptionEvent — the rule-violation interval record
                   #   (nested refs, GeotabTimeSpan duration)
      user.py      # User + UserAccessGroupFilterRef — the account/driver record
                   #   (scalar mirror; list and IAM blocks excluded per the Device
                   #   precedent)
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
    metadata.py    # MetadataSnapshot + render/write — the metadata.json projection (§3)
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
    bisection.py   # BisectingWindowDriver — capped, unsortable Gets fetched whole via adaptive window halving (§14)
    shape_resolution.py  # resolve_request_driver — the RequestShape -> RequestDriver
                   #   seam both composition roots call (§14); RosterFanOut members
                   #   are fed in by the caller
    runner.py      # EndpointRunner — one endpoint's run transaction: the snapshot arm,
                   #   the plan-and-drive watermark arm, and the post-success
                   #   metadata projection (§14, §3)
    batch.py       # process_batch: per-batch validate/frame/window + fold (§14)
    streaming.py   # stream_processed_batches: a driver's pages, validated and framed per batch (§14)
    roster_harvest.py # harvest_roster_members: a feeder's complete membership as roster members (drives streaming, no write)
    roster_refresh.py # RosterRefreshCoordinator: refresh a roster when stale (staleness -> harvest -> reconcile -> apply); refresh only, not fan-out
    resume.py      # resolve_watermark_start — stored-cursor interpretation + its guards (§14)
    backfill.py    # plan_backfill_units: whole-UTC-day chunk -> WorkUnitSpecs (§5)
    unit_loop.py   # the concurrent, prefix-committing claim-and-drive loop over
                   #   work units (§13's settled transaction boundary; §5's
                   #   prefix-advance rule)
    entry.py       # run_endpoint — the orchestration entry: roster-fed driver resolution, roster refresh, feeder tap (§14)
    executors.py   # FetchPoolRegistry — per-provider executors, context-managed (§7)
    fanout.py      # stream_pieces — the bounded-channel threaded fan-out (§7)
  api/             # the public-surface tier: the two verbs and their machinery (§10)
    fetch.py       # fetch — the snapshot-only in-memory convenience verb
    sync.py        # Sync — the config-driven pipeline verb (validate, compose, execute)
    catalog.py     # Endpoints — the typed public identity catalog + available_endpoints
    identity.py    # EndpointIdentity / SnapshotEndpoint / WindowedEndpoint
    auth_ingress.py  # build_provider_profile — the one public auth= shape, coerced at the boundary
  cli.py           # the yaml-run CLI: fleetpull sync <config>
```

The package root holds user-facing modules only; internal code lives in
subpackages. Settled: ALL Pydantic models parsing user-provided YAML
centralize in `config/` — including `RateLimitConfig`, migrated there from
`network/limits/` ahead of the YAML loader (audit fix wave 1):
provider defaults live on the provider configs (`MotiveConfig.rate_limit`),
and `rate_limits_from_configs` derives the limiter registry's per-scope map,
so no composition root invents rate-limit numbers. Placement for everything else is settled the same
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
- `state` and `endpoints` are same-tier siblings and never import each other — this is why the run ledger's `RunMode` shadows the endpoints layer's `SyncMode` instead of importing it (§5, `state/run_ledger.py`).

**Import-linter coverage.** The rules above are mechanically enforced, not
just documented. `pyproject.toml`'s `[tool.importlinter]` carries a
package-wide vertical `layers` contract over every top-level module under
`fleetpull/`:

```
cli
api
orchestrator
storage
endpoints | records | state
models | network
logger
config | roster
exceptions
vocabulary | incremental | timing | model_contract | polars_typing | paths
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
`FeedMode`); the storage kind; the `request_shape` (below); and — settled with
`vehicle_locations` — the
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

**Request cardinality is one closed axis: the `RequestShape` union (unified
2026-07-20).** How one endpoint run decomposes into request chains is a single
concept and a single closed choice, exactly like `SyncMode` — so it is one
tagged union on one field (`request_shape`, default `SingleFetch()` so
single-chain leaves declare nothing), not a field per pattern. The members:
`SingleFetch` (one chain), `RosterFanOut` (one chain per roster member —
names a `RosterKey` and the `member_key` each member binds under; the source
endpoint and column live in the roster registry, never on the shape),
`BisectedWindowFetch` (the unit window fetched whole, halved adaptively on
the capped-response overflow signal — the declaration carries the provider
facts: `results_limit`, `floor`, `event_time_wire_key`), and `ParamSweep`
(one chain per declared query-param value, for providers that partition the
population behind a mandatory closed-enum filter with no all-values request;
the union of the sweeps is the endpoint's one complete dataset). Mutual
exclusion between patterns is structural — one field holds one member — and
the semantic sync-mode pairings (bisected requires watermark/partitioned; a
completeness check requires snapshot + `SingleFetch`; `ParamSweep` requires
snapshot until a windowed sweep is probed) are validated at construction.
The closed-extension contract: a future cardinality pattern is a new union
member plus its arm in the §14 shape resolution — `EndpointDefinition`'s
field set never changes for one again.

**The spec-builder is the only genuine per-endpoint behavior.** A `SpecBuilder`
is a Protocol with one method, `build_spec(resume, member_values) -> RequestSpec`,
where `resume` is a `ResumeValue` (`DateWindow | FeedToken | None`, §4) and
`member_values` carries the per-chain member binding the request shape supplies
— empty for a single chain, one `{member_key: member}` entry per fan-out or
sweep chain, with the spec builder owning its interpretation (a URL-path
placeholder value for a per-vehicle locations endpoint, a query-parameter value
for a sweep). It builds only the first request — URL, base params, and the
resume injection; the page decoder produces every request after it.

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
stored cursor into a resume value (the resume resolver, §4) and a `member_values`,
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
- `logging.getLogger(__name__)` in modules that log (INFO milestones/progress, DEBUG developer detail, WARNING degraded-but-continuing, ERROR failures — settled 2026-07-17); no `print` in production code
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
  runner). The `RosterFanOut.allow_empty_roster` escape is deliberately not
  built — it joins the binding when an endpoint genuinely needs it, not before.
  *Transaction boundary — settled and implemented:* neither of the two
  candidates, but the third the `WorkUnitStore` was built for — per **unit**
  (a date chunk of the whole roster), not per run and not per member. Every
  windowed run plans its window into `sync.backfill_chunk_days`-wide units (a
  daily window degenerates to one unit) and drives them
  `sync.backfill_unit_workers` at a time (claims FIFO by `unit_id`, i.e.
  ascending window order; completions in any order — amended 2026-07-20);
  each unit is its own transaction — fetch the unit's window, finalize its
  partitions, record its ledger row, mark it done with its folded
  observation. The watermark advances per completion across the contiguous
  done-prefix through the store's atomic forward-only write — §5's
  prefix-advance record, which retired the earlier serial-ascending
  constraint together with the per-unit advance it existed to protect; the
  truth invariant (every persisted watermark true at every instant) now
  rests on the prefix rule, not on completion order. Resume precedence:
  incomplete units outrank the
  watermark — a run re-claims and drives them first (an in-progress unit
  found at run start is by definition orphaned, because fleetpull assumes a
  **single driver per state database**), then plans the residual, resolved
  exactly as the resume chain always has (watermark less lookback, floored;
  else frontier; else anchor; cutoff trailing edge), as new units at the
  current chunk size — persisted unit boundaries are honored on resume even
  when `backfill_chunk_days` changed. A failed unit stops further claiming
  (in-flight siblings finish and commit), returns to a claimable state with
  nothing committed, and fails the endpoint after the workers join; the
  completed units stand, and the next run re-claims what remains.
  Completed unit rows are kept, not pruned — the runs-ledger provenance
  doctrine, now also §5-invariant 3 of the prefix rule. One emergent
  consequence, deliberate: a re-invocation whose
  residual window exactly matches an already-done unit's bounds drives
  nothing (the idempotent enqueue collapses onto the kept `done` row), so a
  same-day identical-window re-run is a no-op; the late-arrival margin
  refetch still happens whenever the window shifts.

- **Logging policy (settled 2026-07-17 — pinned open during the concurrency
  vertical so no scoped task could preempt it with ad-hoc narration).** The
  three pinned decisions, all settled and implemented:
  - *Timestamps are UTC everywhere* — the `Z`-suffixed `asctime`
    (`logger/setup.py`). Everything the lines describe — windows,
    watermarks, partitions — is UTC; log time matches data time so incident
    correlation never crosses a timezone boundary. Local-with-offset was
    considered (the operator reading a console lives in local time) and
    rejected: mixed clocks invite off-by-a-timezone misreadings.
  - *Handlers configure the `fleetpull` package logger only*, with
    `propagate = False`. Third-party verbosity (httpx and kin) is a
    deliberate future opt-in, not an accident of root configuration.
  - *Narration is INFO milestones-and-progress.* The level semantics — INFO
    for milestones and progress updates, DEBUG for movement and detail
    geared toward a developer, WARNING for degraded-but-continuing, ERROR
    for failures; a logger bound only in modules that log (§12, CLAUDE.md)
    — carry this content, now built: sync start/end, endpoint
    start/complete, the watermark plan (window bounds and claimable units),
    per-unit completion (in completion order — nondeterministic across
    parallel unit workers since 2026-07-20; the unit id and window bounds on
    the line keep it attributable), and the fan-out heartbeat every 100
    members, with
    DEBUG carrying the per-member detail. Never per page or per record —
    that is flood, not progress. (The motivating incident: the last live
    pre-narration run was ~80 minutes for two log lines.)
  - *Every record carries its thread name* (added with provider-parallel
    `Sync`, 2026-07-20): queue threads are `fleetpull-sync-<provider>`,
    endpoint tasks under a queue worker are
    `fleetpull-sync-<provider>-<endpoint>` (added with the intra-provider
    grain, same date), and fetch workers `fleetpull-<provider>-fetch`, so
    state-layer DEBUG lines that carry only run/unit ids stay attributable
    at a glance when providers — and now sibling endpoints — interleave.

---

## 14. Orchestration: the run executor, the request driver, and the client registry

The layer that sequences fetch, records, storage (§3), and state (§5) into one
endpoint's run. The network, `records/`, `storage/`, and `state/` layers are all
built; this layer, which drives them, is built in full as well (only the
feed arm remains, §15).

**The orchestrator-boundary principle: higher-level orchestrators and tools are
polymorphic — provider-agnostic and endpoint-agnostic.** A caller invoking an
endpoint never knows, or branches on, the provider, whether the endpoint fans
out, its sync mode, its storage cell, or its record identity; every dispatch
keys off `EndpointDefinition` declarations. The `RequestShape` union (request
cardinality is a declared fact, never an identity branch), `select_writer`
(the declared storage/sync cell routes), and the runner's `sync_mode` match
all state this for their seams; `run_endpoint` (`orchestrator/entry.py`)
extends it to driver resolution — the caller boundary that resolves a
definition's declared driver through the shared shape seam
(`resolve_request_driver`, `orchestrator/shape_resolution.py`: one match over
the union, called by both composition roots — the entry with a roster member
source, `fetch` with none) and runs. To `run_endpoint`'s callers the
resolution stays hidden: exposing a resolve-driver step there would leak
exactly the shape distinctions the declarations hide; what the entry
contributes to the seam is the roster half only the stateful composition has.
The entry never reasons about roster freshness (the refresh
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
  (validate -> frame -> guard -> `writer.write`), then `finalize` once and
  complete the run once; on the watermark arm the cursor advance is the unit
  loop's per-completion prefix commit (§5), never a step inside the spine. It
  is cardinality-blind — it never knows,
  or branches on, how many requests a run makes.
- **`RequestDriver`** (`orchestrator/drivers.py`) owns request *cardinality* and
  yields the run's fetched pages (records and durable progress) as a stream of
  batches — the run executor reads the records to validate/frame/write and the
  durable progress to advance a feed cursor. `SingleRequestDriver` issues one
  request chain (`member_values={}`) and yields its pages a page at a time;
  `FanOutRequestDriver` issues one request chain per member
  (`member_values={member_key: member}`), yielding each member's pages — the
  member list the caller's, and the driver member-agnostic: a `RosterFanOut`
  fans the whole roster (one member per backfill unit's chain set, the whole
  roster per incremental run) and a `ParamSweep` fans its declared values
  with `member_key=param` — no separate sweep driver exists.
  `BisectingWindowDriver` (`orchestrator/bisection.py`, added
  2026-07-15) serves capped, unsortable Gets declaring `BisectedWindowFetch`:
  it fetches the unit window whole, halves on the exactly-full overflow
  signal down to the declared floor (then fails loudly), and filters each
  leaf's page to the records anchored in that leaf — one owner per record
  under overlap-matched retrieval, so write-time dedup stays hygiene. Every
  driver yields one page per batch; the runner consumes batches uniformly. A
  driver touches only the client (from the registry) and the
  endpoint's `SpecBuilder`, and yields whole fetched pages; it does no validation,
  framing, or writing. **`member_values` live only in the driver** — the runner never
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
`orchestrator/entry.py`) is the consume half: it reads the refreshed members
and feeds them to the shape seam, which builds the `FanOutRequestDriver` the
runner receives. `EndpointDefinition.request_shape` is matched in exactly one
place — `resolve_request_driver` — and `fetch` calls the same seam with no
roster source, so every stateless shape (`SingleFetch`, `ParamSweep`, and
structurally `BisectedWindowFetch`) serves on both verbs while a
`RosterFanOut` under `fetch` is refused with a loud `ConfigurationError`
naming the endpoint and the roster (fetch's in-memory, no-state contract).

The driver is the missing adapter between one endpoint run and one-or-many request
chains, and it matches grain the existing layers already have: a `SpecBuilder`
builds one first request from `member_values`, `TransportClient.fetch_pages` drives
one chain from one first spec, and a `DatasetWriter` accepts one-or-many frames and
finalizes once. This resolves the §13 question on how a date partition's rows
assemble across the per-vehicle fan-out: the driver yields per vehicle, the runner
writes per vehicle, and `stage_shard` lands each piece to disk immediately (§3), so
the fleet's rows for a date assemble across per-vehicle `write` calls bounded by one
chain's records — never a RAM buffer holding the fleet. Backfill chunk sizing (§13)
remains the one open piece.

**The run is constructed, not self-assembling.** The `EndpointRunner` is injected
with four collaborators — the `ProviderClientRegistry` (client source), the
`RunStateAccess` bundle (the `RunLedger` run recorder, the `CursorStore` cursor
access, and the `WorkUnitStore` unit queue — the three state-database surfaces
always travel together through the run's crash order, so they ride as one
collaborator per the bundle rule), the `Clock`, and the root `FleetpullConfig` —
the container its composition root already holds, read for exactly four values:
`sync.default_start_datetime` (the cold-start anchor), `storage.dataset_root`
(where the writers land), `sync.backfill_chunk_days` (the unit width newly planned
windows tile into), and `storage.drop_exact_duplicates` (the writers' exact-dedup
switch). Passing the root keeps the constructor at four flat parameters without
threading the values individually (pass-the-container over field-threading). The
`EndpointDefinition` and the driver are
`run()` arguments, not constructor fields, so one runner instance runs `vehicles`,
then `vehicle_locations`, each with the driver its caller built. The runner
constructs no clients and reads no credentials. One `Clock` instance is shared by the
runner, the `RunLedger`, the `CursorStore`, and the limiter inside the registry's
runtime — otherwise run timestamps, window resolution, and the future guards skew
apart.

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
— open the run, drive/write/finalize, complete — is the shared spine
`_execute_window`, in the crash order below; the unit's folded in-window
maximum rides its `Executed` outcome instead of advancing anything inline.
`_run_watermark` is the plan-and-drive loop (§13's settled record, amended
2026-07-20): it commits the watermark prefix once at run start (the §5
crash-heal), drives every claimable work unit through the spine
`backfill_unit_workers` at a time, and after each completion records the
unit's observation at `mark_done` and re-commits the prefix — the watermark
advances only across the contiguous done-prefix, through
`advance_watermark_forward`'s in-statement guard (§5's prefix-advance rule).
The `should_advance_watermark` helper and the serial-ascending constraint are
retired together: the per-unit advance the ordering made sound is gone, and
with it the reason unit order was not a free choice — claim order stays FIFO
ascending, completion order is free. A unit fans the whole roster, so each
partition is replaced with every member's rows, exactly the in-full refetch
the partitioned writer already assumes — the writer is unchanged.

**Crash-safety ordering — parquet, ledger, done-mark, prefix commit
(superseded 2026-07-20; cursor-before-completion retired).** §5 fixes
parquet-before-cursor. The executor's earlier second ordering — cursor before
run completion — existed for the frontier-without-lookback hazard: a succeeded
watermark run feeds `coverage_frontier` (resume arm 2, §4), arm 2 applies no
lookback, so a `complete_run` that landed before a then-lost cursor write
would make the next run resume from the frontier without lookback and skip
late arrivals inside the window just written. The per-unit order is now
**parquet finalize -> `complete_run` (the ledger, inside the spine) ->
`mark_done` (the observation) -> prefix commit (both in the unit loop)** —
and it is safe *without* cursor-before-completion because the protection
moved from write-ordering to **unit-gating plus the run-start heal**: a crash
after `complete_run` but before `mark_done` leaves the unit not-done, and
incomplete units persist and outrank the residual plan — `_run_watermark`
re-claims and drives every claimable unit *before* resolving the residual, so
the window is refetched whole (delete-by-window idempotent) and the prefix
commits before any frontier arm could matter; a crash between `mark_done` and
its prefix commit is healed by the run-start `commit_prefix` before any claim.
The hazard could only bite when a data-bearing window's coverage outran its
cursor with no unit left to gate it, and the unit now always remains (or its
observation is already recorded for the heal). A crash mid-spine leaves the
run merely `running` (diagnostic-only — the frontier filters `succeeded`).
Snapshots are unaffected: they hold no cursor and never reach the frontier.

**Two future-time guards, one rule applied where it can surface.** The
`CursorStore`'s only advance discipline is `advance_watermark_forward`'s
strictly-forward guard (§5, 2026-07-20); the future-time checks stay the
watermark arm's — both in the pure helpers it calls. *Guard A*, in
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
no ledger row). The orchestration entry (`run_endpoint`) dispatches on it; the
user-facing surface (`api/sync.py`) discards the return.

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

**Build order.** (1) `ProviderClientRegistry`; (2) `EndpointRunner` +
`SingleRequestDriver` + `RunOutcome`, exercising the snapshot path end-to-end on
`vehicles`; (3) the staging crash-clean; (4) the watermark arm — window resolution,
both guards, the incremental fold, the then-current parquet -> cursor -> ledger
ordering (since superseded by the 2026-07-20 crash-order record above), and a
watermark stub endpoint to exercise it without fan-out. `FanOutRequestDriver` and
the coordinator follow, wiring `vehicle_locations`'s roster fan-out shape.

**Request cardinality unified (decided 2026-07-20).** `EndpointDefinition`
had accreted one optional field per cardinality pattern (`fan_out:
FanOutBinding | None`, `window_bisection: WindowBisection | None`) with
pairwise mutual-exclusion validators, and driver resolution was split between
the entry's private resolver and a hard-coded `SingleRequestDriver` inside
`fetch` — both symptoms of a missing abstraction. The fix is the `SyncMode`
precedent applied to cardinality: one closed tagged union (`RequestShape`,
§11) on one field, structural mutual exclusion, the semantic pairings
consolidated into one construction validator, and one polymorphic resolution
seam (`resolve_request_driver`) both composition roots call. `ParamSweep`
joined as the first new member under the closed-extension contract (first
consumer: Samsara drivers, whose provider partitions the population behind a
mandatory activation-status filter). The contract going forward: a new
cardinality pattern is a new union member plus its resolution arm — never a
new definition field, never a second resolver.

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
That driver was re-pointed through the public `fetch` surface (roadmap
item 5), no longer persisted, and was retired with the scripts folder
2026-07-17 — the persist-ending trace is the June proof. The Motive `vehicle_locations`
date-partitioned/watermark vertical has run end-to-end live
(the since-retired diagnostic driver `scripts/run_vehicle_locations.py`, on
local disk): the incremental loop
mechanics are live-proven — cold backfill, watermark persistence and resume
(through the canonical-UTC serialization path), wholesale date-partition
replacement idempotency, compaction dedup (0 duplicates across ~1.03M
combined rows), and fan-out over a script-harvested roster. The same run
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
   materialized a 6-row `date=2026-06-30` sliver partition via exactly this
   mechanism (1,029,760 + 6 = 1,029,766, to the row). The fix, delivered:
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
   roster fan-out shape through the roster machinery — registry → coordinator
   refresh → store members → `FanOutRequestDriver` — or hands a single-chain
   definition the single-fetch driver; the caller never sees the distinction
   (the orchestrator-boundary principle, §14). The `vehicle_ids` roster is
   declared beside its feeder (`endpoints/motive/vehicles.py`,
   `VEHICLE_IDS_ROSTER`), vehicle_locations declares its shape, and the
   (since-retired) diagnostic script composed the entry instead of
   hand-harvesting. The
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
4. **Pre-API audit, anchored to that design (done, 2026-07-06).** The
   audit swept the composition path the API will sit on, produced the
   wiring inventory, the state-free fetch trace (clean — the item-5 build
   map), and sixteen verdicted findings; audit fix wave 1 cleared
   everything pre-item-5 (the SUCCESS-path parse escape, the rate-limit
   config migration and runtime defaults, the roster feeder-mode guards,
   the empty-member filter, the `JsonObject` relocation to `vocabulary/`,
   the `ResponseModel` bound, the carrier-contract rename, and the script
   comment drifts). The item-6-owned findings (roster discovery, the
   state DB path key, `runs.row_count` semantics, the rate-limit YAML
   key) closed with the Sync vertical. Both point-in-time audit reports
   (`AUDIT.md` and the GeoTab readiness audit) were retired 2026-07-17,
   every finding resolved and every settled fact migrated into §8's
   observed-behaviors table and decision blocks.
5. **Build the fetch side of the public API (§10), after the audit (done —
   `fleetpull/api/`, 2026-07-07):** the `Endpoints` catalog (a static
   committed module plus the two-way parity discipline test against the
   discovery registry), the typed endpoint identities, `fetch` itself, and
   the auth ingress coercion. The snapshot script re-pointed through the
   verb is the audit's consumer-cost evidence, closed.
6. **Config-YAML framework, then the YAML-run tool** — in that order, and
   after the API: the YAML is a serialization of the API surface, and built
   earlier it would serialize a guess. This item absorbs sync's programmatic
   surface: the YAML schema *is* sync's API, so designing the config schema
   and designing `Sync(path).run()` are one unit (§10). `fetch` was separable
   and designed first because its vocabulary does not depend on the schema.
   *The yaml-run tool shipped 2026-07-17: `fleetpull sync <config>`
   (`cli.py` over `Sync`, a console entry point in `pyproject.toml`),
   closing this item.*
7. **One GeoTab endpoint end-to-end before bulk porting.** GeoTab is the
   architectural stress test (different auth, pagination, decode); if it
   bends the abstractions, that must surface before more Motive endpoints are
   stamped from the existing mold. Once it proves the pattern crosses
   providers: bulk-port the remaining Motive and GeoTab models (low-risk; can
   run parallel to item 6). *First half shipped (2026-07-09): the `devices`
   snapshot vertical is built end-to-end — `GeotabConfig` and the two
   method-class scopes, the ingress `ProviderProfileContext` seam, the
   seek-paging Get decoder, the `GetCountOf` completeness guard (probe-settled
   decisions 1 and 2), the union-of-shapes `Device` model, `Endpoints.Geotab`,
   and both verbs; the since-retired diagnostic driver
   `scripts/run_geotab_devices.py` was the live proof. The abstractions held — no driver, runner, or entry change carried a
   provider branch. The `trips` windowed vertical followed (2026-07-13),
   proving the watermark arm crosses providers, and the `exception_events`
   design is settled (2026-07-15; the §8 decision block, probe-gated
   build). The Motive bulk-port began on the pattern's other side
   (2026-07-15): `driving_periods` and `idle_events` shipped as the
   fleet-wide event pair per their §8 decision block, on a shared
   fleet-date-range spec builder; the remaining legacy Motive endpoints
   (`groups`, `users`, the rollup pair) were deferred (originally recorded
   as "deliberately unported"; reframed 2026-07-17 under the
   endpoint-breadth scope principle, §1 — deferred, never excluded). GeoTab
   `exception_events` shipped 2026-07-15 per its §8 decision block — the
   first bisected endpoint (`BisectedWindowFetch` + `BisectingWindowDriver`,
   §14; the shape was named `WindowBisection` until the 2026-07-20
   cardinality unification). The GeoTab `users` snapshot followed (2026-07-16, id-sort and the
   full-population shape proven live per §8's rows): the second seek-walk
   consumer, so the walk's spec builder and `GetCountOfCheck` promoted out
   of the devices leaf into the shared `_seek_walk` module — unified
   2026-07-17 into `_get_requests`, which also carries the windowed
   builder. Second half
   (the feed vertical) remains. Under the endpoint-breadth scope
   principle (§1, 2026-07-17) this item's endgame widened: after the
   Samsara onboarding (foundation, then the legacy four — `vehicles`,
   `drivers`, `trips`, `idling/events` — then the remaining legacy six),
   the port queue continues across all three providers — the deferred
   Motive endpoints, the wider GeoTab entity surface, and
   beyond-legacy endpoints per provider — endpoint by endpoint, each on
   the proven probe-then-build vertical. Samsara `vehicles` shipped
   2026-07-17 (the third provider's first vertical, same-day
   probe-to-build: cursor walk proven live per §8's rows, the
   foundation and the vertical in one branch). Samsara `drivers`
   shipped 2026-07-20 (the second Samsara vertical and the first
   `ParamSweep` consumer: the two-sweep activation-status listing per
   its §8 decision block, riding the unchanged cursor decoder and the
   member-agnostic fan-out driver). Samsara `trips` shipped 2026-07-20
   (the third Samsara vertical and the roster machinery's first
   cross-provider consumer: the v1-only per-vehicle windowed fan-out
   per its §8 decision block, composed entirely from existing
   machinery). Samsara `idling_events` shipped 2026-07-20 (the fourth
   Samsara vertical and the first windowed+cursor pairing: the
   fleet-wide windowed leaf on the existing cursor decoder per its §8
   decision block, zero shared-machinery changes) — the Samsara legacy
   four are COMPLETE 2026-07-20 (`vehicles`, `drivers`, `trips`,
   `idling_events`); the remaining legacy six queue next.* The per-endpoint
   inventory and port queue are tracked in `ENDPOINTS.md` (added
   2026-07-17), updated in the same change as any endpoint addition.
8. **Polish phase, gated on a stable public surface:** full-tree ceremony
   audit, test-coverage audit, documentation audit, the real usage-driven
   README, multi-platform CI (a Windows leg would have caught the
   missing-`tzdata` failure automatically), and the parked staging
   robustness (§13).

**The `work_units` orchestration — built in full (no longer deferred).** The
unified plan-and-drive loop is the only windowed path: the store, the chunk
planner (`plan_backfill_units`), and the claim-and-drive loop
(`orchestrator/unit_loop.py` composed by the runner's watermark arm) plan
every windowed run as units and drive them `backfill_unit_workers` at a time
with per-unit commits under the §5 prefix-advance watermark rule (§13's
settled transaction-boundary record, amended 2026-07-20). The whole-window
watermark arm and the never-wired no-advance per-chunk arm are deleted — the
single-unit degenerate case is the daily run. (The old
deferred-inventory line here read "per-provider executor and per-endpoint
writer threads" — a conflation: the per-provider executor shipped with the
concurrency vertical (§7) as fan-out machinery, orthogonal to backfill, and
per-endpoint writer threads were never part of the settled design; the
single writer per endpoint stands, §3.)

**Formerly deferred, now shipped.** `metadata.json` generation shipped
2026-07-17 (§3): the post-commit, best-effort per-endpoint projection of a
successful run's committed facts — cosmetic, never read by the program. The
YAML config loader, previously deferred here, became roadmap item 6 and
shipped with the Sync vertical. The deferred inventory is empty.
