# fleetpull — Endpoint Manifest

The per-endpoint inventory: every shipped endpoint implementation, the port
queue behind it, and the discipline every new endpoint follows. DESIGN.md
remains the design of record — §8 carries the probe-captured provider
behaviors and per-endpoint decision blocks this file summarizes; §15 item 7
carries the roadmap this file's queue expands. **Update discipline:** adding,
renaming, or re-scoping an endpoint updates this file in the same change.

Sync modes and storage kinds are DESIGN §3/§4 vocabulary: a *snapshot* is a
full current-state listing replaced each run (always a single parquet file);
a *windowed watermark* endpoint fetches a half-open `[start, end)` UTC
window, writes `date=YYYY-MM-DD` partitions, and replaces each covered
partition wholesale. The *feed* mode (GeoTab `GetFeed`) has no shipped
endpoint yet — its vertical is unbuilt.

## Shipped

### Motive

REST over `https://api.gomotive.com`, static `X-API-Key` header
(`MOTIVE_API_KEY` fallback), one `motive` quota scope.

| Endpoint | Wire surface | Mode | Event time | Notes |
|---|---|---|---|---|
| `vehicles` | `GET /v1/vehicles` | snapshot | — | Page-numbered wrapped-list pagination. Feeds the `vehicle_ids` roster (1-day max age, eviction after 3 absent listings); lists inactive and retired vehicles, so historical fan-outs stay covered. |
| `vehicle_locations` | `GET /v3/vehicle_locations/{vehicle_id}` | windowed watermark | `located_at` | Fans out per vehicle over the `vehicle_ids` roster; unpaginated per fetch. Day-granular `start_date`/`end_date`, inclusive both ends. Documented 3-month range max. The id-less collection endpoint is a different animal (active-only last-known snapshot) and is deliberately not this history source. |
| `driving_periods` | `GET /v1/driving_periods` | windowed watermark | `start_time` | Fleet-wide, offset-paginated (page size 100). Window matching START-anchored on UTC days. Loud 30-day range cap (HTTP 400). |
| `idle_events` | `GET /v1/idle_events` | windowed watermark | `start_time` | Fleet-wide, offset-paginated (page size 100). Window matching OVERLAP-anchored on **company-local** days — the wire window pads one day each side and the true UTC window trims post-fetch. No range cap observed; chunking stays 30-day-bounded anyway. |

### GeoTab

JSON-RPC `POST https://{server}/apiv1`; session auth (`Authenticate`, ~14-day
sessions, single-flight refresh) with credentials injected per attempt; two
method-class quota scopes (`geotab` for `Get`, `geotab_authenticate` at the
fixed 10/min auth budget). Application errors arrive inside HTTP 200;
`error.data.type` is the discriminator, never the message text.

| Endpoint | Wire surface | Mode | Event time | Notes |
|---|---|---|---|---|
| `devices` | `Get Device` | snapshot | — | Id-ascending seek walk under the silent 5,000-record `Get` cap; every harvest verified against `GetCountOf` (mismatch fails the run loudly). Union-of-shapes model (GO7/GO9/trailer variants, everything optional). |
| `users` | `Get User` | snapshot | — | The devices pattern bound to `User` (seek walk + `GetCountOf`); id-sort proven live for this type. Scalar mirror — list-of-object and IAM blocks excluded per the Device precedent. |
| `trips` | `Get Trip` + `TripSearch` window | windowed watermark | `stop` | The window rides `search.fromDate`/`toDate` beside the id-sort seek walk. `TripSearch` matches by STOP time (prediction-confirmed), so retrieval and routing coincide on `stop`. Trip recalculation inside the lookback is absorbed by window refetch; beyond-lookback recalcs wait for the feed arm (accepted residual, DESIGN §4). |
| `exception_events` | `Get ExceptionEvent` + windowed search | windowed watermark | `active_from` | Id-sort rejected outright for this type, so the seek template is unavailable: the binding declares the `BisectedWindowFetch` shape (limit 5,000, one-minute floor) and the bisecting driver halves on the exactly-full overflow signal. OVERLAP-anchored matching; unfiltered rule stream by design — rule selection is the consumer's one-expression job. |

### Samsara

REST over `https://api.samsara.com`, bearer token (`SAMSARA_API_KEY`
fallback), one `samsara` quota scope. Success responses carry no rate-limit
headers; the configured self-limit is the only budget. 429s carry fractional
`Retry-After`.

| Endpoint | Wire surface | Mode | Event time | Notes |
|---|---|---|---|---|
| `vehicles` | `GET /fleet/vehicles` | snapshot | — | Explicit cursor walk (`limit` 512 — the documented maximum, honored exactly; `after` thereafter), terminal on `hasNextPage: false` beside an empty-string `endCursor`. Continuation is explicit per page, so no completeness check is needed. Optionality is absence-shaped; `externalIds` is an open user-definable map. |
| `drivers` | `GET /fleet/drivers` | snapshot | — | Two-sweep complete listing (the first `ParamSweep` consumer): the default listing is the ACTIVE set only, so the binding sweeps `driverActivationStatus` over `active`/`deactivated` and the union is the one dataset, split carried by the status column. The status vocabulary is a strict closed enum — any other value is a loud HTTP 400, never silent-empty. Same cursor walk as `vehicles` (limit 512; `after` composes with the status param, so the decoder is unchanged). |

## Port queue

Endpoint breadth is a scope principle (DESIGN §1): an endpoint is deferred,
never excluded for lacking a known consumer. fleet-telemetry-hub seeds the
order below; it is a bootstrap aid, not the ceiling.

### 1. Samsara legacy wave (next)

The legacy four first, then the remaining six — each on its own
probe-then-build vertical:

| Endpoint | Legacy wire surface | Status |
|---|---|---|
| `vehicles` | `/fleet/vehicles` | **shipped 2026-07-17** |
| `drivers` | `/fleet/drivers` | **shipped 2026-07-20** |
| `trips` | `/v1/fleet/trips` | queued (legacy four) — overlap-anchored retrieval verified historically; start-anchored normalization applies (DESIGN §4) |
| `idling_events` | `/idling/events` | queued (legacy four) |
| `addresses` | `/addresses` | queued |
| `vehicle_stats_history` | `/fleet/vehicles/stats/history` | queued |
| `location_stream` | `/assets/location-and-speed/stream` | queued |
| `driver_vehicle_assignments` | `/fleet/driver-vehicle-assignments` | queued |
| `vehicle_fuel_energy` | `/fleet/reports/vehicles/fuel-energy` | queued |
| `driver_fuel_energy` | `/fleet/reports/drivers/fuel-energy` | queued |

### 2. Motive deferred legacy endpoints

| Endpoint | Legacy wire surface | Status |
|---|---|---|
| `groups` | `/v1/groups` | deferred |
| `users` | `/v1/users` | deferred |
| `vehicle_utilization` | `/v2/vehicle_utilization` | deferred — documented company-local rollup timestamps: a documentation obligation on the mirror (verbatim timestamps, the timezone caveat in the model docstring) plus a window-matching probe question |
| `driver_utilization` | `/v2/driver_utilization` | deferred — same rollup-timezone obligation |

### 3. GeoTab growth

Not in the legacy hub (GeoTab is new in fleetpull). Two directions:

- **The feed arm** — the one unbuilt major vertical: the `GetFeed` runner,
  the append-only storage cells, token-commit crash ordering. Active feeds
  (`LogRecord`, `StatusData`) are append-only-complete; calculated feeds
  (`Trip`, `ExceptionEvent`, `FillUp`, `FuelUsed`, `FuelAndEnergyUsed`,
  `FuelTaxDetail`, `ChargeEvent`) re-emit versions, stored as emitted (the
  consumer reconciles by `(id, max version)`). Open question to settle
  empirically first: whether removals are signaled (DESIGN §4).
- **The wider `Get` entity surface** — Zone, Group, Rule, and kin, each
  probed per type before building (sortability, cap behavior, and window
  anchoring have all varied per type).

### 4. Beyond legacy

After the waves above, the queue continues across all three providers'
documented surfaces, endpoint by endpoint, on the same vertical.

## The port discipline

Every endpoint ships through the same probe-then-build vertical:

1. **Probe the live endpoint first.** A capture session (the
   `tests/*_capture.py` harnesses are the pattern) settles the facts the
   binding encodes: envelope shape, pagination mechanics, window-matching
   anchor, per-type caps and sortability, null/absence shape, unit semantics.
   **Encode probed behavior, never documented behavior alone** — providers
   have shipped inert documented-required parameters and unenforced
   documented limits, and window anchoring has differed between sibling
   endpoints on the same provider.
2. **Record the findings** in DESIGN §8 (observed-behaviors rows, plus a
   decision block when the endpoint needed design choices).
3. **Build the vertical:** the pure API-mirror model in
   `models/<provider>/`, the binding factory in `endpoints/<provider>/`
   (discovered — no registration), the catalog identity in `api/catalog.py`,
   and tests from the captured shapes (scrubbed; synthetic identifiers only).
4. **Prove it end-to-end** with a live run — `fleetpull sync` against a
   one-endpoint config, credentials from the environment — inspecting the
   endpoint's `metadata.json` and log output, before the endpoint is called
   done, and update this manifest.

All five verification gates green before any of it merges.
