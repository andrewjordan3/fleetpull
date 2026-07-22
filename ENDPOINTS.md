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
partition wholesale; a *feed* endpoint (GeoTab `GetFeed`) drives a
version-token stream and appends every emitted record into its event date's
partition as numbered `part-NNNNN.parquet` files — stored as emitted,
nothing ever deleted or replaced, the consumer reconciling calculated feeds
by `(id, max version)` and active feeds by `id` (DESIGN §4). The feed
MACHINERY is built in full and the first nine feed verticals — waves
one and two — ride it unchanged (shipped 2026-07-21); the remaining
feed queue is below.

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
| `groups` | `GET /v1/groups` | snapshot | — | The vehicles template verbatim: page-numbered wrapped-list pagination at the configured page size (50 and 100 both honored live). Whole-population census (152 records, every key on all 152) made every `Group` field required; `parent_id` is null on root groups (the groups form a tree). The owner ref's `username`/`driver_company_id` excluded as never-populated (value-unobservable — DESIGN §8). No roster. |
| `users` | `GET /v1/users` | snapshot | — | The vehicles template verbatim; the unfiltered listing is the whole population (2,665-record census, deactivated accounts included — no sweep). ONE dataset despite the perfectly role-partitioned shape: driver records (2,359) carry a driver-only key block absent — not null — on admin (32) / fleet_user (274) records, and the `role` column carries the split (DESIGN §8). Six never-populated keys excluded as value-unobservable; census-open `role`/`status` vocabularies stay plain strs. No roster. |
| `vehicle_utilizations` | `GET /v2/vehicle_utilization` | windowed watermark | `window_start` | The legacy hub's `vehicle_utilization` — the Samsara fuel-energy species on Motive wire (DESIGN §8): rows carry NO time identity, so `fixed_unit_days=1` pins the unit width (the §5 machinery's second consumer) and the `MotiveWindowReportPageDecoder` stamps each row with the sent INCLUSIVE `start_date`/`end_date` label pair — interpreted in COMPANY-LOCAL days (the account zone at UTC−5), documented on the mirror, never converted. Whole fleet every window (1,466 regardless of width; inactive vehicles zeroed with a free-text `message`); the vehicle ref is the shared `VehicleSummary` (third surface, `vin` nullable here). No roster. |
| `driver_idle_rollups` | `GET /v2/driver_utilization` | windowed watermark | `window_start` | The legacy hub's `driver_utilization`, shipped under the WIRE'S OWN envelope vocabulary (`driver_idle_rollups`/`driver_idle_rollup` — not the path's). The vehicle arm's binding with the population swapped: rows are the drivers with activity in the window (per-driver-per-window grain), each attributed to the shared 8-key `UserSummary` (fourth surface) — or NULL on the unattributed rollup bucket row. Bare-INT durations (floats on the vehicle arm — per-arm dtypes). Same fixed 1-day unit, decoder stamps, and company-local caveat (DESIGN §8). No roster. |

### GeoTab

JSON-RPC `POST https://{server}/apiv1`; session auth (`Authenticate`, ~14-day
sessions, single-flight refresh) with credentials injected per attempt; three
method-class quota scopes (`geotab_get` for `Get` at ~650/min,
`geotab_feed` for `GetFeed` at ~60/min — its own method class, proven by the
2026-07-21 header-decrement probe — and `geotab_authenticate` at the fixed
10/min auth budget). Application errors arrive inside HTTP 200;
`error.data.type` is the discriminator, never the message text.

| Endpoint | Wire surface | Mode | Event time | Notes |
|---|---|---|---|---|
| `devices` | `Get Device` | snapshot | — | Id-ascending seek walk under the silent 5,000-record `Get` cap; every harvest verified against `GetCountOf` (mismatch fails the run loudly). Union-of-shapes model (GO7/GO9/trailer variants, everything optional). |
| `users` | `Get User` | snapshot | — | The devices pattern bound to `User` (seek walk + `GetCountOf`); id-sort proven live for this type. Scalar mirror — list-of-object and IAM blocks excluded per the Device precedent. |
| `trips` | `Get Trip` + `TripSearch` window | windowed watermark | `stop` | The window rides `search.fromDate`/`toDate` beside the id-sort seek walk. `TripSearch` matches by STOP time (prediction-confirmed), so retrieval and routing coincide on `stop`. Trip recalculation inside the lookback is absorbed by window refetch; beyond-lookback recalcs wait for the feed arm (accepted residual, DESIGN §4). |
| `exception_events` | `Get ExceptionEvent` + windowed search | windowed watermark | `active_from` | Id-sort rejected outright for this type, so the seek template is unavailable: the binding declares the `BisectedWindowFetch` shape (limit 5,000, one-minute floor) and the bisecting driver halves on the exactly-full overflow signal. OVERLAP-anchored matching; unfiltered rule stream by design — rule selection is the consumer's one-expression job. |
| `log_records` | `GetFeed LogRecord` | feed | `date_time` | The first feed vertical: the ACTIVE GPS stream — no per-record version, so append-only is trivially complete and the consumer reconciles by `id` (DESIGN §4). Whole-page census 2,000/2,000 every key → all-required mirror; `speed` a bare int mirrored verbatim. >50,000 records/day on the probed tenant; `resultsLimit` 50,000 (the protocol max). |
| `status_data` | `GetFeed StatusData` | feed | `date_time` | The log_records binding with the entity swapped: an active feed that, unlike LogRecord, carries a per-record `version` — mirrored as wire truth. `data` (the diagnostic value) is mixed int\|float → float; `controller` a census-open plain str. ~24,500 records/hour; `resultsLimit` 50,000. The name is the wire's uncountable vocabulary (no plural to form). |
| `fill_ups` | `GetFeed FillUp` | feed | `date_time` | Calculated fuel-stop detections, reconciled `(id, max version)`. ESTIMATES-ONLY TENANT (DESIGN §8): no fuel-transaction integration, so every fuel value is provider-derived — `cost` 0.0 throughout, `fuelTransactions` excluded as value-unobservable (empty on 100/100; on integrated tenants it populates with a never-captured shape). The `-1.0` `derivedVolume` sentinel is mirrored verbatim; `driver` is the object-or-`UnknownDriverId` sentinel (the Trip mechanism); `confidence` a comma-joined token list kept one plain str. `resultsLimit` 10,000 — the DOCUMENTED cap, dual provenance: a 50,000 request was ACCEPTED at the 380-record population, so the cap was unprobeable. |
| `fuel_and_energy_used` | `GetFeed FuelAndEnergyUsed` | feed | `date_time` | A WIRE-VOCABULARY name, not a plural (the driver_idle_rollups precedent — DESIGN §8). Calculated per-trip fuel/energy totals, reconciled `(id, max version)`; `FuelUsed` is NOT ported — observed identical to this surface week-wide on the probed tenant, and the provider documents THIS surface as FuelUsed's successor. The estimates-only caveat applies; `confidence` census-open (`'None'` on 1,994/2,000). `resultsLimit` 50,000. |
| `fuel_tax_details` | `GetFeed FuelTaxDetail` | feed | `enter_time` | Calculated IFTA jurisdiction segments — the segment materializes where it begins, so `enter_time` is the event time. The version identity is the `versions` LIST of 16-hex component tokens (list[scalar]); the hourly arrays may be EMPTY lists, mirrored as such; `driver` is the object-or-sentinel mechanism. The estimates-only caveat applies. `resultsLimit` 50,000. |
| `fault_data` | `GetFeed FaultData` | feed | `date_time` | Active with NO per-record version (the LogRecord asymmetry) — append-only-complete, reconciled by `id`. Wave-two conservative requiredness (only `id`/`dateTime`/`device` structural — DESIGN §8); the rare quartet (`diagnosticSeverity`/`riskOfBreakdown`/`severity`/`sourceAddress`, 2/2,000 each) optional scalars. `failureMode` PROVEN object-or-string; every ref rides the defensive `bare_id_to_reference` lift. `faultStates` is a wire-plural name over ONE `{effectiveStatus}` object. `resultsLimit` 50,000. |
| `duty_status_logs` | `GetFeed DutyStatusLog` | feed | `date_time` | EDITABLE HOS log (`editDateTime` the edit trail), versioned — reconciled `(id, max version)`. `device`/`driver` PROVEN object-or-string. `annotations` reduced to a STRICT id-list (`list[str]`; elements exactly `{id}` on the census — any other shape fails loudly; the ids join the wave-three `annotation_logs` vertical). `location` promoted the shared nested-location wrapper — a `{x, y}` coordinate arm (x longitude, y latitude) or a `{formattedAddress}` address arm (the live proof found the address arm the 200-sample census missed). `resultsLimit` 50,000. |
| `driver_changes` | `GetFeed DriverChange` | feed | `date_time` | Versioned driver-to-device assignment events, user-editable — reconciled `(id, max version)`. `driver` PROVEN object-or-string with `isDriver` riding the object arm (null exactly on string-arm rows). `resultsLimit` 50,000. |
| `dvir_logs` | `GetFeed DVIRLog` | feed | `date_time` | Model `DvirLog` (house casing; the wire typeName keeps `DVIRLog`). Versioned certified/edited inspections — reconciled `(id, max version)`. `device`/`engineHours`/`odometer` a commonly-absent trio (205/500 each); `engineHours` modeled float per the cross-surface mixed proof. `defectList` is a wire-plural name over ONE `{id, name}` node — `children` EXCLUDED (empty on all 200 sampled nodes, element shape unobservable; revisit on a tenant with populated children). The shared nested-location wrapper (coordinate or address arm); `duration` an opaque string mirrored verbatim. `resultsLimit` 50,000. |
| `annotation_logs` | `GetFeed AnnotationLog` | feed | `date_time` | Versioned duty-status-log annotations (8,857 records) — reconciled `(id, max version)`. COMPLETES the wave-two loop: `dutyStatusLog.id` is the BACK-REFERENCE to `duty_status_logs` (whose `annotations` id-list points here). Primary ref `dutyStatusLog` (the annotated log) required; `driver` optional. Both refs object-only at scale, both ride the defensive `bare_id_to_reference` lift. `comment` census-open. `resultsLimit` 50,000. |
| `shipment_logs` | `GetFeed ShipmentLog` | feed | `date_time` | Versioned shipment manifests (2,771 records) — reconciled `(id, max version)`. Primary ref `driver` (the log family convention) required; `device` optional; both object-only at scale, both ride the defensive lift. `activeFrom`/`activeTo` the shipment's active window; `commodity`/`documentNumber`/`shipperName` census-open strs (`shipperName` synthetic in fixtures). `resultsLimit` 50,000. |
| `audits` | `GetFeed Audit` | feed | `date_time` | Versioned config audit-trail entries (20,000 records) — reconciled `(id, max version)`. The SIMPLEST vertical: NO reference fields. `comment`/`name`/`userName` census-open strs (`userName` synthetic in fixtures). `resultsLimit` 50,000. |
| `text_messages` | `GetFeed TextMessage` | feed | `sent` | Dispatch messages (25,000 records). NO per-record `version` AND NO `dateTime` (the FaultData/LogRecord asymmetry) — append-only-complete, reconciled by `id`; the event time is `sent` (25,000/25,000). Delivered/read receipts re-emit under newer FEED `toVersion`, stored-as-emitted (feed versioning, not a `version` key). `messageContent` is a NESTED block `{contentType, ids}` — `ids` a DIRECT `list[str]` (elements are strings on the wire, NOT the annotations id-object reduction). `device` optional (no required primary ref); `recipient` synthetic in fixtures. `resultsLimit` 50,000. |
| `media_files` | `GetFeed MediaFile` | feed | `from_date` | Versioned media attachments — reconciled `(id, max version)`. THIN evidence (55 records over 730 days), so conservative. NO `dateTime` — the event time is `fromDate` (55/55). `device` PROVEN mixed object-or-string (42 str / 13 object); `driver` string-only observed; both ride the lift, BOTH refs OPTIONAL (a media file's primary entity is ambiguous). THREE documented exclusions (empty on all 55, element/content shape unobservable, `extra='ignore'` absorbs, revisit when populated): `metaData`, `tags`, `thumbnails`. `mediaType`/`name`/`solutionId`/`status` census-open. `resultsLimit` 50,000. |

### Samsara

REST over `https://api.samsara.com`, bearer token (`SAMSARA_API_KEY`
fallback), one `samsara` quota scope. Success responses carry no rate-limit
headers; the configured self-limit is the only budget. 429s carry fractional
`Retry-After`.

| Endpoint | Wire surface | Mode | Event time | Notes |
|---|---|---|---|---|
| `vehicles` | `GET /fleet/vehicles` | snapshot | — | Explicit cursor walk (`limit` 512 — the documented maximum, honored exactly; `after` thereafter), terminal on `hasNextPage: false` beside an empty-string `endCursor`. Continuation is explicit per page, so no completeness check is needed. Optionality is absence-shaped; `externalIds` is an open user-definable map. Feeds the Samsara `vehicle_ids` roster (1-day max age, eviction after 3 absent listings); lists unplugged units, and eviction hysteresis covers removals. |
| `drivers` | `GET /fleet/drivers` | snapshot | — | Two-sweep complete listing (the first `ParamSweep` consumer): the default listing is the ACTIVE set only, so the binding sweeps `driverActivationStatus` over `active`/`deactivated` and the union is the one dataset, split carried by the status column. The status vocabulary is a strict closed enum — any other value is a loud HTTP 400, never silent-empty. Same cursor walk as `vehicles` (limit 512; `after` composes with the status param, so the decoder is unchanged). |
| `trips` | `GET /v1/fleet/trips` | windowed watermark | `start_time` | The LEGACY v1 surface only (the modern candidates 404); `vehicleId` is REQUIRED, so the binding fans out per vehicle over the Samsara `vehicle_ids` roster — the roster machinery's first cross-provider consumer. The wire record does NOT echo `vehicleId`, so `SamsaraTripsPageDecoder` stamps it onto every trip off the sent spec (the sole synthesized field — a numeric string, joining `Vehicle.id`); without it the stored row has no vehicle attribution. Unpaginated `{"trips": [...]}` envelope; `startMs`/`endMs` epoch milliseconds. Retrieval is OVERLAP-anchored (re-verified per-type 2026-07-20); ownership is start-anchored via the post-fetch filter, no wire pad. Loud, exactly-90-day range cap — HTTP 400 with a text/plain rpc-error body (the plain-string posture beyond 5xx). |
| `idling_events` | `GET /idling/events` | windowed watermark | `start_time` | Fleet-wide with per-record asset attribution (`asset.id`), so no fan-out — the first windowed+cursor pairing: the RFC3339 `startTime`/`endTime` window rides the same cursor walk as `vehicles`/`drivers`, but at THIS endpoint's `limit` maximum of 200 (limit=512 is a loud JSON 400 — the first captured per-endpoint limit tier; never assume a sibling's limit). Retrieval is START-anchored on UTC (a discriminating pair; NOT Motive `idle_events`' company-local overlap), so retrieval and routing coincide on `start_time` with no wire pad. Records carry NO end key — the interval is start + `durationMilliseconds`. Loud sub-3-months range cap (JSON 400). |
| `addresses` | `GET /addresses` | snapshot | — | The vehicles template verbatim: the standard cursor walk at this endpoint's probed 512 tier (513 is a loud HTTP 400 — the tier probed per-endpoint, never assumed from a sibling). The full walk was the whole 25-record population in one page. `geofence.polygon` (24/25) is model-excluded wholesale — its only content is a `vertices` list-of-objects — while the top-level `latitude`/`longitude` keep every address's center point; `tags` excluded per the standing precedent. No roster sourced or consumed. |
| `engine_states` | `GET /fleet/vehicles/stats/history`, `types=engineStates` | windowed watermark | `time` | One of the THREE endpoints the legacy stats surface splits into (disjoint per-type schemas, so one entity per endpoint — DESIGN §8). The cursor walks the VEHICLE axis within the fixed RFC3339 window (zero vehicle-id overlap across pages, proven live); the series-unnesting decoder (composing the cursor decoder by delegation) emits one flat record per reading with synthesized `vehicleId`/`vehicleName`/`vehicleSerial`/`vehicleVin`. Probed 512 limit tier; `types` API-enforced on input (loud 400, never silent-empty); retrieval reading-time anchored on the half-open `[start, end)` window. Only carrier vehicles returned per type; `value` a plain str (`On`/`Off`/`Idle` census-closed only, not API-enforced). No roster. |
| `gps_readings` | `GET /fleet/vehicles/stats/history`, `types=gps` | windowed watermark | `time` | The gps arm of the stats triple — the engine_states row's mechanics verbatim (vehicle-axis cursor, series-unnesting decoder, 512 tier, reading-time anchoring). Seven always-present reading keys (`time`, coordinates, `headingDegrees`, `speedMilesPerHour` mixed int\|float modeled float, `isEcuSpeed`, `reverseGeo {formattedLocation}`); optional `address {id, name}` — the address-book reference (401/2,512 sampled). |
| `odometer_readings` | `GET /fleet/vehicles/stats/history`, `types=obdOdometerMeters` | windowed watermark | `time` | The odometer arm of the stats triple — the engine_states row's mechanics verbatim (vehicle-axis cursor, series-unnesting decoder, 512 tier, reading-time anchoring). Series keys exactly `{time, value}` with `value` the OBD odometer in bare-int METERS, mirrored verbatim. |
| `asset_locations` | `GET /assets/location-and-speed/stream` | windowed watermark | `happened_at_time` | The legacy hub's `location_stream`, renamed per the name=plural-of-entity invariant. The `ids` filter is REQUIRED (id-less → HTTP 400) with the batch cap API-enforced at 50, so the binding declares the first `BatchedRosterFanOut`: one cursor-walk chain per sorted 50-member comma-joined batch of the Samsara `vehicle_ids` roster — transport packing only (records self-identify via `asset.id`, a STRING here), resolved onto the existing member-agnostic fan-out driver, whose progress lines count batches for this shape. Standard cursor walk at this surface's probed 512 tier (513 → 400); reading-time anchored on the half-open `[start, end)` window. No speed key observed despite the wire path's name; `location.geofence` observed-empty (DESIGN §8). |
| `driver_vehicle_assignments` | `GET /fleet/driver-vehicle-assignments` | windowed watermark | `start_time` | ONE dataset despite the REQUIRED two-value `filterBy` (missing/bogus → loud HTTP 400): full 24h walks under `vehicles` and `drivers` returned IDENTICAL row sets (216 = 216 as tuple sets), so the axis is traversal, not partition — `filterBy=vehicles` is baked in as a fixed builder param (the stats triple's `types` idiom), no sweep, no second endpoint. Standard cursor walk with `results_limit=50` declared as documentation of the server's own FIXED 50-record paging — the `limit` param is proven ignored (513 not rejected; no enforced tier). Retrieval OVERLAP-anchored (midnight-spanning assignments shared across adjacent windows); ownership start-anchored via the post-fetch filter, no wire pad — the trips decisions mirrored. `vehicle.externalIds` carries the literal dotted `samsara.serial`/`samsara.vin` wire keys on the nested ref. No roster. |
| `vehicle_fuel_energy_reports` | `GET /fleet/reports/vehicles/fuel-energy` | windowed watermark | `window_start` | The legacy hub's `vehicle_fuel_energy`, renamed per the name=snake-plural-of-model invariant (`VehicleFuelEnergyReport`). The first WINDOW-GRAIN ROLLUP endpoint: the provider aggregates over exactly the requested window (widening the window GREW per-vehicle metrics) and day rollups are NON-ADDITIVE into wider windows (89/267 mismatched), so the binding declares `fixed_unit_days=1` on its `WatermarkMode` — the unit width is part of the row's meaning and never floats with `backfill_chunk_days`. Rows carry NO event-time key; the `SamsaraWindowReportPageDecoder` extracts the NESTED report list (`data` is an object holding `vehicleReports`) and stamps every report with the sent window (`windowStartDate`/`windowEndDate` verbatim from the sent `startDate`/`endDate` — this family's own param names, RFC3339 accepted despite them). `results_limit=100` documents the server's own ~100-report paging (`limit` proven ignored: 512/513/10 all paged identically). Census-open `energyType`/`currencyCode` stay plain strs; the dotted `externalIds` on the nested vehicle ref. No roster. |
| `driver_fuel_energy_reports` | `GET /fleet/reports/drivers/fuel-energy` | windowed watermark | `window_start` | The legacy hub's `driver_fuel_energy`, renamed per the name=snake-plural-of-model invariant (`DriverFuelEnergyReport`). The vehicle arm's binding with the path and report key swapped (`data.driverReports`): the same metric core + `estFuelEnergyCost` attributed to `driver {id, name}` — NO `externalIds` was ever observed on this arm. The pair's window-grain and non-additivity proofs apply, so `fixed_unit_days=1`, the decoder-stamped `window_start` routing, `results_limit=100` as documentation of the server's own paging, and the `startDate`/`endDate` naming quirk are all shared. No roster. |

## Port queue

Endpoint breadth is a scope principle (DESIGN §1): an endpoint is deferred,
never excluded for lacking a known consumer. A legacy telematics package
seeded the initial porting order below; it is a bootstrap aid, not the
ceiling.

### 1. Samsara legacy wave (COMPLETE 2026-07-21)

Every legacy-hub Samsara endpoint is shipped — the legacy four first
(complete 2026-07-20), then the remainder, each on its own
probe-then-build vertical:

| Endpoint | Legacy wire surface | Status |
|---|---|---|
| `vehicles` | `/fleet/vehicles` | **shipped 2026-07-17** |
| `drivers` | `/fleet/drivers` | **shipped 2026-07-20** |
| `trips` | `/v1/fleet/trips` | **shipped 2026-07-20** |
| `idling_events` | `/idling/events` | **shipped 2026-07-20** |
| `addresses` | `/addresses` | **shipped 2026-07-20** |
| `vehicle_stats_history` | `/fleet/vehicles/stats/history` | **shipped 2026-07-20 as three endpoints: `engine_states`, `gps_readings`, `odometer_readings`** (disjoint per-type schemas — DESIGN §8) |
| `location_stream` | `/assets/location-and-speed/stream` | **shipped 2026-07-20 as `asset_locations`** (renamed per the name=plural-of-entity invariant; the first `BatchedRosterFanOut` consumer — DESIGN §8) |
| `driver_vehicle_assignments` | `/fleet/driver-vehicle-assignments` | **shipped 2026-07-20** |
| `vehicle_fuel_energy` | `/fleet/reports/vehicles/fuel-energy` | **shipped 2026-07-21 as `vehicle_fuel_energy_reports`** (renamed per the name=snake-plural-of-model invariant, model `VehicleFuelEnergyReport`; the first fixed-unit-width endpoint — DESIGN §8) |
| `driver_fuel_energy` | `/fleet/reports/drivers/fuel-energy` | **shipped 2026-07-21 as `driver_fuel_energy_reports`** (renamed per the name=snake-plural-of-model invariant, model `DriverFuelEnergyReport`) |

### 2. Motive deferred legacy endpoints (COMPLETE 2026-07-21)

Every legacy-hub Motive endpoint is shipped:

| Endpoint | Legacy wire surface | Status |
|---|---|---|
| `groups` | `/v1/groups` | **shipped 2026-07-21** |
| `users` | `/v1/users` | **shipped 2026-07-21** (one dataset; the role-partitioned shape rides the `role` column — DESIGN §8) |
| `vehicle_utilization` | `/v2/vehicle_utilization` | **shipped 2026-07-21 as `vehicle_utilizations`** (the wire's plural envelope vocabulary, model `VehicleUtilization`; the company-local rollup obligation discharged as a docstring caveat beside the decoder's window stamp, `fixed_unit_days=1` — DESIGN §8) |
| `driver_utilization` | `/v2/driver_utilization` | **shipped 2026-07-21 as `driver_idle_rollups`** (the wire's OWN envelope vocabulary — not the path's; model `DriverIdleRollup` — DESIGN §8) |

### 3. GeoTab growth

Not in the legacy hub (GeoTab is new in fleetpull). Two directions:

- **The feed verticals** — the feed MACHINERY is built in full (2026-07-21:
  the append-log storage cell, the kind-guarded token commit, the runner's
  per-page seed-or-resume drive, the `geotab_feed` rate class, the shared
  `GetFeed` spec builder, and the `FeedEndpoint` catalog identity — DESIGN
  §3/§4/§5/§14). Seeding via `search.fromDate` is wire-proven (2026-07-21,
  despite the docs claiming some types' search is ignored); removals may be
  unsignaled (the dated accepted residual, DESIGN §4). The probed
  **14-vertical feed queue**, each to ship on its own probe-then-build
  vertical:
  - *The five original feed entities — ALL SHIPPED 2026-07-21* (feed wave
    one, zero shared-machinery changes; the DESIGN §8 block carries the
    probe-settled decisions):

    | Feed entity | Endpoint | Status |
    |---|---|---|
    | `LogRecord` | `log_records` | **shipped 2026-07-21** (active — append-only-complete, no per-record version) |
    | `StatusData` | `status_data` | **shipped 2026-07-21** (active, WITH a per-record version — mirrored) |
    | `FillUp` | `fill_ups` | **shipped 2026-07-21** (calculated; the estimates-only caveat; the 10,000 documented-cap dual provenance) |
    | `FuelAndEnergyUsed` | `fuel_and_energy_used` | **shipped 2026-07-21** (wire-vocabulary name, not a plural; `FuelUsed` NOT ported — observed identical on the probed tenant, its provider-documented predecessor) |
    | `FuelTaxDetail` | `fuel_tax_details` | **shipped 2026-07-21** (calculated; the `versions` list identity; the estimates-only caveat) |

    `Trip` and `ExceptionEvent` stay on their shipped `Get` verticals;
    migrating them to the feed is a recorded evaluation item, not queue
    debt.
  - *Tier 1 — feed wave two, ALL SHIPPED 2026-07-21* (zero
    shared-machinery changes; the DESIGN §8 wave two block carries the
    probe-settled decisions):

    | Feed entity | Endpoint | Status |
    |---|---|---|
    | `FaultData` | `fault_data` | **shipped 2026-07-21** (active — no per-record version) |
    | `DutyStatusLog` | `duty_status_logs` | **shipped 2026-07-21** (editable, versioned; the strict annotations id-list) |
    | `DriverChange` | `driver_changes` | **shipped 2026-07-21** (versioned; the proven object-or-string driver) |
    | `DVIRLog` | `dvir_logs` | **shipped 2026-07-21** (model `DvirLog`; the `defectList.children` documented exclusion) |
  - *Tier 2 — feed wave three, ALL SHIPPED 2026-07-21* (zero
    shared-machinery changes; the DESIGN §8 wave three block carries the
    probe-settled SCALE-census decisions):

    | Feed entity | Endpoint | Status |
    |---|---|---|
    | `AnnotationLog` | `annotation_logs` | **shipped 2026-07-21** (versioned; the `dutyStatusLog` back-reference completing the wave-two loop) |
    | `ShipmentLog` | `shipment_logs` | **shipped 2026-07-21** (versioned; `driver` primary ref) |
    | `Audit` | `audits` | **shipped 2026-07-21** (versioned; the simplest vertical — no reference fields) |
    | `TextMessage` | `text_messages` | **shipped 2026-07-21** (NO version AND NO dateTime — append-only, `event_time_column='sent'`; the `messageContent` `{contentType, ids}` nested block) |
    | `MediaFile` | `media_files` | **shipped 2026-07-21** (versioned; NO dateTime — `event_time_column='from_date'`; PROVEN mixed `device`; the three empty-container exclusions; thin 55-record evidence) |
  - *Deferred as unobservable on the probed tenant* (no data to probe a
    shape from — deferred, never excluded): `ChargeEvent`,
    `TrailerAttachment`, `IoxAddOn`, `CustomData`,
    `EmissionComplianceEvent`, `Route`. `DeviceStatusInfo` is wire-proven
    NOT feed-capable.
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
   endpoints on the same provider. A census is a tenant-scoped observation:
   it proves shapes and surface behavior for the probed account at capture
   time, never data semantics and never other tenants' shapes.
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
