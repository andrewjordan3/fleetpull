# fleetpull — Pre-API Audit (roadmap item 4)

**Date:** 2026-07-06
**Anchor:** DESIGN §10 as recorded at commit `c6f55db` (fetch: snapshot-only,
in-memory, `Endpoints` catalog, string-or-named `auth=`; sync: config-path
construction, vocabulary deferred to item 6). The evidence artifact is
`scripts/run_vehicle_locations.py` (the hand-written consumer cost) plus
`scripts/run_vehicles_snapshot.py` (the existence proof for the state-free
trace).
**Scope boundary:** coverage, docstring, and README audits belong to the
polish phase (roadmap item 8) and are explicitly out of scope here. This
report ships no fixes.
**Verdict vocabulary:** `blocks-the-API` (must be fixed before or with the
verb's build — each such finding names which verb: fetch/item 5 or
sync/item 6), `correctness-independent` (a defect regardless of the API),
`cosmetic`, `leave-alone` (with the reason it stays). Evidence is labeled
**read** (inspected in source) or **inferred** (derived from docstrings or
design text without executing/inspecting the cited body).

Fleet-specific numbers cited from the live run describe this fleet only and
generalize to nothing.

---

## Part A — the wiring inventory

Every element the script hand-wires between the constants block and the
`run_endpoint` call, one row each. Classifications: **fetch-hides** (public
`fetch` absorbs internally), **sync-hides** (`sync`/yaml-run absorbs),
**config-knob** (user-controlled value; candidate YAML key named),
**diagnostic-scaffolding** (script-only reporting), **accidental** (should
exist in no consumer's hands).

| # | Element | Script location | Class | Notes |
|---|---------|-----------------|-------|-------|
| 1 | `MOTIVE_API_KEY` | `run_vehicle_locations.py:74` | config-knob | fetch: the `auth=` parameter; YAML: `providers.motive.api_key` |
| 2 | `DATASET_ROOT` | `:80` | config-knob | `storage.dataset_root` (exists as `SyncConfig.dataset_root`) |
| 3 | `USE_TRUSTSTORE` | `:83` | config-knob | `http.use_truststore` (exists as `HttpConfig.use_truststore`) |
| 4 | `MOTIVE_BASE_URL` | `:85` | config-knob | `providers.motive.base_url` (exists on `MotiveConfig`) |
| 5 | `BACKFILL_LOOKBACK_DAYS` + the `default_start_date` computation | `:91`, `:321–323` | diagnostic-scaffolding | The diagnostic derives a bounded anchor from `now`; the real knob is row 24's `default_start_date` |
| 6 | `LOOKBACK_DAYS`, `CUTOFF_DAYS` | `:97–98` | config-knob | `providers.motive.lookback_days` / `.cutoff_days` (exist on `MotiveConfig`) |
| 7 | `MOTIVE_RATE_LIMIT = RateLimitConfig(...)` | `:103–105` | config-knob | `rate_limits.<quota_scope>` — the values have **no config home today** (finding AUD-12); `RateLimitConfig` still lives in `network/limits/`, with its migration to `config/` already planned (DESIGN §11) |
| 8 | `_DEDUP_IDENTITY_COLUMN` | `:111` | diagnostic-scaffolding | Dedup-report key only |
| 9 | `_INCREMENTAL_SEMANTICS`, `logging.basicConfig`, all `print` reporting | `:113`, `:142` | diagnostic-scaffolding | |
| 10 | `MotiveConfig(...)` construction (`_build_motive_config`) | `:145–149` | fetch-hides | fetch builds provider config from defaults plus its few arguments; sync builds it from YAML |
| 11 | `build_endpoint_registry([motive_config])` | `:305` | fetch-hides | Identity → definition resolution moves behind the verb; the consumer holds only an `Endpoints` catalog identity |
| 12 | `endpoint_registry.get(Provider.MOTIVE, 'vehicle_locations')` | `:306` | fetch-hides | Replaced by `Endpoints.Motive.vehicle_locations`; also removes the consumer's `Provider` enum import (row 27) |
| 13 | `RosterRegistry([VEHICLE_IDS_ROSTER])` + the `VEHICLE_IDS_ROSTER` leaf import | `:38`, `:307` | sync-hides | fetch touches no roster (§10); the hand-listing is finding AUD-05 |
| 14 | `endpoint_directory(...)` + `parse_date_partition_segment` (paths imports) | `:58`, `:308–310` | diagnostic-scaffolding | Report reads only; writers derive their own paths internally |
| 15 | `StateDatabase` + `initialize()` + `migrate_to_head` | `:168–171` | sync-hides | |
| 16 | the state DB path expression `dataset_root / '.fleetpull' / 'state.sqlite3'` | `:168` | config-knob | `state.database_path`, defaulting to this convention per DESIGN §5 — **no `SyncConfig` field exists** (finding AUD-13) |
| 17 | `CursorStore(database, clock)` | `:175` | sync-hides | |
| 18 | `RunLedger(database, clock)` | `:176` | sync-hides | |
| 19 | `RosterStore(database)` | `:177` | sync-hides | |
| 20 | `ProviderProfile(StaticHeaderAuth('X-API-Key', SecretStr(key)), MotiveResponseClassifier())` (+ the `SecretStr` import) | `:34`, `:315–318` | fetch-hides | The `auth=` ingress must own the provider → header-name/classifier map; `'X-API-Key'` is provider knowledge no consumer should type |
| 21 | `HttpConfig(...)`, `RetryConfig()`, `RateLimiterRegistry({scope: limit})`, `ClientRuntime(...)` (`_build_client_runtime`) | `:152–161` | fetch-hides | Values are rows 3/7; `RetryConfig` defaults seed future `retry.*` YAML keys |
| 22 | `random.Random()` and `SystemSleeper()` | `:28`, `:159–160` | accidental | Test-injection seams (`random_source`, `sleeper`) with obvious production defaults; forcing every composition root to know jitter/sleep internals exist is exposure without benefit. The internal composition should default them (as `RateLimiterRegistry` already defaults its clock — read, `limits/registry.py:28`) |
| 23 | `SystemClock()` | `:172`, `:320` | sync-hides | The fetch trace needs no clock: the limiter self-supplies one (read, `limits/registry.py:28`); the clock serves state stores, window resolution, and the coordinator — all sync territory |
| 24 | `SyncConfig(default_start_date, dataset_root)` | `:324–326` | config-knob | `sync.default_start_date` + row 2; construction itself is sync-hides, the two values are the knobs |
| 25 | `ProviderClientRegistry({Provider.MOTIVE: profile}, runtime)` | `:328` | sync-hides | Multi-provider client pooling; fetch composes one `TransportClient` directly (proven by `run_vehicles_snapshot.py:80`) |
| 26 | `RosterRefreshCoordinator(...)` | `:329–331` | sync-hides | |
| 27 | `EndpointRunner(...)` | `:332–338` | sync-hides | |
| 28 | `run_locations_once()` closure + `run_endpoint(...)` | `:340–344` | sync-hides | The composed entry call is what `sync.run()` iterates per endpoint |
| 29 | `Provider` / `QuotaScope` vocabulary imports | `:68` | fetch-hides | §10 consumers address endpoints via the catalog; vocabulary enums stay internal |
| 30 | `_RunReporter`, `_read_partition_frames`, `_print_partition_layout`, `_print_run_outcome`, `_print_watermark`, `_check_duplicates`, roster-count print, net-new analysis (+ the `polars`, `DateWatermark`/`IncrementalCursor`, `Executed`/`CaughtUp`/`RunOutcome`, `EndpointDefinition`/`ResponseModel`, `Callable`/`date`/`timedelta`/`Path` imports they exist for) | `:181–291`, `:346–386` | diagnostic-scaffolding | The report half of the script; not consumer cost |

**Completeness check:** every `fleetpull` import (script lines 36–68), every
third-party import (`polars`, `SecretStr`, `random`), every stdlib import
(`logging`, `Callable`, `date`/`timedelta`, `Path`), and every constructor
call in `main` and its helpers (`_build_motive_config`, `_build_client_runtime`,
`_build_state`, `_RunReporter`, plus the inline constructions at `:305–344`)
appears in exactly one row above. Passes.

---

## Part B — the state-free fetch trace

**The path exists cleanly.** `scripts/run_vehicles_snapshot.py` is the
existence proof, composed end to end with zero imports from `state`,
`storage`, `orchestrator`, `roster`, `incremental`, or `paths` (read):

```
config            MotiveConfig, HttpConfig, RetryConfig
endpoints.motive  build_endpoint(config)  →  EndpointDefinition (snapshot)
network.auth      StaticHeaderAuth        ┐
network.classifiers MotiveResponseClassifier ├→ ProviderProfile
network.limits    RateLimiterRegistry({scope: RateLimitConfig})  ┐
timing            SystemSleeper (seam; see row 22)               ├→ ClientRuntime
config            HttpConfig, RetryConfig                        ┘
network.client    TransportClient(profile, runtime)
                  spec = definition.spec_builder.build_spec(resume=None, path_values={})
                  client.fetch_pages(spec, definition.page_decoder, scope)
records           validate_records(records, Model) → models_to_dataframe(models, Model)
```

No `StateDatabase`, no cursor, no ledger, no roster, no staging, no disk, and
— verified against source — no clock (the limiter registry defaults its own,
`limits/registry.py:28`, read). The orchestrator is bypassed entirely: for a
single snapshot chain, `client.fetch_pages` is called directly and
`SingleRequestDriver` adds nothing. **No forced state dependency was found;
there are zero blocks-the-API findings from this trace's structure.** This
composition list is the item-5 build map (closing section).

---

## Findings

### AUD-01 — unclassified `json.loads` on the success path escapes raw
- **Location:** `src/fleetpull/network/client/transport.py:287` (`_attempt`)
- **Evidence (read):** on a SUCCESS-classified response whose classifier left
  `parsed_body` empty (Motive/Samsara are status-only classifiers), the client
  runs `json.loads(body_text)` outside any try. A 200 response with a
  non-JSON body raises `json.JSONDecodeError` (a `ValueError` subclass) that
  escapes `fetch_pages` uncaught. A TLS-intercepting proxy serving an HTML
  block page with status 200 is exactly the environment this package defends
  against elsewhere (`truststore` is a dependency for it), so the input is
  realistic, and the escape sits directly on the Part B fetch trace.
- **Consumer cost:** a public `fetch` caller obeying §10's `Raises` promise
  (`FleetpullError` + four subclasses) crashes on an exception type the
  promise says cannot happen.
- **Verdict:** **blocks-the-API** (fetch, item 5). Fix shape for the owner:
  classify the parse failure (`ProviderResponseError` with a safe detail),
  beside the existing SUCCESS-with-no-body guard at `:186–189`.
- **Owner:** the exception-contract slice of item 5 (or a small standalone
  fix prompt before it).
- **Resolution (2026-07-06):** fixed in audit wave 1 — the SUCCESS-path parse is guarded beside the no-body guard, raising `ProviderResponseError` with a sanitized excerpt, one attempt, with the block-page-shaped test.

### AUD-02 — `JsonObject`'s home couples three packages to `network.contract`
- **Location:** `src/fleetpull/records/validation.py:16` (the flagged edge);
  also `orchestrator/batch.py:22` and `network/client/page.py:6`; the alias
  is defined in `network/contract/request.py`.
- **Evidence (read):** `validate_records(records: Sequence[JsonObject], ...)`
  — the records layer's input type is imported from the network package.
  The layering is legal (records sits above network in the enforced
  vertical), but the *concept* is generic JSON vocabulary, not a network
  contract: three packages import it from a package whose name says
  transport. The §15 item-4 concern — a type that could leak into public
  signatures — is real but bounded: §10's `fetch` surface exposes frames and
  models, never raw record dicts, so no public signature needs the alias.
- **Consumer cost:** none today at the public surface; internal legibility
  cost only (a reader of `records/` is sent to `network/contract` for a
  plain type alias).
- **Verdict:** **cosmetic** — relocate the JSON aliases to a leaf
  (`vocabulary/` or a small `json` leaf) when convenient; no behavior
  change, moderate import churn (three importers plus tests).
- **Owner:** item 5 wave (it touches the modules that wave already opens).
- **Resolution (2026-07-06):** fixed in audit wave 1 — the three aliases moved to `vocabulary/json_types.py` (already the bottom tier, so no layers-contract change); the contract face no longer re-exports them and every importer routes through the vocabulary face.

### AUD-03 — records validation binds `BaseModel` where `ResponseModel` is the contract
- **Location:** `src/fleetpull/records/validation.py:21`,
  `records/convert.py:26–28`.
- **Evidence (read):** `validate_records[ModelT: BaseModel]` and
  `models_to_dataframe(records: Sequence[BaseModel], model_class:
  type[BaseModel])` accept any Pydantic model; the package contract is
  `ResponseModel` (frozen, `extra='ignore'`, `populate_by_name` — the config
  policy `model_contract/` exists to carry). Nothing breaks today; the cost
  is that the type system permits feeding the records stage a model that
  skipped the response-model policy, and the public `fetch` should promise
  frames derived from `ResponseModel`-conforming models only.
- **Verdict:** **cosmetic** (tighten the bound; `records` already sits above
  `model_contract` in the vertical, so the import is legal).
- **Owner:** item 5 wave.
- **Resolution (2026-07-06):** fixed in audit wave 1 — `validate_records` and `models_to_dataframe` now bind `ResponseModel`; every test fixture already conformed.

### AUD-04 — the retained import-linter contract's name no longer states its purpose
- **Location:** `pyproject.toml:282` — `name = "Endpoints sit above the
  contracts and carriers they compose"`.
- **Evidence (read):** the contract inventory is 8 (verified); this one is
  retained because its middle tier (`models | network.contract |
  incremental`) enforces same-tier independence — notably `models` ⊥
  `incremental` — that the coarse package vertical cannot express (recorded
  when the comprehensive contract landed). The name says "endpoints sit
  above", which the vertical also enforces; the load-bearing part is the
  independence, which the name omits.
- **Consumer cost:** none at runtime; a future maintainer reading a broken
  contract by its name will mis-locate the invariant.
- **Verdict:** **cosmetic** — rename to state the independence purpose.
- **Owner:** any next `pyproject.toml`-touching prompt; item 5 wave at the
  latest.
- **Resolution (2026-07-06):** fixed in audit wave 1 — renamed to "Models, the network contract, and incremental are independent same-tier carriers"; body unchanged.

### AUD-05 — roster registration is hand-listed while endpoints are discovered
- **Location:** `scripts/run_vehicle_locations.py:307`
  (`RosterRegistry([VEHICLE_IDS_ROSTER])`) vs
  `endpoints/registry.py:build_endpoint_registry` (the walk).
- **Evidence (read):** endpoint composition is discovery-driven (one leaf
  module = one endpoint, nothing to register); roster composition requires
  the composition root to import each declaration from its provider leaf and
  list it explicitly. For the script (one roster) this is one line; for
  sync's YAML-driven composition it is a hand-maintained parallel list that
  will drift from the provider leaves as rosters multiply — the exact
  manifest-maintenance failure the endpoint walk was built to avoid.
- **Verdict:** **blocks-the-API** (sync, item 6 — fetch touches no roster).
  The fix need not be a second discovery walk (the endpoint walk is the
  single sanctioned import-discipline exception); an explicit per-provider
  export (`MOTIVE_ROSTERS: tuple[RosterDefinition, ...]`) aggregated at one
  composition point, plus a parity test against declarations, preserves
  explicit construction while removing per-root hand-listing.
- **Owner:** item 6.

### AUD-06 — nothing constrains a roster's feeder to snapshot mode at declaration time
- **Location:** `roster/definition.py` (no mode field or check);
  `roster/registry.py` (no validation); the guarded route is
  `orchestrator/roster_refresh.py:refresh_if_stale` (isinstance
  `SnapshotMode` check, read); the **unguarded** route is the entry tap:
  `orchestrator/entry.py:run_endpoint` → `sourced_by` → observe →
  `apply_listing`, which reconciles whatever an `Executed` run of the
  `source_endpoint` listed, with no mode check (read).
- **Evidence (read):** `reconcile` is only correct over a *complete*
  listing. A `RosterDefinition` whose `source_endpoint` names a
  watermark-mode endpoint would fail loudly on the coordinator's harvest
  route — but on the tap route, every windowed run of that feeder would
  reconcile a *partial* listing, mass-incrementing absence counts for every
  member outside the window and (with an eviction threshold) evicting live
  members within a few runs. No current declaration triggers this
  (`VEHICLE_IDS_ROSTER` sources the snapshot `vehicles`), so it is latent.
- **Verdict:** **correctness-independent** — a real defect class independent
  of the API, currently unreachable, and it must be closed before item 6
  lets YAML declare rosters. Cheapest closure: validate feeder mode at
  registration (the registry already resolves nothing — the check belongs
  where a definition meets the endpoint catalog: the entry tap and/or the
  coordinator's registration seam).
- **Owner:** its own small fix prompt, scheduled before item 6.
- **Resolution (2026-07-06):** fixed in audit wave 1 — the entry tap rejects a non-snapshot sourced definition before anything runs (mirroring the coordinator's harvest guard), and `tests/endpoints/test_roster_discipline.py` enforces the rule at declaration level; both guards plant-and-fire proven with permanent negative-shape tests.

### AUD-07 — script comment drift: the `LOOKBACK_DAYS` "modest window" promise
- **Location:** `scripts/run_vehicle_locations.py:93–96`.
- **Evidence (read):** the comment promises a small lookback "keeps Run 2's
  resume window similarly modest", which holds only under the unstated
  assumption that Run 2 resumes the same day as Run 1's watermark: the
  floored window is `[floor(watermark − lookback), trailing_edge)`, so a
  watermark several days old widens Run 2's pull regardless of
  `LOOKBACK_DAYS`.
- **Verdict:** **cosmetic**.
- **Owner:** the next script-touching prompt.
- **Resolution (2026-07-06):** fixed in audit wave 1 — the comment now states the window is `[floor(watermark − lookback), trailing_edge)`, so lookback bounds the margin, not the window.

### AUD-08 — script comment drift: `DATASET_ROOT` still claims a cold-start-only proof
- **Location:** `scripts/run_vehicle_locations.py:76–79`.
- **Evidence (read):** "Must be a fresh/empty directory -- this script
  proves the *cold* backfill arm, which assumes no prior state." The script
  has deliberately reused state since the resume/floored-window work: Run 2
  exists to exercise resume, and the live diagnostic ran against retained
  state. The fresh-directory requirement is now wrong as stated.
- **Verdict:** **cosmetic**.
- **Owner:** the next script-touching prompt.
- **Resolution (2026-07-06):** fixed in audit wave 1 — the comment now records deliberate state retention (cold start on an empty directory, resume thereafter).

### AUD-09 — OneDrive/AV staging-clear robustness (note-only)
- **Location:** `storage/staging.py:clear_partition_staging`
  (`shutil.rmtree` → `PermissionError`/`WinError 5` on synced/scanned
  filesystems).
- **Evidence:** recorded in DESIGN §13 with its intended fix
  (retry-then-warn) and regression-test shape; owned by the polish phase
  (§15 item 8).
- **Verdict:** **leave-alone** here — already parked with an owner; recorded
  per the prompt, not re-litigated.

### AUD-10 — roster policy values are declaration-hardcoded
- **Location:** `endpoints/motive/vehicles.py` — `_VEHICLE_IDS_MAX_AGE =
  timedelta(days=1)`, `_VEHICLE_IDS_EVICTION_THRESHOLD = 3` (read).
- **Evidence (read):** the values are named module constants with recorded
  rationale, consumed only through `VEHICLE_IDS_ROSTER`. No configuration
  path reaches them.
- **Verdict:** **leave-alone** as declared constants for now — they are
  declaration facts in the same family as `quota_scope`, and no operator
  need has surfaced. If item 6's schema grows a roster section, the
  candidate keys are `rosters.vehicle_ids.max_age` /
  `.eviction_threshold`; decide there, not preemptively.

### AUD-11 — `backfill_chunk` has no config field
- **Location:** `orchestrator/backfill.py:plan_backfill_units(…, chunk)`
  (read); no corresponding field on `SyncConfig` (read — its fields are
  exactly `default_start_date`, `dataset_root`).
- **Evidence (read):** the planner takes `chunk` as a parameter; the loop
  that would supply it is unbuilt (deferred, §15).
- **Verdict:** **leave-alone** until the backfill loop lands; the seed key
  for item 6 is `sync.backfill_chunk_days` (whole days, per the planner's
  guard).

### AUD-12 — rate-limit values have no config home
- **Location:** `scripts/run_vehicle_locations.py:103–105` (placeholder
  values, script-local); `network/limits/config.py` (`RateLimitConfig`,
  read); DESIGN §11 (migration to `config/` already planned).
- **Evidence (read):** every composition root must invent
  `RateLimitConfig` values; nothing in `config/` carries them; `fetch`
  cannot ask its caller for a limiter registry.
- **Verdict:** **blocks-the-API** (fetch, item 5): `fetch` must ship
  internal per-provider defaults, and the value's config home
  (`rate_limits.<quota_scope>`, or a per-provider section) must be settled
  no later than item 6. This is the already-planned `RateLimitConfig`
  migration finally acquiring its deadline.
- **Owner:** item 5 (internal defaults) + item 6 (the YAML key).
- **Resolution (2026-07-06):** the item-5 half landed in audit wave 1 — `RateLimitConfig` migrated to `config/rate_limit.py` (the DESIGN §11 planned move, executed early), `MotiveConfig.rate_limit` carries the documented conservative default, and `rate_limits_from_configs` derives the registry map so no composition root invents numbers; the YAML key shape remains item 6's. Inventory row 22's remedy landed alongside: `ClientRuntime` defaults `random_source`/`sleeper` to the production implementations (both still injectable), and both scripts shed the obsolete wiring.

### AUD-13 — the state DB path convention exists only as a script expression
- **Location:** `scripts/run_vehicle_locations.py:168`; DESIGN §5 promises
  `state.database_path` defaulting to `<dataset_root>/.fleetpull/state.sqlite3`;
  `SyncConfig` has no such field (read).
- **Evidence (read):** the convention DESIGN §5 documents as config-backed
  is hand-encoded in the diagnostic; sync cannot honor "SQLite stays on
  local disk when parquet sits on a network filesystem" (§5's stated reason
  for separability) without the field.
- **Verdict:** **blocks-the-API** (sync, item 6). Seed key:
  `state.database_path` with the §5 default.
- **Owner:** item 6.

### AUD-14 — stdlib tripwire exceptions escape the `FleetpullError` promise on the sync path
- **Location (read):** `storage/writers.py` window tripwire (`ValueError`),
  `timing/canon.py` guards (`ValueError`), `incremental/window.py`
  (`ValueError`), `endpoints/shared/url_paths.py:35`
  (`UrlPathTemplateError(ValueError)`), `records/fields.py` /
  `records/event_time.py` (`TypeError`), `records/roster_members.py`
  (`ValueError`).
- **Evidence:** these are require-inside bug tripwires by doctrine — a
  strict interior failure means a fleetpull bug, and wrapping them in
  `FleetpullError` would blunt exactly the loud-failure design the
  enforcement machinery exists for. §10's promise governs operational
  failures; a crashed tripwire is not an operational outcome a consumer
  should handle.
- **One exception inside the group (read):** `url_paths.py:79–80` raises on
  an *empty-string* path value. Roster members come from provider data
  (`extract_roster_members` rejects nulls but stringifies whatever else the
  feeder listed — read), so a provider listing an empty-string id would
  crash a fan-out run with a bare `ValueError` — operational data reaching a
  wiring-bug tripwire. Latent; no such id has been observed.
- **Verdict:** **leave-alone** for the tripwire class (by design); the
  empty-member edge is flagged to the item-6/exception-contract owner as a
  boundary-validation candidate (reject or skip empty members at the roster
  ingress, where the coordinator already owns policy).
- **Resolution (2026-07-06, the empty-member edge only):** `extract_roster_members` now filters null and empty-string values loudly (a warning with the column and counts) instead of raising on nulls or passing empties through to unbuildable URLs — the two garbage shapes reconciled deliberately to one filtering behavior. The tripwire class itself remains leave-alone by design.

### AUD-15 — protocol inventory: seventeen Protocols, all earning their keep
- **Location (read):** 17 `Protocol` classes across `orchestrator` (9),
  `network` (4), `storage` (1), `timing` (2), `endpoints.shared` (1);
  `ClientSource` deliberately duplicated in `runner.py` and
  `roster_refresh.py`.
- **Evidence:** every orchestrator Protocol has exactly one production
  consumer — and a fake behind every unit test of its module
  (`test_runner.py`, `test_roster_refresh.py`, `test_entry.py` construct no
  SQLite, no HTTP). The cost (one indirection layer per collaborator) buys
  the entire fast-test surface plus module independence (the duplicated
  `ClientSource` is a recorded decision, not an accident). The
  `polars_typing` package similarly has a single consumer
  (`storage/atomic.py`, read) and exists to quarantine a private
  `polars._typing` import — the documented purpose.
- **Verdict:** **leave-alone** — this is the enforcement-machinery-is-the-
  product case: the indirection is what the tests and the layering contracts
  stand on. No single-consumer Protocol was found whose removal would not
  cost either test isolation or a layering rule.

### AUD-16 — `runs.row_count` now carries two semantics
- **Location (read):** `state/run_ledger.py` (`row_count` = records fetched,
  per the runner: `runner.py` passes `records_fetched`);
  `orchestrator/roster_refresh.py` (`complete_run(run_id,
  row_count=len(listed))` — distinct-member count, acknowledged in a call-
  site comment). Also the naming seam: the same quantity is
  `Executed.records_fetched` in the outcome and `row_count` in the ledger.
- **Evidence:** a `runs` reader cannot interpret `row_count` without knowing
  whether the row was a runner execution or a coordinator harvest; the
  column is informational (no resume logic reads it), so the ambiguity is
  diagnostic-only.
- **Verdict:** **cosmetic** — either record the harvest's fetched-record
  count (requires threading a count out of `harvest_roster_members`) or
  document the dual semantics in the DDL comment/`RunLedger` docstring.
- **Owner:** item 6 wave (which touches the ledger for sync reporting) or
  polish.

### Part E sweep — inspected and found consistent (no finding)

Traced across code, DDL, SQL, parameters, docstrings, and DESIGN (read):
`absence_count` (DDL `state/migrations.py:190` = store docstrings = 
`read_counts` mapping), `eviction_threshold` (`RosterDefinition` =
`reconcile` parameter), `source_endpoint`/`source_column`, `member`,
`dataset_root`, `default_start_date`, `start_date`/`end_date` (wire = spec
builder = §10 vocabulary binding), `located_at`, `quota_scope`,
`vehicle_id`-vs-wire-`id` (an intentional, documented alias at the model
boundary, not drift). The one drift-class finding from this sweep is AUD-16.

---

## Closing

### The item-5 build map (from Part B)

`fetch(endpoint_identity, auth=…)` composes, in order, with no state
machinery: `MotiveConfig`-family provider config (defaults + the verb's few
arguments) → the endpoint definition (via the identity's `(provider, name)`
against `build_endpoint_registry`) → `ProviderProfile` from the auth ingress
(provider → header/classifier map; AUD-01's parse fix lands here too) →
`ClientRuntime` with internal defaults (`HttpConfig`, `RetryConfig`,
`RateLimiterRegistry` with per-provider default limits — AUD-12;
`random`/`sleeper` defaulted internally — row 22) → `TransportClient` →
`spec_builder.build_spec(resume=None, path_values={})` →
`fetch_pages` → `validate_records` → `models_to_dataframe`. Plus the
`Endpoints` catalog module, the snapshot-typed identities, and the two-way
parity test (§10). Nothing else.

### The item-6 schema seed (the config-knob column)

| Candidate key | Source | Exists today? |
|---|---|---|
| `providers.motive.api_key` (the auth credential family) | row 1 | no (auth config surface) |
| `providers.motive.base_url` | row 4 | `MotiveConfig.base_url` |
| `providers.motive.records_per_page` | (snapshot script) | `MotiveConfig.records_per_page` |
| `providers.motive.lookback_days` / `.cutoff_days` | row 6 | `MotiveConfig` |
| `http.use_truststore` (+ timeouts) | row 3 | `HttpConfig` |
| `retry.*` | row 21 | `RetryConfig` (defaults) |
| `rate_limits.<quota_scope>` | row 7, AUD-12 | **no** — script-local values |
| `storage.dataset_root` | row 2 | `SyncConfig.dataset_root` |
| `sync.default_start_date` | row 24 | `SyncConfig.default_start_date` |
| `state.database_path` | row 16, AUD-13 | **no** — script expression only |
| `sync.backfill_chunk_days` | AUD-11 | **no** — parameter with no caller |
| (`rosters.*` policy — only if item 6 wants it) | AUD-10 | declared constants |

### Verdict tally

blocks-the-API: **4** (AUD-01 fetch/item 5; AUD-12 fetch/item 5 + item 6;
AUD-05, AUD-13 sync/item 6) · correctness-independent: **1** (AUD-06) ·
cosmetic: **6** (AUD-02, AUD-03, AUD-04, AUD-07, AUD-08, AUD-16) ·
leave-alone: **5** (AUD-09, AUD-10, AUD-11, AUD-14, AUD-15).
