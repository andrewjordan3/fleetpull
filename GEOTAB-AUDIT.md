# GeoTab Readiness Audit

Date: 2026-07-09. Report only — nothing in the tree was changed. Every claim
cites `file:line` against the current tree (branch
`claude/per-provider-executor-og4n1o`, after the unit-loop vertical).

**Verification legend.** *Captured* = verified against live GeoTab responses
(the June 2026 scrubbed captures; provenance markers appear in the source and
fixtures themselves). *Documented-only* = modeled from provider docs or
inference, never observed on the wire. The house rule already demands the
distinction: "encode probed provider behavior, never documented behavior
alone" (DESIGN.md:1022–1028).

Findings are numbered `GTA-xx` and indexed at the end.

**Update doctrine.** This is a point-in-time report and is never silently
rewritten: changes land as dated appended notes under the affected entries
(`**Update (2026-07-09):** …`-style), mirroring AUDIT.md's resolution-line
doctrine. Original text stands as written on its date.

---

## 1. Built inventory

### 1.1 Auth stack — complete, tested, and entirely unwired

| Artifact | Where | State | Basis |
|---|---|---|---|
| `GeotabAuthConfig` (username, password `SecretStr`, database, server w/ bare-hostname validator) | `src/fleetpull/config/geotab.py:18-70` | Complete; exported from the config face (`config/__init__.py:5,17`) | n/a (config) |
| `AuthenticationResult`, `GeotabSession` (id, resolved host, generation, acquired-at) | `network/auth/models.py:16-57` | Complete | n/a (carriers) |
| `build_geotab_authenticator` — the real `Authenticate` POST to `https://{server}/apiv1`, slice-model envelope parsing, `ThisServer` host resolution, dedicated quota-scope slot | `network/auth/authenticate.py:222-289` | Complete | **Captured**: outcomes arrive in HTTP 200 "per verification" (authenticate.py:194-197); `InvalidUserException` → `AuthenticationError` (147-148). **Documented-only**: a real redirect (`path != 'ThisServer'`) — "no capture shows one" (authenticate.py:168-170) |
| `GeotabSessionManager` — single-flight cache, generation counter, reactive `invalidate` + proactive refresh | `network/auth/manager.py:44-223` | Complete | **Documented-only**: the 14-day lifetime and 100-session LRU cap ("Documented GeoTab policy, not a server-returned contract", manager.py:32-35; no `expires_in` in the response) |
| `GeotabSessionAuth` — injects `credentials` into JSON-RPC `params`, retargets the URL to the session host, thread-local failure attribution, one-retry `on_auth_failure` | `network/auth/strategies.py:66-169` | Complete | Behavior contracts captured via the classifier (below); the strategy itself is fleetpull logic |
| Unit tests for all of the above | `tests/network/auth/` (test_authenticate, test_manager, test_strategies, test_models) | Complete; fixtures "synthetic, in the captured Authenticate shapes" (test_authenticate.py:1-7) | Mixed (see legend per fixture) |

**GTA-01 — the auth stack has zero production consumers.** Nothing outside
`network/auth/` constructs `GeotabSessionManager`, `GeotabSessionAuth`, or
`build_geotab_authenticator` (grep: only `network/auth/` itself plus a
docstring mention at `network/contract/auth.py:5`). The api-ingress GeoTab arm
raises instead of building a profile (auth_ingress.py:90-99). Built and
live-shaped in June; never composed.

### 1.2 Classifier and the captured fixture set

`GeotabResponseClassifier` (`network/classifiers/geotab.py:99-145`) is
complete: 5xx → TRANSIENT before parsing (109-114), non-JSON → FATAL
(116-127), envelope-driven otherwise — `error.data.type` is the sole
discriminator (2-13), unknown types fail loud (87-96). Wired into nothing
(the ingress never selects it; contrast `MotiveResponseClassifier` at
auth_ingress.py:88).

Fixture inventory (`tests/network/classifiers/test_geotab.py:12-88`), verbatim
markers from the file:

| Fixture | Marker | What it proves |
|---|---|---|
| `Authenticate` success (`credentials` + `path: "ThisServer"`) | **Captured** (13-17) | Success envelope shape; sessionId location |
| Invalid session on a data call, HTTP 200 | **Captured** (19-27) | `InvalidUserException` for dead sessions |
| Bad credentials on Authenticate, HTTP 200, same `type` | **Captured** (29-39) | The type collision that forces contextual disambiguation (classifiers/geotab.py:70-76; DESIGN.md:1011) |
| `OverLimitException`, HTTP 200, paired header `retry-after: 56`, message "10 per 1m" | **Captured** (41-49) | RATE_LIMITED + integer Retry-After (DESIGN.md:1012); the fixed 10/min Authenticate budget (authenticate.py:233-234) |
| `GetFeed` success — `{"data": [], "toVersion": "00000000034561f1"}` (trimmed) | **Captured** (51-54) | Feed result envelope; `toVersion` is a string (DESIGN.md:1014) |
| Load-balancer HTML page with 4xx | **Captured** (76-81) | The non-JSON FATAL branch |
| `DbUnavailableException` → TRANSIENT | **CONSTRUCTED** (56-64); classifier comment "Documented GeoTab transient" (geotab.py:81-82) | **Documented-only** — never observed live (GTA-08) |
| Unknown exception type; missing `data.type` | **CONSTRUCTED** (66-74, 83-88) | Negative shapes, fleetpull-side |

Also captured but deliberately unconsumed: success responses carry
`X-Rate-Limit-*` budget headers (DESIGN.md:1013 — feed-forward loop rejected
for v1).

### 1.3 Decoders

| Artifact | Where | State | Basis |
|---|---|---|---|
| `GeotabFeedPageDecoder` — `result.data` records; `toVersion` on **every** page incl. terminal; advance strips `search`, sets `fromVersion`; `resultsLimit` read from the sent body; terminal when `len(data) < resultsLimit` | `network/decoders/geotab.py:92-153` | Complete, tested (`tests/network/decoders/test_geotab.py`) | Mixed. **Captured**: envelope shape (June fixture above); `fromVersion` sent with `search` stripped — "verified: the API accepts `fromVersion` alone, and tolerates both" (DESIGN.md:963-965); `search.fromDate` historical bootstrap works (DESIGN.md:1014). **Documented-only**: the page-boundary rule itself — decoder tests are "synthetic, constructed in the verified GetFeed shapes" (test_geotab.py:1-5); no multi-page live sequence was ever captured (GTA-09) |
| `SinglePageDecoder` — top-level-list records under one key, always terminal | `network/decoders/single_page.py:22-55` | Complete (built for Motive vehicle_locations) | Candidate for GeoTab `Get` **if** a `Get` result is a plain object list under top-level `result` and single-page — both unverified (GTA-06); `require_record_list` demands exactly that shape (network/contract/envelopes.py:78-109) |

No `Get` decoder exists beyond that candidacy; no GeoTab response models exist
(`src/fleetpull/models/` holds only `motive`).

### 1.4 Vocabulary, cursors, state

| Artifact | Where | State |
|---|---|---|
| `Provider.GEOTAB` | `vocabulary/provider.py:27` | Present |
| `QuotaScope.GEOTAB` | `vocabulary/quota_scope.py:37` | Present; the dedicated Authenticate scope is deliberately **not** a member — "named at the composition root" (quota_scope.py:14-15) — but no composition root names it anywhere (GTA-05) |
| `FeedMode` — config-free marker; append write semantic documented on it | `endpoints/shared/base.py:115-127` | Present; a `SyncMode` member (base.py:133); `ResumeValue` already includes `FeedToken` (base.py:142) |
| `FeedToken` cursor | `incremental/cursor.py:52-63` | Present |
| `FeedToken` persistence — `CursorKind.FEED_TOKEN`, serialize + parse both directions | `state/cursors.py:51,76-77,134-135` | **Complete.** The state layer is feed-ready today |
| Runner feed arm | `orchestrator/runner.py:288-291` | **Stub**: `case FeedMode(): raise NotImplementedError` |
| Feed writer cells (single + partitioned append) | `storage/writers.py:429-434` ("the single-file watermark cell and the feed cells are not built"), module docstring writers.py:21 | **Absent** |
| `durable_progress` delivery to a consumer | `orchestrator/streaming.py:10-15` — the non-feed pipe "drops `durable_progress` … so the feed arm drives its own when built" | **Absent by design** |

### 1.5 Ingress, catalog, config plumbing

| Artifact | Where | State |
|---|---|---|
| `AuthInput` union accepts `Mapping[str, str] \| GeotabAuthConfig` | `api/auth_ingress.py:40-45` | Signature-ready |
| Ingress GeoTab arm | `api/auth_ingress.py:90-99` | **Stub**: raises `ConfigurationError('provider has no exposed endpoints')` |
| Catalog | `api/catalog.py:25-51` | Motive-only; no `Endpoints.Geotab` namespace |
| Parity test's FeedMode arm | `tests/api/test_catalog.py:25-41` | Deliberately unmapped: `FeedMode()` → `None`, so the first feed endpoint **forces** the identity-naming decision loudly rather than silently bucketing as `WindowedEndpoint` |
| Provider config section | `config/providers.py:159-172` — `ProvidersConfig` has only `motive` | **`GeotabConfig` does not exist — confirmed.** Only `GeotabAuthConfig` (credentials) exists, and nothing loads it from YAML (GTA-02) |
| Credential env-var fallback | `config/providers.py:42` — `{'motive': 'MOTIVE_API_KEY'}` | Motive-only |
| `Sync` enablement/selection/profile chain | `api/sync.py:219-229` (`-> list[tuple[Provider, MotiveConfig]]`), `api/sync.py:242+`, `_provider_profiles` walks the same list | Typed Motive-concrete; docstring says "Samsara/GeoTab widen this as they port" (GTA-03) |
| `config.example.yaml` | no `geotab` key anywhere | Absent |
| Endpoint discovery walk | `endpoints/registry.py:10-33` | Generic: a new `endpoints/geotab/` package with `build_endpoint(config: GeotabConfig)` leaves is discovered automatically once a `GeotabConfig` instance is in the supplied configs; the structural contract test (`tests/endpoints/test_endpoint_contract.py`) guards factory shape |
| Transport POST + JSON body | `network/contract/request.py:29-30` (`HttpMethod.POST`), `request.py:52-56` (`json_body`, docstring names "the GeoTab JSON-RPC envelope"), `network/client/transport.py:279-287` (`request(method=…, json=prepared.json_body)`) | **Wired end to end in the transport** — the load-bearing unknown from the prompt is closed in code. Never exercised through `fetch_pages` by any real endpoint (the authenticator POSTs through its own throwaway client, authenticate.py:282-287); decoder tests construct POST specs but never drive the transport |

### 1.6 DESIGN's recorded GeoTab ground truth

The verified-behaviors table (DESIGN.md:1006-1014) carries five GeoTab rows,
all June-captured, matching the code above. The feed storage doctrine —
active vs calculated feeds, append-only stores every emitted version, the
consumer reconciles by `(id, max version)` — is settled at DESIGN.md:393-401,
with the tombstone question explicitly open at DESIGN.md:403-411. §13 keeps
the umbrella open item: "GeoTab specifics pending API access: `GetFeed`
semantics in practice, real rate limits, which entities map to which storage
strategies (the auth model is settled — session-based, §8)" (DESIGN.md:1641).

---

## 2. Gap inventory

Sizing: S = hours, M = a focused prompt, L = multiple prompts. Dependencies
in brackets.

### 2.0 Shared foundation (both paths need all of it)

| # | Gap | Size | Notes |
|---|---|---|---|
| G1 | **`GeotabConfig` provider section**: endpoints list, `rate_limit` default (real numbers unknown — DESIGN.md:1641), auth fields (likely nesting/absorbing `GeotabAuthConfig`), `quota_scope` ClassVar, `ProvidersConfig.geotab`, resolution fan-in, example YAML. Env-var fallback design is open: the credential is four fields, not one string (`PROVIDER_CREDENTIAL_ENV_VARS` assumes one var per provider, providers.py:42) | **M** | Blocks nearly everything |
| G2 | **`Sync` enablement widening**: `_enabled_providers` / `_validated_selection` / `_provider_profiles` are Motive-concrete (sync.py:219-260) | **M** [G1] | Mechanical but touches the composition root and its tests |
| G3 | **Ingress GeoTab arm** (auth_ingress.py:90-99). Structural wrinkle, GTA-04: building the GeoTab profile needs `build_geotab_authenticator(http_config, limiter_registry, quota_scope)` (authenticate.py:222-226) — collaborators `build_provider_profile(endpoint, auth)` does not receive. Motive's profile is buildable from the credential alone; GeoTab's is not. The seam must grow or GeoTab profile construction must move to where `ClientRuntime` lives | **M** (S code, M design decision) [G1] |
| G4 | **Authenticate quota-scope registration**: the dedicated 10/min scope (captured: "Maximum admitted 10 per 1m", test_geotab.py:43-44) has no registry entry mechanism — `rate_limits_from_configs` emits provider scopes only (network/limits/registry.py:16-33) | **S** [G3] |
| G5 | **`endpoints/geotab/` package** + first leaf (`build_endpoint` factory, JSON-RPC spec builder). Discovery is free (registry.py:10-16); DESIGN already slots the package (DESIGN.md:1337,1354) | **S** per endpoint [G1, capture] |
| G6 | **Catalog namespace** `Endpoints.Geotab` + identity + two-way parity | **S** [G5] |
| G7 | **`models/geotab/` response model** for the first entity — written from captured JSON, not docs (the standing rule, DESIGN.md:1022-1028). No capture of a full `Device` or non-empty feed record exists in the tree | **S–M per entity** [Postman P3/P5/P8] |
| G8 | **Transport POST end-to-end proof**: wired (§1.5) but never driven through `fetch_pages` → limiter → classifier with a live body | **S** (falls out of the first endpoint) [G5] |

**Update (2026-07-09):** G7's capture dependency is closed — the probe
session captured full `Device` records (three polymorphic shapes, including
the trailer shape) and data-bearing `LogRecord` feed pages, so both first
models can now be written from captured JSON; the build work itself remains.
Sizing unchanged, with one addition from the Device polymorphism finding
(GTA-13): the model is the union of observed fields with everything
optional, and the year-one sentinel datetimes need microsecond-precision
handling or exclusion.

**Update (2026-07-09, devices build):** G1–G8 are all closed by the devices
vertical. G1: `GeotabConfig` in `config/providers.py` (nested
`GeotabAuthConfig`, both method-class budgets; env fallback settled as
password-only — `GEOTAB_PASSWORD` fills the one secret field, the other
three always come from YAML). G2: `Sync` widened on the concrete union
(`MotiveConfig | GeotabConfig`). G3: the ingress arm is real behind the
`ProviderProfileContext` seam (GTA-04's resolution). G4:
`rate_limits_from_configs` emits `GEOTAB_AUTHENTICATE` from any supplied
`GeotabConfig`. G5/G6: `endpoints/geotab/devices.py` +
`Endpoints.Geotab.devices`, parity-tested. G7: `models/geotab/device.py`,
the union-of-shapes model over the committed captures. G8: the e2e tests
drive `fetch` and `Sync` through MockTransport over the captured envelopes
— auth stack, transport POST, classifier, seek decoder, and completeness
guard composed for the first time.

### 2.1 Snapshot path (`Get`, e.g. `typeName=Device`) — additional

| # | Gap | Size | Notes |
|---|---|---|---|
| G9 | `Get` spec builder (JSON-RPC body: `method: "Get"`, `params.typeName`; credentials injected by the strategy, strategies.py:117-137) | **S** [G5] |
| G10 | `Get` result decoder: possibly zero new code — `SinglePageDecoder(records_key='result')` fits **iff** the probe confirms a plain object list under top-level `result` and single-page semantics (GTA-06). Otherwise a tiny GeoTab Get decoder | **S** [Postman P3/P4] |
| G11 | Writer: **no gap** — `SnapshotWriter` exists (writers.py:176-193); snapshot arm, `fetch` exposure, and the run path all exist. No roster needed | — |

Snapshot total beyond the shared foundation: **S**.

**Update (2026-07-09) — G10 REVERSED.** `SinglePageDecoder` does **not** fit
GeoTab `Get`: the probe found a silent hard cap at 5,000 records with no
continuation signal of any kind (a single-page decode would silently truncate
a 5,666-device fleet — GTA-11). Replaced by two gaps, per the probe-settled
decisions (DESIGN §8, 2026-07-09): a **seek-paging Get decoder** —
`sort.sortBy: "id"` ascending, `offset` = last returned id, terminal on the
empty page, `lastId` never sent (**S–M**) — and the **`GetCountOf`
completeness guard** beside every Device harvest — one refetch on mismatch,
persistent mismatch raises `ProviderResponseError` naming both counts
(**S**). Snapshot total beyond the shared foundation becomes **S–M**.

**Update (2026-07-09, devices build):** the snapshot path is closed. G9: the
JSON-RPC spec builder lives in the devices leaf. G10's replacements shipped:
`GeotabGetPageDecoder` (`network/decoders/geotab.py`, boundary-capture
tested) and `GetCountOfCheck` behind the new
`EndpointDefinition.completeness_check` declaration, honored by the
single-fetch driver (both verbs). G11 held as stated: `SnapshotWriter`
carried the writes with no gap.

### 2.2 Feed path (`GetFeed`) — additional

| # | Gap | Size | Notes |
|---|---|---|---|
| G12 | **FeedMode identity naming** — the parity test's deliberately unmapped arm forces it (test_catalog.py:25-41): is a feed identity `WindowedEndpoint`, or a third type? | **S** (decision + mapping) [G6] |
| G13 | **The feed runner arm**: resume from the stored `FeedToken` (persistence exists, cursors.py:76-77,134-135; `resolve_watermark_start` rejects it on watermark endpoints, orchestrator/resume.py:79-84), a pipe that *keeps* `durable_progress` (streaming.py:10-15 drops it), token-commit crash ordering (parquet → token → ledger analogue), and the unit-loop question: the plan-and-drive loop is date-window-based (runner.py) — a version-token feed cannot tile into date units, so the feed arm is its own path beside it | **L** [G12] |
| G14 | **Feed writer cells** (append semantics; both `single` and `date_partitioned` cells raise today, writers.py:429-434). Calculated feeds append every emitted version by design (DESIGN.md:393-401) | **M** [G13 design] |
| G15 | **Storage-kind / event-time mapping per entity** (§13, DESIGN.md:1641): `DATE_PARTITIONED` requires an `event_time_column` (base.py:308-311) — which feed entities have one worth partitioning on is per-entity design | **S–M per entity** [Postman P5/P8] |
| G16 | **Consumer reconcile rule** `(id, max version)` is settled as the *consumer's* concern (DESIGN.md:400-401) — needs recording in consumer-facing docs when the first calculated feed ships; no fleetpull code | **S** |
| G17 | **Tombstone semantics** for calculated feeds — open empirical question (DESIGN.md:403-411); does not block append-only v1 but shapes consumer guidance | Probe [P8, plus long-lived observation] |

Feed total beyond the shared foundation: **L**.

---

## 3. Postman probe checklist

**Scrubbing convention — apply before any capture is shared or committed**
(the June pattern, test_geotab.py:13-54, plus CLAUDE.md Data Hygiene): session
ids → `SyntheticSessionId000001`-style; database → `exampledb`; usernames →
`user@example.com`; GUIDs → zero-GUIDs with a counter suffix
(`00000000-0000-0000-0000-000000000001`); version cursors → zero-padded
synthetic hex (`00000000034561f1` shows the length convention); device/entity
ids and names → `synthetic-…`; no real VINs, serials, coordinates, company or
driver names. Capture the **full** JSON envelope plus **all response headers**
(`retry-after` especially, and the `X-Rate-Limit-*` family) for every call.

All calls: `POST https://{server}/apiv1`, JSON body as shown,
`credentials` = `{"database": "<db>", "userName": "<user>", "sessionId": "<sid>"}`
from P1.

| # | Call | Body | Capture | Closes |
|---|---|---|---|---|
| P1 | `Authenticate` | `{"method": "Authenticate", "params": {"database": "<db>", "userName": "<user>", "password": "<pw>"}}` | Full envelope + headers. Note the `path` value verbatim | Re-confirms the June capture; a non-`ThisServer` `path` would be the first observed redirect (authenticate.py:168-170, documented-only today) |
| P2 | `Get` Device, no limit | `{"method": "Get", "params": {"typeName": "Device", "credentials": {…}}}` | Full envelope (result count + 2–3 full Device objects verbatim) + headers | **GTA-06 / G10**: is `result` a plain object list (SinglePageDecoder fits) or something else; **G7**: the Device field inventory the model is written from; whether an implicit server-side cap truncates large fleets |
| P3 | `Get` Device, `resultsLimit: 2` | same + `"resultsLimit": 2` in `params` | Full envelope + headers | Pagination/limit behavior on plain `Get`, observed not assumed: is there **any** continuation signal when truncated, or is `Get` hard-capped with no cursor? Decides whether the snapshot decoder can be single-page |
| P4 | `GetFeed` LogRecord, bootstrap | `{"method": "GetFeed", "params": {"typeName": "LogRecord", "resultsLimit": 5, "search": {"fromDate": "<recent ISO-8601 Z>"}, "credentials": {…}}}` | Full envelope (non-empty `data` — the June capture was trimmed to `[]`) + headers | **G7** (LogRecord record shape for the model); `toVersion` form on a data-bearing page; confirms `search.fromDate` bootstrap (DESIGN.md:1014) with records |
| P5 | `GetFeed` LogRecord, from token | same params but `"fromVersion": "<P4's toVersion>"`, **no** `search` | Full envelope | Token advance: records strictly after the token? overlap? (the decoder's advance semantics, geotab.py:139-152); re-confirms `fromVersion`-alone acceptance (DESIGN.md:963-965) |
| P6 | Repeat P5 until a short page | iterate `fromVersion` | The **last two** envelopes verbatim (the final full page and the terminal short/empty page), each with its `toVersion` | **GTA-09 / G13**: the page-boundary rule (`len(data) < resultsLimit` → terminal, geotab.py:133-138) observed across a real boundary; whether an exactly-full final page yields one extra empty page; whether `toVersion` still moves on an empty page |
| P7 | `GetFeed` Trip (calculated), bootstrap + one advance | as P4/P5 with `"typeName": "Trip"` | 2–3 full Trip objects verbatim; the `id` and `version` fields especially | **G15/G16/G17**: calculated-feed record shape; whether `version` is per-record and its form — the groundwork for the `(id, max version)` consumer rule and the tombstone question (DESIGN.md:393-411) |
| P8 | `Get` with a bogus `typeName` | `{"method": "Get", "params": {"typeName": "NotAThing", "credentials": {…}}}` | Full error envelope | Whether unmet error types share the captured envelope shape (`error.data.type` present — classifiers/geotab.py:53-60 falls to FATAL on a missing type); cheap insurance for the classifier's malformed-envelope branch |
| P9 *(optional — burns the auth quota)* | 11 rapid `Authenticate` calls | as P1 | The `OverLimitException` envelope **and** its `retry-after` header | Already captured June (`retry-after: 56`); re-run only if header re-confirmation is wanted. `DbUnavailableException` (GTA-08) cannot be provoked on demand — capture opportunistically if ever observed |

Postman cannot exercise the JSON-RPC-over-HTTP-200 behavior any other way, so
every capture should note the **HTTP status code** alongside the body — the
classifier's whole design rests on errors arriving in 200
(classifiers/geotab.py:4-7; DESIGN.md:1010).

**Update (2026-07-09) — probe session executed** (Andrew, live MyGeotab;
findings recorded in DESIGN §8's observed-behaviors table and probe-settled
decisions). Status per probe:

- **P1 ✓** — `path: "ThisServer"` re-captured; envelope byte-shape matches
  the June fixture. The redirect case remains documented-only.
- **P2 ✓** — closed by the cap discovery: `Get` hard-caps at 5,000
  silently; `GetCountOf` captured 5,666 against it (GTA-11).
- **P3 ✓** — `resultsLimit` honored exactly at/under the cap; above it,
  still exactly 5,000; no continuation signal exists on plain `Get`.
- **P4 ✓** — data-bearing `search.fromDate` bootstrap captured (LogRecord,
  six-field shape); the June empty-data fixture is explained: a bare
  `GetFeed` is cursor-at-now.
- **P5 ✓** — strict token advance captured: versions strictly after the
  sent `fromVersion`, no overlap; ids and `toVersion` share one counter
  space (the docs' `"b14C3EE"` ↔ `"000000000014c3ee"` pair).
- **P6 partial** — the short-page caught-up shape observed live (GTA-09's
  live half closed); the exactly-full-final-page edge is **accepted as a
  residual**, not left open — worst case one extra empty call per sync
  (DESIGN §13, 2026-07-09).
- **P7 deferred** — explicitly deferred to Trip's port: LogRecord is a raw
  immutable feed, so the calculated-feed questions (version re-emission,
  tombstones — GTA-10) don't bite it.
- **P8 ✓** — plus a bonus capture: `MissingMethodException` whose message
  lies while `error.data.type` stays truthful (the
  read-the-type-never-the-message rule's strongest exhibit), and
  `JsonSerializerException` (code `-32700`) for a malformed body, same
  envelope shape.
- **P9 skipped** — the June `OverLimitException` capture stands.

**Fixture earmarks (2026-07-09)** — captures earmarked to become committed
test fixtures in the build prompts (earmarked only; the build prompts commit
them, scrubbed per this section's convention): the terminal empty feed page;
a seek page boundary (last id of page N + first id of page N+1); the
`MissingMethodException` and `JsonSerializerException` envelopes; the
data-bearing bootstrap LogRecord page; a trailer-shape Device record
(`deviceType: "None"`, `productId: -1`, `tmpTrailerId`).

---

## 4. Recommended build order

**Recommendation: snapshot-first (`Get` `typeName=Device`), with the feed
probes (P4–P7) executed in the same Postman session so feed design starts
from captured reality.** Input to a decision, not a decision.

Reasoning. The two paths share the entire foundation (G1–G8): config section,
Sync enablement, the ingress arm and its GTA-04 seam question, the auth quota
scope, the endpoints package, the catalog namespace, and the first live proof
of the auth stack + transport POST + classifier working end to end. The
snapshot path adds only S-sized work on top of that (a spec builder and
possibly zero decoder code), rides the existing `SnapshotWriter` and snapshot
runner arm unchanged, and doubles as DESIGN's own roadmap intent — "One
GeoTab endpoint end-to-end before bulk porting. GeoTab is the architectural
stress test (different auth, pagination, decode)" (DESIGN.md:2049-2052). Every
one of the June-built-but-unwired components (GTA-01) gets its first
production execution against the smallest possible amount of new machinery,
so a surprise (an ingress seam that has to move, a transport POST wrinkle, a
session-refresh edge) surfaces in an S-sized change, not inside an L-sized
feed arm.

Feed-first would land the production-pressure data sooner in the best case,
and the feed machinery is the better-verified half of the June work (decoder
+ captured envelope + token persistence). But it stacks the L-sized runner
arm (G13 — a new resume pipe, token crash-ordering, and a deliberate bypass
of the date-based unit loop), the append writer cells (G14), and the forced
identity-naming decision (G12) on top of the same unproven foundation — the
first live GeoTab request ever made by fleetpull would happen inside the
largest new subsystem, where an auth or transport surprise is most expensive
to localize.

What snapshot-first risks: the Device snapshot itself is scaffolding, not the
telemetry under production pressure — if the calendar is the binding
constraint, the feed arm starts one prompt later. It also bets that `Get` is
decoder-trivial (GTA-06); P2/P3 settle that before any code is written. What
feed-first risks: debugging session auth, POST transport, and classifier
behavior for the first time inside an L build; making the G12 naming and
G15 storage-mapping decisions before P4–P7 captures exist.

Either order, nothing should be built ahead of the Postman captures: the
tree's own standing rule is that bindings settle against probes, never docs
alone (DESIGN.md:1022-1028).

**Update (2026-07-09):** the probes ran; P2/P3 settled the GTA-06 bet
*against* decoder-triviality (see the G10 reversal in §2.1). The
snapshot-first recommendation stands: the seek-paging decoder plus the
`GetCountOf` guard move the snapshot delta from S to S–M, still well short
of the feed arm's L, and the captures the feed design needs (P4–P6) are now
in hand as this order assumed.

---

## Findings index

| # | Finding | Where |
|---|---|---|
| GTA-01 | The complete GeoTab auth stack (manager, strategy, authenticator) has zero production consumers — built and tested, never composed | §1.1; auth_ingress.py:90-99 |
| GTA-02 | `GeotabConfig` (provider section) does not exist; `GeotabAuthConfig` exists but is unreachable from YAML (`ProvidersConfig` is Motive-only) | config/providers.py:159-172; config/geotab.py:18 |
| GTA-03 | `Sync`'s enablement/selection/profile chain is typed Motive-concrete and must widen | api/sync.py:219-260 |
| GTA-04 | `build_provider_profile(endpoint, auth)` cannot construct GeoTab's profile: the authenticator needs `http_config` + `limiter_registry` + a quota scope the ingress does not receive — a seam decision, unresolved by design here | auth_ingress.py:52-99; authenticate.py:222-226 |
| GTA-05 | The dedicated Authenticate quota scope (captured 10/min) is "named at the composition root", but no composition root names it and no registry entry mechanism exists for non-provider scopes | quota_scope.py:14-15; network/limits/registry.py:16-33 |
| GTA-06 | `SinglePageDecoder(records_key='result')` may cover GeoTab `Get` with zero new code — the result shape and single-page behavior are unverified (routes to P2/P3) | single_page.py:22-55; envelopes.py:78-109 |
| GTA-07 | The feed path cannot ride the unit loop: the windowed arm is date-based; `FeedMode` raises in the runner, both feed writer cells raise, and the streaming pipe drops `durable_progress` by design | runner.py:288-291; writers.py:429-434; streaming.py:10-15 |
| GTA-08 | `DbUnavailableException` → TRANSIENT is documented-only (its fixture is marked CONSTRUCTED); unprovokable on demand — capture opportunistically | classifiers/geotab.py:81-86; test_geotab.py:56-64 |
| GTA-09 | The GetFeed page-boundary rule (`len(data) < resultsLimit` terminal) has never been observed across a live page boundary (decoder fixtures are synthetic) — routes to P6 | decoders/geotab.py:133-138; tests/network/decoders/test_geotab.py:1-5 |
| GTA-10 | Calculated-feed removal (tombstone) semantics are an open empirical question that shapes consumer guidance, not v1 storage — routes to P7 and longer-lived observation | DESIGN.md:403-411 |

**Update (2026-07-09) — resolutions and new findings from the live probe
session** (details in DESIGN §8's observed-behaviors table and probe-settled
decisions):

- GTA-06 — **resolved, reversed**: `SinglePageDecoder` does not fit `Get`
  (the silent 5,000 cap); see the G10 reversal, §2.1.
- GTA-09 — **half resolved**: the short-page caught-up shape was observed
  live; the exactly-full-final-page edge is accepted as a residual
  (DESIGN §13, 2026-07-09), no longer routed to probing.
- GTA-10 — **deferred, unchanged**: P7 deferred to Trip's port; LogRecord
  is a raw immutable feed the calculated-feed questions don't bite.

**Update (2026-07-09, devices build) — resolutions from the shipped
vertical:**

- GTA-01 — **resolved**: the auth stack has production consumers. The
  ingress GeoTab arm composes `build_geotab_authenticator` →
  `GeotabSessionManager` → `GeotabSessionAuth` behind
  `build_provider_profile`, and the e2e tests drive the composed stack
  through a mock transport on the captured envelopes (one Authenticate,
  session credentials on every data call).
- GTA-02 — **resolved**: `GeotabConfig` exists (`config/providers.py`),
  loads from YAML, and `ProvidersConfig.geotab` reaches it; the four-field
  credential nests as `auth`, with `GEOTAB_PASSWORD` filling only the
  password.
- GTA-03 — **resolved**: `Sync`'s enablement/selection/profile chain is
  typed on the concrete union `MotiveConfig | GeotabConfig`; Samsara
  remains the noted future widening.
- GTA-04 — **resolved**: the seam grew a third parameter —
  `ProviderProfileContext(http_config, limiter_registry, clock)` — the
  union of composition-root collaborators; `Sync` passes its shared clock,
  `fetch` constructs one at the profile-build site (DESIGN §10).
- GTA-05 — **resolved**: `QuotaScope.GEOTAB_AUTHENTICATE` exists and
  `rate_limits_from_configs` registers it from `GeotabConfig`'s
  `authenticate_rate_limit` wherever a `GeotabConfig` is supplied; the
  ingress names it to the authenticator factory.
- GTA-11 — **mitigated as designed**: the seek decoder + the `GetCountOf`
  completeness guard (probe-settled decisions 1–2) shipped; silent
  truncation on `Get` is now structurally loud.

| # | Finding (added 2026-07-09) | Where |
|---|---|---|
| GTA-11 | `Get` silently hard-caps at 5,000 records — no continuation signal or metadata; `GetCountOf` is the truth instrument (captured 5,666 vs the capped 5,000: 666 records invisible to a bare `Get`). Any uncursored `Get` harvest is silent-truncation-by-construction on large fleets | DESIGN §8 observed table (2026-07-09 rows); decisions 1–2 |
| GTA-12 | The feed is version-ordered, not event-time-ordered: `dateTime` non-monotonic within a page and dipping before the requested `fromDate`; bootstrap semantics are *ingested-since*, not *occurred-since*. Date watermarks must never derive from feed `dateTime`s — the empirical justification for `FeedToken` opacity | DESIGN §8 observed table; decision 4 |
| GTA-13 | Device schema polymorphism and the sentinel set: at least three record shapes (GO7-era, GO9-era, trailer with `deviceType: "None"` / `productId: -1` / `tmpTrailerId`); `activeTo: 2050-01-01` = active; VIN fields carry `""` and `"?"`; `ignoreDownloadsUntil` at `0001-01-01` overflows nanosecond timestamp columns (microsecond precision or exclusion required) | DESIGN §8 observed table |
| GTA-14 | Rate budgets are per method class, advertised in `X-Rate-Limit-*` headers on every response (GetFeed class ~60/min, multiple windows; Get class ~650/min, single datum; June's 10/min OverLimit consistent with an Authenticate-class budget) — with the docs' caveat that headers may precede enforcement ("Coming Soon"). Config defaults cite the captured values; no ramp probe needed | DESIGN §8 observed table; decision 3 |
