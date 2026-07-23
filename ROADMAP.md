# fleetpull — Roadmap

The consolidated, actionable index of deferred engineering work: the "what to
build next, and why" companion to `DESIGN.md`'s "how the built pieces fit." A
running run may surface a better idea (Think Before Coding, `CLAUDE.md`) — this
file is where the not-yet-built ideas wait with enough context to be picked up
cold.

**Relation to `DESIGN.md`.** `DESIGN.md` stays the design of record. The
*rationale* for each item below lives in the DESIGN section it governs — the
tensions register (§13), the build-status deferrals (§15), the rate-limiting
design (§7), and the sections each item cites. This file does not relocate or
restate that rationale; it aggregates the work items, sharpens each into
concrete steps, and points back. When an item ships, its DESIGN section updates
in the same change (the discipline `CLAUDE.md` requires), and its entry here is
struck.

**Relation to `ENDPOINTS.md`.** Endpoint coverage — the shipped inventory and
the port queue — is `ENDPOINTS.md`'s job and is **not duplicated here**. The
port queue is cross-referenced once, at the end.

**Not a scope-expansion list.** The hard scope boundaries (`CLAUDE.md`: no
cross-endpoint merging, no unified schema, no semantic dedup, no warehouse
loading, no assumed end use) hold over everything below. These are
implementation deferrals within the settled scope, never widenings of it.

---

## 1. Per-endpoint quota scopes for Samsara

**Why.** fleetpull models every Samsara endpoint under a single
`QuotaScope.SAMSARA`, budgeted at `requests_per_period=100, period_seconds=60`
— 100 requests per **minute** (`_SAMSARA_DEFAULT_RATE_LIMIT`,
`config/providers.py`, with its rationale comment directly above). That comment
is honest that the number is a deliberate conservative placeholder pending a
per-endpoint scope split; the finding below is that the placeholder is not just
conservative but categorically mismatched to the endpoints it governs.

100 req/min is Samsara's **"Level One"** rate-limit category, which across
Samsara's published rate-limit chart applies **only to POST / PATCH / DELETE
(writes)**. Every endpoint fleetpull calls is a read (GET), and none of them
belong to Level One. The real documented per-organization tiers for the GET
paths we ship (each path verified in `endpoints/samsara/*.py`) are far higher:

| Endpoint | Wire path | Documented tier | vs. configured 100/min |
|---|---|---|---|
| `trips` | `GET /v1/fleet/trips` | (Legacy) Tier 2 — **50 req/s** (3,000/min) | ~30× |
| `vehicles` | `GET /fleet/vehicles` | (Legacy) Tier 1 — **25 req/s** | ~15× |
| `drivers` | `GET /fleet/drivers` | Level Two — **5 req/s** | ~3× |
| `idling_events` | `GET /idling/events` | *not in the published chart* | governed by the global ceiling |

Global ceilings sit above the per-endpoint tiers: **150 req/s per token** and
**200 req/s per organization**. These are cross-endpoint caps on the whole
token/org, not per-endpoint budgets — see the design question below.

**Observed impact.** An 18-month utilization backfill's Samsara pulls took
~8h20m (`trips`) and ~8h54m (`idling_events`). The cause is self-inflicted: the
single shared 100/min bucket both throttled far below every endpoint's real
tier *and* forced two independent workloads — `trips` (a per-vehicle fan-out,
611 vehicles × windows) and `idling_events` (a fleet-wide windowed cursor walk)
— to contend for one bucket that Samsara itself would meter as separate
per-category budgets. Splitting the scope removes both the throttle and the
contention at once.

**What to do.**

- Add a `QuotaScope` member per Samsara metering category and pin each endpoint
  at its documented tier — the exact shape fleetpull **already ships for
  GeoTab**: `QuotaScope.GEOTAB_GET / GEOTAB_FEED / GEOTAB_AUTHENTICATE`
  (`vocabulary/quota_scope.py`) emitted from one `GeotabConfig` via
  `scope_rate_limits()` (`config/providers.py`). `SamsaraConfig.scope_rate_limits()`
  overrides the base one-scope emission the same way, so the limiter registry
  derives every Samsara scope from the config with no provider special-casing.
  This is a code-plus-config change (a new enum member's limits stay config),
  never config-only — exactly what §13 records for the pattern.
- Pin the tier constants from the table above: `trips` 50 req/s, `vehicles`
  25 req/s, `drivers` 5 req/s. Set each scope's `burst` and `max_concurrency`
  per tier as well — a per-second tier wants a different burst shape than the
  old 100/min bucket carried, so do not copy the placeholder's `burst=10,
  max_concurrency=2` blindly onto a 50 req/s scope.
- Give each endpoint definition its `quota_scope`, mirroring how GeoTab feed
  endpoints declare `GEOTAB_FEED` while `Get` endpoints declare `GEOTAB_GET`.
- **Confirm the 429 / Retry-After path stays sub-second-faithful.** Samsara
  returns `Retry-After` in **fractional seconds** (e.g. `0.40235`), and
  `penalize()` takes float seconds. Verified clean today:
  `_parse_retry_after_seconds` (`network/contract/classifier.py`) parses with
  `float()` and preserves the fraction, and `_penalize_scope`
  (`network/client/transport.py`) passes it through unfloored. Keep it that way
  as the scope count multiplies — a stray `int()` here would silently
  under-penalize every sub-second backoff.

**Design question to settle before building.** The per-endpoint tiers are
per-organization-per-endpoint budgets, but the 150 req/s (token) and 200 req/s
(org) ceilings are cross-endpoint caps on the whole token. Per-endpoint scopes
model the former naturally and the latter not at all: three independent scopes
at 50 + 25 + 5 req/s stay under the ceiling by construction, but a design that
later pins several endpoints near their tiers could sum past it. Decide whether
to (a) rely on the tier sum staying comfortably under the ceiling, or (b) add a
shared Samsara-global ceiling limiter the per-endpoint scopes nest under.
`idling_events` — absent from the chart — is the immediate instance: it has no
per-endpoint tier, so it must fall back to whatever ceiling model this question
resolves to, not to an invented per-endpoint number.

**Interim mitigation (available today, no code change).** For a one-off large
Samsara backfill, a downstream `providers.samsara.rate_limit` override in the
consuming YAML (e.g. 25–50 req/s) lifts the whole provider scope safely: the
429 `Retry-After` backoff is the safety net if the override overshoots a real
tier. The recurring / daily-window use case does **not** need this — its
windows are tiny and never approach even the placeholder budget.

**Pointers.** DESIGN §7 (the scope-split it anticipates: "Samsara documents
per-endpoint limits … the likely next split") and §13 ("Per-endpoint quota
scopes"); `config/providers.py` (`_SAMSARA_DEFAULT_RATE_LIMIT` and its
rationale comment; `GeotabConfig.scope_rate_limits()` as the template);
`vocabulary/quota_scope.py` (the enum to extend); `endpoints/samsara/*.py` (the
paths to tag). Source: Samsara rate-limit docs,
`developers.samsara.com/docs/rate-limits`, reviewed 2026-07-23 (documented, not
captured — so treat the tiers as the port discipline treats all docs:
authoritative for the split's shape, re-provable against a live 429 if one
appears).

---

## 2. Pin the real Motive rate limit

**Why.** The Motive scope's budget (`_MOTIVE_DEFAULT_RATE_LIMIT`, 60/min,
`config/providers.py`) is a placeholder like Samsara's — §13 couples the two —
but unlike Samsara there is no published tier chart to pin it against. The one
rate limit Motive documents at all, on `/vehicle_locations`, was not observed
to enforce (§8), and its success responses carry no rate-limit headers, so the
real budget is unobservable outside a 429. Beyond that inert line, the only
rate-limit statements that exist are **unofficial forum answers from Motive
staff** on Motive's API community forum, each given per-endpoint in reply to a
specific question, saying that the named endpoint is not rate limited —
verbatim, on the Geofence API: *"There is no rate limit on the Geofence API's,
but its appreciated that best practices should be followed while consuming all
of the API's."* Treat these as hints, not guarantees: they are unofficial,
scoped to the single endpoint each answer names, and may change — do **not**
generalize "no limit" across the API. So there is no authoritative number to
pin, and the honest posture is empirical. The current 60/min (with `burst=10`,
`max_concurrency=2`) is the diagnostic's proven-safe conservative posture — a
live full-fleet fan-out ran under it without a single 429 — not a measured
ceiling.

**What to do.**

- Empirically probe the Motive utilization endpoints — `driving_periods` and
  `idle_events` — one at a time, pushing throughput to find how high it can
  safely go under best practices (backoff, no hammering), and record the
  observed safe ceiling per endpoint. GeoTab's `Get`/`GetFeed`/`Authenticate`
  budgets were pinned the same empirical way — from the provider's own
  rate-limit signal, not its docs — but there the signal was the
  `X-Rate-Limit-*` headers GeoTab returns on ordinary successful responses
  (only the Authenticate tier came from an actual OverLimit). Motive returns no
  such headers on success (§8), so its probe has to drive *toward* a 429 to
  read the limit at all.
- Set `_MOTIVE_DEFAULT_RATE_LIMIT` (or per-endpoint scopes) to the observed
  ceiling. The other outcome is equally acceptable: nothing enforces and no 429
  surfaces under a good-faith push, in which case that is recorded and the
  conservative 60/min default stands **by decision**, not by omission.
- The transport-boundary limiter plus 429 `Retry-After` backoff is the safety
  net throughout, and stands regardless of what the probe finds — a proven-safe
  floor under any observed ceiling, and the reason a probe can push at all. Like
  GeoTab's captured headers, the probe informs the config default only; it does
  not add a live header-driven loop — the v1 limiter stays reactive (configured
  budget plus 429 penalty).
- If the probe shows Motive meters per-endpoint, this folds into the same
  per-endpoint-scope machinery as item 1.

**Pointers.** DESIGN §13 ("Rate-limit values for Motive and Samsara are
placeholders") and §8 (the `/vehicle_locations` limit observed inert; Motive
success responses carry no rate-limit headers); `config/providers.py`
(`_MOTIVE_DEFAULT_RATE_LIMIT` comment). Source: Motive publishes no rate-limit
tier chart; the "no rate limit" statements are unofficial per-endpoint answers
from Motive staff on Motive's API community forum, reviewed 2026-07-23 —
treated as the port discipline treats every unwritten claim: a hint to probe
against, re-provable only against a live 429.

---

## 3. Records-layer capability gaps

Three deferred pieces of the records / schema-derivation layer, grouped because
they share a decision surface — how faithfully a wire shape maps to columns —
and none has yet met an endpoint that forces it.

**What to do (each independent, built only when a real endpoint needs it).**

- **The `schema_overrides` hatch (§9).** The one contract piece still deferred
  on the endpoint binding (`endpoints/shared/base.py`). Build it **complete —
  the dtype side and the value-serialization side together** — when a real
  derivation gap needs it; a schema-only half-build is forbidden (§15).
- **The list-of-structs derivation vertical.** The records schema derivation
  supports scalars, enums, `list[scalar]`, and nested models only. `list[struct]`
  fields are therefore excluded wholesale today — `tags` on Samsara `vehicle`
  and `address`, the GeoTab Device/User precedent
  (`models/samsara/vehicle.py` records the exclusion and this obligation). Build
  the derivation vertical when an endpoint's primary payload — not an
  excludable side field — is a list of structs.
- **The flattening opt-out (§13).** Flattening is default-on and the
  per-endpoint opt-out exists in the design, but no shipped endpoint has needed
  it. Left as a live tension, not a build item, until one does.
- **Nested event-time field support.** `_require_date_like_field`
  (`endpoints/shared/base.py`) resolves a top-level Pydantic field only; a
  nested event-time field is deferred until an endpoint needs one.

**Pointers.** DESIGN §9 (records / flattening / schema derivation), §13
(flattening opt-out), §15 (the `schema_overrides` half-build ban);
`endpoints/shared/base.py`; `models/samsara/vehicle.py`.

---

## 4. `updated_after` ingestion-time CDC hook (uncommitted)

**Why.** The late-upload gap: a vehicle offline for days uploads
old-`located_at` records later, and a `located_at` watermark with a fixed
lookback never re-fetches them. Motive accepts `updated_after` on the
per-vehicle history endpoint (though observed inert, §8), making it a candidate
for closing the gap at ingestion time rather than by widening the lookback.

**What to do.** This is an uncommitted candidate, not a design commitment — it
belongs to the incremental-strategy design conversation before any code. The
open question: whether ingestion-time CDC is worth a second watermark axis, or
whether a wider lookback remains the simpler correct answer.

**Pointers.** DESIGN §13 (the uncommitted candidate) and §15 (listed unbuilt).

---

## 5. Unbuilt storage cell: the date-window `single` writer

**Why.** The storage mechanism matrix (§3) has one unbuilt cell: single-file
windowed storage (a `single` writer over a windowed sync mode). No shipped
endpoint needs it.

**What to do.** Build it when an endpoint needs single-file windowed storage,
under the recorded obligation: serialize, or reject `backfill_unit_workers > 1`
— one shared file cannot host parallel units (§3/§5).

**Pointers.** DESIGN §3 (the mechanism matrix), §5 (the unit-worker
obligation), §15 (listed unbuilt).

---

## 6. Page-fanning within one member's request chain

**Why.** The concurrency ladder (§7) runs providers, endpoints, and
units/members concurrently, but one grain is deliberately absent: fanning the
*pages* within a single member's request chain. Each member's chain still walks
its pages strictly serially.

**What to do.** This awaits envelope verification (a page-parallel walk needs a
total page count or equivalent up front, which most cursor walks do not expose)
and will be recorded with its own design when it lands — it is a new grain on
the ladder, not a tweak to an existing one.

**Pointers.** DESIGN §7 (the non-goal recorded at the fan-out design: "page-
fanning within one member's chain awaits envelope verification").

---

## 7. Staging-clear robustness on synced / scanned filesystems

**Why.** A live run on a OneDrive-synced `dataset_root` failed in
`clear_partition_staging` (`shutil.rmtree` → `PermissionError` / `WinError 5`):
the sync handler held the staging directory during cleanup. This is **not a
correctness bug** — at the clear point the finalized partition is already
promoted, so leftover staging is cosmetic, not corrupting. It is Windows
reality, not OneDrive-specific: endpoint antivirus scan-on-write produces the
same `WinError 5` on fresh writes, in exactly the corporate Windows
environments fleet telematics runs in.

**What to do.**

- Best-effort removal with a short retry/backoff, degrading to a logged
  `WARNING` rather than crashing a run whose data landed correctly.
- A deterministic regression test: hold an external handle on the staging
  directory and assert retry-then-warn (no OneDrive required).
- Keep the standing docs note that `dataset_root` should be a real filesystem
  path, not a live cloud-synced folder.

This is polish-phase work (item 8), broken out here because it carries a
concrete fix and test.

**Pointers.** DESIGN §13 (the full incident and intended fix) and §15 (the
polish-phase gate).

---

## 8. The polish phase

**Why.** A cluster of tree-wide passes deliberately gated on a stable public
surface — running them before the surface settles would audit code still in
motion.

**What to do (the recorded set, §15).**

- A full-tree ceremony audit, a test-coverage audit, and a documentation audit.
- The usage-driven README rewrite.
- Multi-platform CI — notably a Windows leg, which would have caught the
  missing-`tzdata` failure automatically.
- **Signal hardening.** A `KeyboardInterrupt` landing while the main thread
  joins the provider queue workers abandons the remaining joins, so the
  client/pool context managers can close while a worker still fetches — noisy
  worker tracebacks and bounded exit latency, **never corruption** (the
  per-unit parquet → ledger → done-mark → prefix-commit ordering keeps state
  sound, the same crash story §5/§14 tell). Deferred here as an accepted
  residual, recorded at the provider-parallel activation.
- The staging-clear robustness fix (item 7) rolls up here.

**Pointers.** DESIGN §15 (the polish-phase gate and its members); §7 (the
signal-hardening accepted residual).

---

## 9. Endpoint coverage — the port queue

Endpoint breadth is a scope principle, not roadmap debt: an endpoint is
deferred, never excluded, for lacking a known consumer (DESIGN §1). The live
coverage queue — the GeoTab `Get` entity surface (Zone, Group, Rule, and kin),
the feed entities deferred as unobservable on the probed tenant (`ChargeEvent`,
`TrailerAttachment`, `IoxAddOn`, `CustomData`, `EmissionComplianceEvent`,
`Route`), and the beyond-legacy surfaces across all three providers — is
tracked in **`ENDPOINTS.md`'s port queue** on the standard probe-then-build
vertical, and is **not duplicated here**.

A related, lighter category also lives at its own decision points rather than
here: per-tenant **census items** — model fields excluded as empty/unobservable
on the probed tenant, each carrying a "revisit when a tenant populates it" note
(e.g. `defectList.children` on `dvir_logs`, the three empty-container
exclusions on `media_files`, `geofence.polygon` on `addresses`). These are
probe-when-data-appears items, not engineering work; they are recorded in the
relevant `models/` docstrings and in `ENDPOINTS.md`'s per-endpoint notes.

**Pointers.** `ENDPOINTS.md` (Port queue; the port discipline); DESIGN §1
(breadth as a scope principle), §15 (the ongoing port queue).
