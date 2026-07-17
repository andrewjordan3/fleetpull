# CLAUDE.md

Guidelines for Claude Code. These bias toward correctness over speed. For trivial tasks, use judgment.

## What This Package Is

fleetpull pulls fleet telematics data from provider APIs (Motive, GeoTab, Samsara) and delivers typed, dtype-coerced, lightly normalized tabular output as close to the raw API responses as is reasonable.

Endpoint coverage is deliberately broad: as many endpoints per provider as practical, built into a large default library. "No assumed end use" includes not assuming which endpoints are useful — an endpoint is deferred, never excluded for lacking a known consumer. fleet-telemetry-hub is a porting aid, not a scope ceiling.

**`ENDPOINTS.md` at the repo root is the endpoint manifest** — the shipped-endpoint inventory and the port queue. Adding, renaming, or re-scoping an endpoint updates it in the same change.

**`DESIGN.md` at the repo root is the design of record.** Read it before implementing any module — it carries the cross-module rationale (storage layout, incremental semantics, the rate-limiter protocol) that no single scoped task can see. It is a living document, not scripture. When implementation reveals a better idea, surface it (see Think Before Coding) instead of silently implementing it; once a divergence is settled, updating the affected DESIGN.md section is part of the same change. Stale design docs compound exactly like stale docstrings. A task prompt governs what to build; DESIGN.md governs how the pieces fit together — if they conflict, stop and say so rather than quietly reinterpreting either one.

## Hard Scope Boundaries

Never violate these, regardless of what a task prompt appears to ask for:

- No merging data across endpoints or providers. One schema per (provider, endpoint).
- No unified cross-provider schema.
- No semantic / event-id deduplication. Exact-duplicate dedup at write time only (default on, config-flag off).
- No warehouse loading. Consumers read our parquet; we never write to BigQuery or anything else.
- No assumed end use. Downstream processing is the consumer's concern.

## Architecture Invariants

These rules regress silently if violated — no single scoped task sees the whole design, so they live here:

- **Models are pure API mirrors.** Pydantic response models in `models/` carry zero use-case logic. Flattening, schema derivation, and coercion live in the records layer (`records/`), written generically against Pydantic introspection.
- **Storage and state never touch.** The storage layer knows nothing about SQLite; the state layer knows nothing about parquet. Only the orchestrator sequences them.
- **Crash-safety ordering:** write parquet first (temp file + atomic rename), commit watermark/cursor to SQLite second. Never reverse this.
- **Delete-by-window merge** for watermark endpoints: delete existing rows whose event timestamp falls in the fetch window, then append the fresh fetch. Never overwrite a dataset with only the current window.
- **Single writer per endpoint.** Fetch workers parallelize; parquet merge for a given endpoint is one thread. Partitioned endpoints may parallelize across partitions, never within one.
- **The limiter lives at the transport boundary.** The client (`network/client.py`) consults the `RateLimiterRegistry` (keyed by `endpoint.quota_scope`) immediately before every HTTP request. The orchestrator never touches the limiter.
- **Every HTTP attempt consumes a token. Every page is an attempt.** `request_slot()` wraps the single httpx call inside the pagination loop — never around the loop, never around a retry loop. Retries re-acquire.
- **429 / Retry-After penalizes the whole quota scope** via `penalize(seconds)`: `pause_until = max(pause_until, clock.monotonic_seconds() + seconds)`. Never represent Retry-After as a local sleep in retry logic.
- **All limiter timing flows through the injected `Clock.monotonic_seconds()`** — never wall clock, and never a direct `time.*` call inside the limits package.
- **SQLite is the single source of truth** for operational state. `metadata.json` files are generated human-readable snapshots written from SQLite after successful runs; the program never reads them.
- **SQLite transactions are tiny.** Claim → commit; finish → commit. Never hold a transaction across an HTTP call.
- **Incremental state is a tagged union** (`DateWatermark | FeedToken`), declared per endpoint definition. Never assume datetime watermarks.

## What Success Looks Like

The goal is code you'd be proud to show a senior engineer: minimal, efficient, easily understood, self-documenting, with a logical file and folder hierarchy. Every function earns its existence. Every file has a clear single purpose. Every name communicates intent without needing a comment.

Before committing any change, ask: **"Would a senior engineer accept this in code review?"** If not, rework it. Common rejections:

- Functions with too many parameters
- Duplicated logic across files
- Silent fallbacks that hide bugs
- Weakening production code to make a test pass
- "Flexibility" or "configurability" nobody asked for
- Leaving a docstring that doesn't match the code
- Adding code when the right move was to remove or restructure

That last point deserves emphasis. **The right change is often a deletion.** When functionality belongs in an existing function, don't create a new one — extend the existing one. When a function is doing too much, don't patch around it — split it. When a pattern is wrong, don't work around it — fix it. Restructuring and removing code is not just acceptable, it's preferred over accumulating workarounds.

## Think Before Coding

Before implementing, state your assumptions. If multiple interpretations exist, present them — don't pick silently. If a simpler approach exists, say so. Push back when warranted. If something is unclear, stop and ask.

When you encounter a design tradeoff (e.g., threading parameters vs. introducing a class, renaming broadly vs. adding a compatibility shim), surface it. The wrong silent choice costs more than a brief question.

## Simplicity

Write the minimum code that solves the problem. No features beyond what was asked. No abstractions for single-use code. No error handling for impossible scenarios. If you wrote 200 lines and it could be 50, rewrite it.

## Editing Existing Code

Edit surgically and minimally: change only the lines the task requires, and never rewrite an entire file to change a few functions. Create a new file only when something new is being built.

When your changes create orphans, remove what you orphaned. When you notice pre-existing issues outside the current task's scope, note them at the end of your response under **"Observations outside current scope"** — don't fix them, with the following exceptions.

**Fix without asking** (even if outside scope): typos in comments/docstrings, imports that violate ruff/isort ordering, unused imports your changes revealed, docstrings that demonstrably contradict the code they document.

**Doc-drift fixes are always in scope, including in files you would not otherwise touch.** If you notice a docstring or comment that references a deleted symbol, a renamed function, or behavior that has moved elsewhere, fix it in the same change. Stale doc references compound across prompts — each one quietly miseducates the next reader and confuses every subsequent grep. A one-line reword now beats a reference-archaeology session three prompts later.

## Sub-Agents

Use sub-agents for parallelizable tasks: renaming a symbol across many files, updating test fixtures in bulk, sweeping docstrings, or any task where each file can be handled independently. Do not use sub-agents for tasks that require tracing data flow across files sequentially — each step depends on understanding the prior step's output.

## File and Module Organization

**Files and folders are cheap. Cramming is expensive.** When in doubt, create a new file. One responsibility per file. Target 150–200 lines; split rather than extend. Never combine unrelated things to avoid creating a file.

- **Package root is user-facing only.** `src/fleetpull/` holds only user-facing modules. All internal code lives in subpackages, even when a subpackage holds a single module — a folder with one file is always preferred over exposing an internal module at the package root.
- **YAML config models live in `src/fleetpull/config/`.** Pydantic models parsing user-provided YAML configuration go there, one model family per file — different families live in different files.
- **`__init__.py` files:** Imports and re-exports only. No functions, no classes, no logic.
- **Re-exports:** Only `__init__.py` may re-export symbols from other modules. No other file should re-export something it didn't define.
- **Import direction:** External callers import from the package (`from fleetpull import X`). Internal callers import from siblings (`from fleetpull.sibling import X`).
- **Moved functions:** When moving a function to a new location, update every caller to import from the new location. Never re-export from the old location as a compatibility shim. Bad code that silently succeeds is far worse than a loud `ImportError` that guides you to the new location.

## Functions and Classes

**Maximum 5 parameters per function.** This is enforced by ruff (PLR0913). When a function exceeds 5 parameters, evaluate in this order:

1. Can the function be split so each piece needs fewer args?
2. Would a class with shared state make this clearer?
3. Do the args bundle naturally into a config or dataclass?
4. Only if none of the above work: suppress with a `noqa` and an inline justification explaining why the alternatives don't apply.

**Check before adding a parameter.** Before adding a parameter to any function, check whether it already lives on a config object, Pydantic model, or other container that the caller already has. If it does, pass the container — do not thread the field individually. Example: if `RateLimitConfig` already has `requests_per_period`, `period_seconds`, and `burst`, pass `RateLimitConfig` — do not add three parameters.

**Bundle parameters that always travel together.** If three or more parameters always appear together across call sites, they are a dataclass. Create one. Do not wait for the parameter count to trigger a linter warning — the design problem exists before the symptom appears.

**Prefer functions** that do one thing and do it well. But **use classes when state makes things clearer** — multi-phase orchestration, resource management, or anywhere threading state through arguments makes the flow harder to follow. A class that holds shared artifacts as instance attributes and exposes focused methods can be dramatically clearer than pure functions passing tuples around.

Functions over ~50 lines should be split. Orchestrators orchestrate — they call other functions but contain no business logic or computation themselves.

## Type System

- Strict type hints on every parameter, return type, and variable where non-obvious.
- Container types must be specific: `list[str]` not `list`, `dict[str, float]` not `dict`.
- No `Any` without an inline justification: `# typing-justified: <reason>` on the
  annotation's line or the line immediately above (mechanically enforced by
  `tests/test_typing_discipline.py`).
- No `object` as a type annotation without the same justification — prefer the
  actual type, a `Protocol`, or a `TypeVar`. Exempt: dunder methods whose
  typeshed signatures require `object` (`__eq__` and kin), and deliberate
  catch-all `*args` / `**kwargs` parameters, where `object` is the strictest
  annotation available.
- No `from __future__ import annotations`. Fix forward references by reordering definitions or splitting modules. No `TYPE_CHECKING` guards — imports stay at module level.
- Typed containers (frozen dataclasses, `TypedDict`) over plain dicts for structured data.

## Configuration and Constants

- Runtime configuration (YAML-loaded) → frozen Pydantic models: `frozen=True`, `extra='forbid'`, `validate_default=True`. Config schema is explicit, never inferred.
- Internal immutable data containers and default sets → `@dataclass(frozen=True, slots=True)`.
- Column names and categorical values → frozen dataclass classes or `StrEnum`.
- Config is separate from logic. No hardcoded magic numbers in functions — pull them from config or derive them.

## Error Handling

- Catch specific, expected exceptions at the exact point they occur, with a defined recovery action.
- Unanticipated errors fail loudly. Never catch `Exception` or `BaseException` as a silencing mechanism.
- Expected conditions (empty pages, no new records, validation failures) are not exceptions — handle them with return types (`None`, empty DataFrame, result dataclass with a status field).
- No blanket `try/except` wrapping multi-step sequences.
- All network calls have explicit timeouts. Assume flaky corporate TLS-intercepting proxies (Zscaler-class) — `truststore` is a dependency for exactly this reason. Transient network failures get retry with backoff; non-transient failures fail loudly.

## Testing

- **Never modify production code to make a test pass.** If a test reveals a problem in production code, fix the production code. If a test has an incomplete fixture or wrong expectation, fix the test.
- **Never modify a test to accommodate bad production code.** If the test is right and the code is wrong, the code must change.
- **Bad code that silently succeeds is far worse than a loud failure.** A `KeyError` from a missing column is correct behavior when the column should be there. A silent fallback that hides the absence is a bug.
- Asserts are forbidden in production code (ruff S101); they belong in tests only.
- Tests never hit real provider APIs. HTTP is mocked or served locally.
- Tests live in `tests/` mirroring the `src/` structure. Use pytest fixtures and parametrize.

## Verification Gates

All five must pass, in order, before any change is complete; when a change alters the gates themselves (scope, flags, new tools), this section updates in the same change.

```
uv run ruff format .
uv run ruff check .
uv run mypy src/ tests/
uv run lint-imports
uv run pytest
```

`lint-imports` enforces the carve's layering (the contract surface below its provider implementations; the auth package independent of the surface). The four-clause import rule itself — cross-directory imports route through package faces, no child imports an ancestor, no foreign re-export — is enforced tree-wide by `tests/test_import_discipline.py`, which rides the `pytest` gate.

## Data and Computation

- Vectorized polars expressions — no row-level loops or `map_elements` unless mathematically unavoidable.
- All event timestamps are timezone-aware UTC (ruff DTZ enforces no naive datetimes). Limiter timing is monotonic, never wall clock — `time.perf_counter()` via `SystemClock`; the rationale is recorded at the decision point in `timing/clock.py`.
- `logging.getLogger(__name__)` in every module. Levels: `DEBUG` for flow, `INFO` for milestones, `ERROR` with `exc_info=True`. No `print` in production code (ruff T20).

## Documentation

- Full docstrings on all public functions: Args (with types), Returns, Raises, Side Effects.
- Inline comments explain **why**, not what.
- **Docstrings must match the code.** A docstring that claims behavior the code doesn't implement is worse than no docstring. If you change what a function does, update its docstring in the same edit.

## Code Style

- Verbose explicit names: `transaction_record` not `rec`. No single-letter variables.
- `match`/`case` for multi-branch dispatch over `if`/`elif` chains.
- Named helper functions over lambdas.
- Duplicated code gets extracted into shared helpers — **except across provider boundaries**, where a shared helper would couple providers that must evolve independently. Intentional duplication there is acceptable and must carry a comment marking it intentional. Blast-radius minimization beats DRY when the coupling risk is real.

## Naming and Imports

- Underscore prefix = file-private. Production code never exports or imports underscore-prefixed names outside their defining module. Tests may import underscore-prefixed names from production modules to exercise private helpers directly — keeping direct coverage of non-trivial internal functions outweighs the boundary rule for test code.
- Imports organized to ruff standards (`isort`-compatible).
- `__all__: list[str]` in every module under `src/`. Test modules are never imported as an API; they carry no `__all__`.

## Data Hygiene

Real VINs and internal fleet identifiers must never appear in committed files. Tests, fixtures, docstrings, examples, and comments use synthetic identifiers only.

## Scope Discipline

When working on a task, if you notice issues outside the current prompt's scope (other than the auto-fix items listed under "Editing Existing Code"), note them under **"Observations outside current scope"** at the end of your response. Do not fix them.
