# fleetpull

fleetpull pulls fleet telematics data from provider APIs — **Motive**,
**Samsara**, and **GeoTab** — and delivers it as typed, dtype-coerced,
lightly normalized tabular output: Polars DataFrames in memory, parquet on
disk, staying as close to the raw API responses as is reasonable.

It is deliberately narrow. fleetpull does no cross-endpoint merging, builds
no unified cross-provider schema, performs no semantic deduplication, loads
no warehouse, and assumes no end use — downstream processing is the
consumer's concern. What it does instead is be rigorous about retrieval:
probed (never merely documented) provider behavior, crash-safe incremental
state, token-bucket rate limiting at the transport boundary, and one
explicit schema per (provider, endpoint).

**Status:** alpha. The two public verbs below are settled, and coverage is
broad — the Motive and Samsara endpoint inventories and the GeoTab `Get` and
feed surfaces are all shipped (see [ENDPOINTS.md](ENDPOINTS.md)). Pre-1.0,
the internals are still free to improve.

## Install

```bash
pip install fleetpull
# or
uv add fleetpull
```

The latest development version, straight from source:

```bash
pip install git+https://github.com/andrewjordan3/fleetpull
```

Python ≥ 3.12. Core dependencies: `httpx`, `polars`, `pydantic` 2.x,
`pyyaml`, `truststore`, `tzdata`.

To scaffold a starter config for the `sync` verb below, run
`fleetpull init-config` — it writes an annotated `fleetpull_config.yaml`
you edit and point `sync` at.

## The two verbs

### `fetch` — one snapshot, in memory

The programmatic convenience verb: one endpoint's full current listing as an
eager, typed Polars DataFrame. No disk, no state, no configuration file.

```python
from fleetpull import Endpoints, fetch

vehicles = fetch(Endpoints.Motive.vehicles, auth='your-api-key')
devices = fetch(
    Endpoints.Geotab.devices,
    auth={'username': '...', 'password': '...', 'database': '...'},
)
```

- `auth` is a bare API-key string for Motive/Samsara and named fields (a
  mapping or `GeotabAuthConfig`) for GeoTab. Credentials are wrapped in
  `SecretStr` at the boundary and never appear in errors or logs.
- Behind a TLS-intercepting corporate proxy (Zscaler-class), pass
  `use_truststore=True` to build TLS contexts from the OS trust store.
- `fetch` exposes **snapshot endpoints only** — a snapshot is bounded by
  entity count, so the in-memory contract stays honest. Windowed history is
  `sync` territory, and the type checker (plus a runtime guard) enforces the
  split.
- An empty result is a zero-row frame carrying the full typed schema, never
  `None`.

### `Sync` — config-driven incremental pipeline

The pipeline verb: a YAML config selects providers and endpoints; each run
fetches incrementally, writes parquet, and commits its resume state.
`fleetpull init-config` writes a documented starter config to edit.

```python
from fleetpull import Sync

Sync('fleetpull_config.yaml').run()
```

```yaml
sync:
  default_start_date: 2025-01-01   # cold-start backfill anchor

storage:
  dataset_root: /data/fleet        # parquet lands here

logging:
  console_level: INFO

providers:
  motive:
    endpoints: [vehicles, vehicle_locations, driving_periods]
    # api_key: falls back to the MOTIVE_API_KEY environment variable
  samsara:
    endpoints: [vehicles]
    # api_key: falls back to SAMSARA_API_KEY
  geotab:
    auth:
      username: user@example.com
      database: my_database
      # password: falls back to GEOTAB_PASSWORD
    endpoints: [devices, users, trips]
    lookback_days: 7               # late-arrival refetch margin
```

The same run is available from the shell: `fleetpull sync fleetpull_config.yaml`.

Endpoints run and commit independently — one endpoint's failure never halts
its siblings; a run with failures ends by raising `SyncFailuresError`
carrying every failure (queue order within each provider — feeders then
consumers, config order within each — providers in config order).

Output is one folder per endpoint under `dataset_root`:

```
data/
  motive/
    vehicles/                    # snapshot: one file, replaced each run
      data.parquet
      metadata.json              # human-readable run summary — never read by the program
    driving_periods/             # windowed: hive date partitions
      date=2026-07-15/part.parquet
      date=2026-07-16/part.parquet
      metadata.json
```

Hive `date=YYYY-MM-DD` layout is read natively by `pl.scan_parquet` and
BigQuery external tables. Operational state (watermarks, run ledger, backfill
work units) lives in SQLite at `<dataset_root>/.fleetpull/state.sqlite3`;
crash-safety ordering (parquet first, cursor second) plus delete-by-window
merge makes interrupted runs refetch idempotently — at-least-once fetching,
exactly-once data.

## Output contract

- **One schema per (provider, endpoint).** Column dtypes derive from each
  endpoint's Pydantic response model; nested objects flatten to
  double-underscore-joined columns (`driver__id`). No cross-endpoint or
  cross-provider unification, ever.
- **Event timestamps are timezone-aware UTC** end to end.
- **Exact-duplicate rows** (artifacts of pagination and crash refetch) are
  dropped at write time; same-id-different-payload reconciliation belongs to
  consumers.
- Values arrive as the provider sent them — no unit conversion, no semantic
  cleanup. Provider quirks worth knowing (GeoTab's seconds-despite-the-name
  `engineHours`, sentinel dates, per-endpoint window anchoring) are recorded
  in [ENDPOINTS.md](ENDPOINTS.md) and DESIGN §8.

## Errors

Consumers catch `FleetpullError` or its five public subclasses:

| Exception | When | Reasonable reaction |
|---|---|---|
| `ConfigurationError` | Bad config or wiring | Fix config, rerun |
| `AuthenticationError` | Rejected credentials | Fix credentials |
| `ProviderResponseError` | Non-retryable or contract-violating response | Investigate before rerunning |
| `RetriesExhaustedError` | Transient/rate-limit budget ran out | Rerun later |
| `SyncFailuresError` | One or more endpoints failed inside a sync run | Inspect `failures`, act per member |

Everything else is internal. Rate limits are respected automatically — a
shared token-bucket limiter sits at the transport boundary and a 429's
`Retry-After` pauses the whole quota scope.

## Documentation

- [ENDPOINTS.md](ENDPOINTS.md) — every shipped endpoint, its mechanics, and
  the port queue.
- [DESIGN.md](DESIGN.md) — the design of record: architecture, invariants,
  and the probe-captured provider behaviors every binding encodes.
- [CLAUDE.md](CLAUDE.md) — engineering standards and verification gates.

## Contributing

Contributions are welcome and encouraged — a new endpoint, a bug fix, a
sharper docstring, or a provider quirk you've hit in the wild. Start with
[CONTRIBUTING.md](CONTRIBUTING.md); the short version:

```bash
uv sync --group dev
uv run ruff format . && uv run ruff check . \
  && uv run mypy src/ tests/ \
  && uv run lint-imports \
  && uv run pytest
```

These five gates are exactly what CI runs on every pull request, so a green
local run is a green CI run. Tests never hit real provider APIs; new
endpoints are built probe-first from live captures — the port discipline is
in [ENDPOINTS.md](ENDPOINTS.md), and the engineering standards are in
[CLAUDE.md](CLAUDE.md).

## Acknowledgements

Built on [Polars](https://pola.rs), [Pydantic](https://pydantic.dev),
[httpx](https://www.python-httpx.org), and
[truststore](https://truststore.readthedocs.io) — fleetpull is a thin, rigorous
layer over their work.

## Author

By Andrew Jordan. Questions, bugs, and endpoint requests are best raised as
[GitHub issues](https://github.com/andrewjordan3/fleetpull/issues).

## License

[Apache License 2.0](LICENSE).
