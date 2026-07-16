# scripts/run_geotab_exception_events.py
"""Hand-run driver: prove the GeoTab exception_events bisected vertical.

The live proof of the first bisected endpoint composed at once: the
config-driven verb loads a generated YAML selecting exception_events,
the session stack Authenticates, the plan-and-drive unit loop tiles the
resume window into work units, the bisecting driver fetches each unit
window whole (halving on the exactly-full overflow signal — rarely
expected at steady-state widths, load-bearing on dense days), and the
date-partitioned writer replaces each covered ``date=YYYY-MM-DD``
partition on ``active_from``. The stream is UNFILTERED — every rule;
consumers select rules downstream (DESIGN §8's decision block).

The diagnostic reporting is reads over the results: per-partition row
counts and the frame dtypes for ``duration`` (the Duration column),
``driver__id`` / ``diagnostic__id`` (the sentinel flattening),
``rule__id`` (the consumer's selection column), and ``active_from``
(the event-time column), plus the run-ledger rows.

Because the endpoint is windowed, this script drives the sync path over
a short recent window (the ``run_geotab_trips.py`` pattern: the
generated config's ``default_start_date`` is a few days back, so Run 1
exercises the cold-backfill arm and a re-run resumes from the persisted
watermark). State is deliberately retained across invocations.

Set GEOTAB_USERNAME, GEOTAB_DATABASE, GEOTAB_PASSWORD, and DATASET_ROOT
in the environment (the password never lands in a file -- ``Sync``'s
credential fallback reads it), then run from the repo root:

    uv run python scripts/run_geotab_exception_events.py

Errors propagate with a traceback by design -- this is a debugging driver.
"""

import os
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl

from fleetpull import Sync
from fleetpull.paths import endpoint_directory
from fleetpull.vocabulary import Provider

# --- knobs (the config file below is generated from these) ------------------

# The account-identifying credential fields come from the environment,
# never this file; the password rides GEOTAB_PASSWORD through Sync's
# credential fallback. The server is GeoTab's public entry host.
GEOTAB_USERNAME: str = os.environ.get('GEOTAB_USERNAME', '')
GEOTAB_DATABASE: str = os.environ.get('GEOTAB_DATABASE', '')
GEOTAB_SERVER: str = 'my.geotab.com'

# Where the parquet and the SQLite operational-state DB land.
DATASET_ROOT: str = os.environ.get('DATASET_ROOT', '')

# True behind the Zscaler-intercepting laptop; False on Colab / a GCP VM.
USE_TRUSTSTORE: bool = True

# The cold-start window width in whole days: an unbounded backfill would
# pull the full multi-year event history; this keeps Run 1 to a few
# recent days.
BACKFILL_LOOKBACK_DAYS: int = 3

# The watermark late-arrival margin and trailing-edge holdback, in whole
# days. One day of lookback absorbs the observed post-creation mutation
# envelope (~1 h).
LOOKBACK_DAYS: int = 1
CUTOFF_DAYS: int = 0


def _write_config_file(dataset_root: Path) -> Path:
    """Generate the run's YAML config beside the dataset root.

    The password is deliberately absent: Sync's environment fallback
    resolves GEOTAB_PASSWORD into the auth section's password field, so
    the secret never lands in a file. Username and database arrive via
    the environment (the knobs above) and are written with the server.

    Args:
        dataset_root: Where the dataset (and the generated config) live.

    Returns:
        The written config path.
    """
    default_start_date = (
        datetime.now(tz=UTC) - timedelta(days=BACKFILL_LOOKBACK_DAYS)
    ).date()
    config_path = dataset_root / 'fleetpull-geotab-events-diagnostic.yaml'
    config_path.write_text(
        f'sync:\n'
        f'  default_start_date: {default_start_date.isoformat()}\n'
        f'storage:\n'
        f'  dataset_root: {dataset_root}\n'
        f'logging:\n'
        f'  console_level: INFO\n'
        f'  file_path: {dataset_root / "fleetpull-geotab-events-diagnostic.log"}\n'
        f'  file_level: DEBUG\n'
        f'http:\n'
        f'  use_truststore: {str(USE_TRUSTSTORE).lower()}\n'
        f'providers:\n'
        f'  geotab:\n'
        f'    auth:\n'
        f'      username: {GEOTAB_USERNAME}\n'
        f'      database: {GEOTAB_DATABASE}\n'
        f'      server: {GEOTAB_SERVER}\n'
        f'    endpoints: [exception_events]\n'
        f'    lookback_days: {LOOKBACK_DAYS}\n'
        f'    cutoff_days: {CUTOFF_DAYS}\n',
        encoding='utf-8',
    )
    return config_path


def _print_partitions(endpoint_dir: Path) -> pl.DataFrame:
    """Print per-partition row counts; return the combined frame."""
    frames: list[pl.DataFrame] = []
    print('\nOn-disk partitions:')
    part_files = sorted(endpoint_dir.glob('date=*/part.parquet'))
    if not part_files:
        print(f'  No partitions under {endpoint_dir} yet.')
        return pl.DataFrame()
    for part_file in part_files:
        frame = pl.read_parquet(part_file)
        frames.append(frame)
        print(f'  {part_file.parent.name}/part.parquet: {frame.height} rows')
    combined = pl.concat(frames)
    print(f'  Total: {combined.height} rows across {len(frames)} partitions.')
    return combined


def _print_dtype_proof(combined: pl.DataFrame) -> None:
    """The live proof of the Duration dtype and the sentinel flattening."""
    if combined.is_empty():
        print('No rows to type-check yet.')
        return
    print('\nDtype proof (Duration, sentinels, rule selection, event time):')
    for column in (
        'duration',
        'driver__id',
        'diagnostic__id',
        'rule__id',
        'active_from',
    ):
        print(f'  {column}: {combined.schema[column]}')
    rule_counts = combined.get_column('rule__id').value_counts().sort('count')
    print('Distinct rules in the window (the consumer selection column):')
    for rule_id, count in rule_counts.iter_rows():
        print(f'  {rule_id!r}: {count} events')


def _print_ledger_rows(dataset_root: Path) -> None:
    """Report the run's ledger rows -- status straight from SQLite."""
    connection = sqlite3.connect(dataset_root / '.fleetpull' / 'state.sqlite3')
    try:
        rows = connection.execute(
            "SELECT endpoint, mode, status FROM runs WHERE provider = 'geotab' "
            'ORDER BY run_id'
        ).fetchall()
    finally:
        connection.close()
    print('\nLedger rows (provider=geotab):')
    for endpoint_name, mode, status in rows:
        print(f'  {endpoint_name} [{mode}]: {status}')


def main() -> None:
    """Drive the exception_events vertical through Sync and report the reads."""
    required_env = (
        'GEOTAB_USERNAME',
        'GEOTAB_DATABASE',
        'GEOTAB_PASSWORD',
        'DATASET_ROOT',
    )
    for variable_name in required_env:
        if not os.environ.get(variable_name, ''):
            raise SystemExit(f'Set {variable_name} in the environment before running.')

    dataset_root = Path(DATASET_ROOT)
    dataset_root.mkdir(parents=True, exist_ok=True)
    config_path = _write_config_file(dataset_root)
    print(f'Generated config: {config_path}')

    Sync(config_path).run()

    endpoint_dir = endpoint_directory(
        dataset_root, Provider.GEOTAB.value, 'exception_events'
    )
    combined = _print_partitions(endpoint_dir)
    _print_dtype_proof(combined)
    _print_ledger_rows(dataset_root)


if __name__ == '__main__':
    main()
