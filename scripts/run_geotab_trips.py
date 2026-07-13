# scripts/run_geotab_trips.py
"""Hand-run driver: prove the GeoTab trips watermark vertical through ``Sync``.

The live proof of the first windowed GeoTab endpoint composed at once:
the config-driven verb loads a generated YAML with a geotab section, the
session stack Authenticates, the plan-and-drive unit loop tiles the
resume window into work units, each unit's ``Get``/``TripSearch`` walk
seeks id-ascending pages with the window riding ``search`` on every
advance, and the date-partitioned writer replaces each covered
``date=YYYY-MM-DD`` partition wholesale. The diagnostic reporting is
reads over the results: per-partition row counts and the frame dtypes
for ``driving_duration`` (the Duration column), ``driver__id`` (the
sentinel flattening), and ``start`` (the event-time column), plus the
run-ledger rows.

Because trips is windowed, this script drives the sync path over a short
recent window (the ``run_vehicle_locations.py`` pattern: the generated
config's ``default_start_date`` is a few days back, so Run 1 exercises
the cold-backfill arm and a re-run resumes from the persisted
watermark). State is deliberately retained across invocations.

Set GEOTAB_PASSWORD in the environment (never in a file -- ``Sync``'s
credential fallback reads it; the other three credential fields are not
secrets and live in the knobs below) and DATASET_ROOT, then run from the
repo root:

    uv run python scripts/run_geotab_trips.py

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

# The three non-secret credential fields -- safe to write in a file. The
# password comes from the GEOTAB_PASSWORD environment variable only.
GEOTAB_USERNAME: str = 'user@example.com'
GEOTAB_DATABASE: str = 'exampledb'
GEOTAB_SERVER: str = 'my.geotab.com'

# Where the parquet and the SQLite operational-state DB land. Set this for
# your environment before running.
DATASET_ROOT: str = ''

# True behind the Zscaler-intercepting laptop; False on Colab / a GCP VM.
USE_TRUSTSTORE: bool = True

# The cold-start window width in whole days: an unbounded backfill would
# pull the fleet's full Trip history; this keeps Run 1 to a few recent
# days (the vehicle_locations script's posture).
BACKFILL_LOOKBACK_DAYS: int = 3

# The watermark late-arrival margin and trailing-edge holdback, in whole
# days. For trips the lookback is also what absorbs GeoTab's Trip
# recalculation: a recalculated trip inside the margin is refetched and
# its partitions replaced.
LOOKBACK_DAYS: int = 1
CUTOFF_DAYS: int = 0


def _write_config_file(dataset_root: Path) -> Path:
    """Generate the run's YAML config beside the dataset root.

    The password is deliberately absent: Sync's environment fallback
    resolves GEOTAB_PASSWORD into the auth section's password field, so
    the secret never lands in a file. Username, database, and server are
    not secrets and are written from the knobs above.

    Args:
        dataset_root: Where the dataset (and the generated config) live.

    Returns:
        The written config path.
    """
    default_start_date = (
        datetime.now(tz=UTC) - timedelta(days=BACKFILL_LOOKBACK_DAYS)
    ).date()
    config_path = dataset_root / 'fleetpull-geotab-trips-diagnostic.yaml'
    config_path.write_text(
        f'sync:\n'
        f'  default_start_date: {default_start_date.isoformat()}\n'
        f'storage:\n'
        f'  dataset_root: {dataset_root}\n'
        f'logging:\n'
        f'  console_level: INFO\n'
        f'  file_path: {dataset_root / "fleetpull-geotab-trips-diagnostic.log"}\n'
        f'  file_level: DEBUG\n'
        f'http:\n'
        f'  use_truststore: {str(USE_TRUSTSTORE).lower()}\n'
        f'providers:\n'
        f'  geotab:\n'
        f'    auth:\n'
        f'      username: {GEOTAB_USERNAME}\n'
        f'      database: {GEOTAB_DATABASE}\n'
        f'      server: {GEOTAB_SERVER}\n'
        f'    endpoints: [trips]\n'
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
    print('\nDtype proof (Duration column, sentinel flattening, event time):')
    for column in ('driving_duration', 'driver__id', 'start'):
        print(f'  {column}: {combined.schema[column]}')
    sentinel_share = (
        combined.get_column('driver__id') == 'UnknownDriverId'
    ).sum() / combined.height
    print(f'  UnknownDriverId share of driver__id: {sentinel_share:.0%}')


def _print_ledger_rows(dataset_root: Path) -> None:
    """Report the trips run's ledger rows -- status straight from SQLite."""
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
    """Drive the trips watermark vertical through Sync and report the reads."""
    if not os.environ.get('GEOTAB_PASSWORD', ''):
        raise SystemExit('Set GEOTAB_PASSWORD in the environment before running.')
    if not DATASET_ROOT:
        raise SystemExit('Set DATASET_ROOT to a destination path before running.')

    dataset_root = Path(DATASET_ROOT)
    dataset_root.mkdir(parents=True, exist_ok=True)
    config_path = _write_config_file(dataset_root)
    print(f'Generated config: {config_path}')

    Sync(config_path).run()

    endpoint_dir = endpoint_directory(dataset_root, Provider.GEOTAB.value, 'trips')
    combined = _print_partitions(endpoint_dir)
    _print_dtype_proof(combined)
    _print_ledger_rows(dataset_root)


if __name__ == '__main__':
    main()
