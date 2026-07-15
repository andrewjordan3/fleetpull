# scripts/run_motive_driving_idle.py
"""Hand-run driver: prove the Motive driving_periods + idle_events verticals.

The live proof of the fleet-wide Motive event pair composed at once: the
config-driven verb loads a generated YAML selecting both endpoints, each
runs its plan-and-drive unit loop over the resume window, the shared
wrapped-list decoder walks the offset pages, and the date-partitioned
writer replaces each covered ``date=YYYY-MM-DD`` partition wholesale.
idle_events additionally proves the wire-window pad: its requests reach
one day past each side of the resume window while the written partitions
stay inside it (the company-local overlap matching, DESIGN section 8).

The diagnostic reporting is reads over the results: per-partition row
counts and the dtypes for the load-bearing columns per endpoint --
``start_time`` (the event-time anchor), ``duration`` (float seconds),
``distance`` (the verbatim formatted string, null on in-progress rows)
for driving_periods; ``start_time``/``end_time`` and ``end_type`` for
idle_events -- plus the run-ledger rows.

Because both endpoints are windowed, this script drives the sync path
over a short recent window (the ``run_geotab_trips.py`` pattern: the
generated config's ``default_start_date`` is a few days back, so Run 1
exercises the cold-backfill arm and a re-run resumes from the persisted
watermark). State is deliberately retained across invocations.

Set MOTIVE_API_KEY and DATASET_ROOT in the environment (the key never
lands in a file -- ``Sync``'s credential fallback reads it), then run
from the repo root:

    uv run python scripts/run_motive_driving_idle.py

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

# Where the parquet and the SQLite operational-state DB land.
DATASET_ROOT: str = os.environ.get('DATASET_ROOT', '')

# True behind the Zscaler-intercepting laptop; False on Colab / a GCP VM.
USE_TRUSTSTORE: bool = True

# The cold-start window width in whole days: an unbounded backfill would
# pull months of fleet-wide events; this keeps Run 1 to a few recent days.
BACKFILL_LOOKBACK_DAYS: int = 3

# The watermark late-arrival margin and trailing-edge holdback, in whole
# days. One day of lookback also refetches yesterday's in-progress
# driving periods once they complete.
LOOKBACK_DAYS: int = 1
CUTOFF_DAYS: int = 0

_ENDPOINT_DTYPE_COLUMNS: dict[str, tuple[str, ...]] = {
    'driving_periods': ('start_time', 'duration', 'distance'),
    'idle_events': ('start_time', 'end_time', 'end_type'),
}


def _write_config_file(dataset_root: Path) -> Path:
    """Generate the run's YAML config beside the dataset root.

    The API key is deliberately absent: Sync's environment fallback
    resolves MOTIVE_API_KEY into the provider's key field, so the secret
    never lands in a file.

    Args:
        dataset_root: Where the dataset (and the generated config) live.

    Returns:
        The written config path.
    """
    default_start_date = (
        datetime.now(tz=UTC) - timedelta(days=BACKFILL_LOOKBACK_DAYS)
    ).date()
    config_path = dataset_root / 'fleetpull-motive-events-diagnostic.yaml'
    config_path.write_text(
        f'sync:\n'
        f'  default_start_date: {default_start_date.isoformat()}\n'
        f'storage:\n'
        f'  dataset_root: {dataset_root}\n'
        f'logging:\n'
        f'  console_level: INFO\n'
        f'  file_path: {dataset_root / "fleetpull-motive-events-diagnostic.log"}\n'
        f'  file_level: DEBUG\n'
        f'http:\n'
        f'  use_truststore: {str(USE_TRUSTSTORE).lower()}\n'
        f'providers:\n'
        f'  motive:\n'
        f'    endpoints: [driving_periods, idle_events]\n'
        f'    lookback_days: {LOOKBACK_DAYS}\n'
        f'    cutoff_days: {CUTOFF_DAYS}\n',
        encoding='utf-8',
    )
    return config_path


def _print_partitions(endpoint_dir: Path, list_key: str) -> pl.DataFrame:
    """Print per-partition row counts; return the combined frame."""
    frames: list[pl.DataFrame] = []
    print(f'\nOn-disk partitions ({list_key}):')
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


def _print_dtype_proof(combined: pl.DataFrame, columns: tuple[str, ...]) -> None:
    """The live proof of the load-bearing column dtypes."""
    if combined.is_empty():
        print('No rows to type-check yet.')
        return
    print('Dtype proof:')
    for column in columns:
        print(f'  {column}: {combined.schema[column]}')


def _print_ledger_rows(dataset_root: Path) -> None:
    """Report the run's ledger rows -- status straight from SQLite."""
    connection = sqlite3.connect(dataset_root / '.fleetpull' / 'state.sqlite3')
    try:
        rows = connection.execute(
            "SELECT endpoint, mode, status FROM runs WHERE provider = 'motive' "
            'ORDER BY run_id'
        ).fetchall()
    finally:
        connection.close()
    print('\nLedger rows (provider=motive):')
    for endpoint_name, mode, status in rows:
        print(f'  {endpoint_name} [{mode}]: {status}')


def main() -> None:
    """Drive both Motive event verticals through Sync and report the reads."""
    for variable_name in ('MOTIVE_API_KEY', 'DATASET_ROOT'):
        if not os.environ.get(variable_name, ''):
            raise SystemExit(f'Set {variable_name} in the environment before running.')

    dataset_root = Path(DATASET_ROOT)
    dataset_root.mkdir(parents=True, exist_ok=True)
    config_path = _write_config_file(dataset_root)
    print(f'Generated config: {config_path}')

    Sync(config_path).run()

    for endpoint_name, dtype_columns in _ENDPOINT_DTYPE_COLUMNS.items():
        endpoint_dir = endpoint_directory(
            dataset_root, Provider.MOTIVE.value, endpoint_name
        )
        combined = _print_partitions(endpoint_dir, endpoint_name)
        _print_dtype_proof(combined, dtype_columns)
    _print_ledger_rows(dataset_root)


if __name__ == '__main__':
    main()
