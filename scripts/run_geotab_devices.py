# scripts/run_geotab_devices.py
"""Hand-run driver: prove the GeoTab devices snapshot end to end through ``Sync``.

The live proof of the whole GeoTab foundation composed at once: the
config-driven verb loads a generated YAML with a geotab section, the
session auth stack Authenticates (its own quota scope, the single-flight
manager), the seek-paging ``Get`` walk pulls the full Device listing in
id-ascending pages under the silent 5,000-record cap, the ``GetCountOf``
completeness guard proves the walk lost nothing, and the snapshot writer
replaces ``geotab/devices/data.parquet`` wholesale. The diagnostic
reporting is reads over the results: record count, the ``deviceType``
polymorphism and per-column null share (the union-of-shapes model made
visible), the parquet layout, and the run-ledger row.

Set GEOTAB_PASSWORD in the environment (never in a file -- ``Sync``'s
credential fallback reads it; the other three credential fields are not
secrets and live in the knobs below) and DATASET_ROOT, then run from the
repo root:

    uv run python scripts/run_geotab_devices.py

Errors propagate with a traceback by design -- this is a debugging driver.
"""

import os
import sqlite3
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
    config_path = dataset_root / 'fleetpull-geotab-diagnostic.yaml'
    config_path.write_text(
        f'sync:\n'
        f'  default_start_date: 2026-06-01\n'
        f'storage:\n'
        f'  dataset_root: {dataset_root}\n'
        f'logging:\n'
        f'  console_level: INFO\n'
        f'  file_path: {dataset_root / "fleetpull-geotab-diagnostic.log"}\n'
        f'  file_level: DEBUG\n'
        f'http:\n'
        f'  use_truststore: {str(USE_TRUSTSTORE).lower()}\n'
        f'providers:\n'
        f'  geotab:\n'
        f'    auth:\n'
        f'      username: {GEOTAB_USERNAME}\n'
        f'      database: {GEOTAB_DATABASE}\n'
        f'      server: {GEOTAB_SERVER}\n'
        f'    endpoints: [devices]\n',
        encoding='utf-8',
    )
    return config_path


def _print_polymorphism(snapshot: pl.DataFrame) -> None:
    """Make the union-of-shapes model visible: types and null shares.

    Distinct ``device_type`` values show the hardware generations and the
    trailer sentinel; per-column null share shows which fields each shape
    carries (a trailer-heavy fleet nulls the telematics parameters, a
    GO7-era fleet nulls the vinInfo block).
    """
    print('\nDistinct device_type values:')
    type_counts = snapshot.get_column('device_type').value_counts().sort('device_type')
    for row in type_counts.iter_rows():
        print(f'  {row[0]!r}: {row[1]} records')
    print('\nNull share per column (the shape polymorphism, quantified):')
    null_counts = snapshot.null_count().row(0, named=True)
    for column in snapshot.columns:
        null_share = null_counts[column] / snapshot.height
        print(f'  {column}: {null_share:.0%} null')


def _print_ledger_row(dataset_root: Path) -> None:
    """Report the devices run's ledger row -- status straight from SQLite."""
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
    """Drive the devices snapshot through Sync and report the reads."""
    if not os.environ.get('GEOTAB_PASSWORD', ''):
        raise SystemExit('Set GEOTAB_PASSWORD in the environment before running.')
    if not DATASET_ROOT:
        raise SystemExit('Set DATASET_ROOT to a destination path before running.')

    dataset_root = Path(DATASET_ROOT)
    dataset_root.mkdir(parents=True, exist_ok=True)
    config_path = _write_config_file(dataset_root)
    print(f'Generated config: {config_path}')

    Sync(config_path).run()

    endpoint_dir = endpoint_directory(dataset_root, Provider.GEOTAB.value, 'devices')
    snapshot_path = endpoint_dir / 'data.parquet'
    print(f'\nParquet layout: {snapshot_path}')
    snapshot = pl.read_parquet(snapshot_path)
    print(f'Snapshot: {snapshot.height} Device records, {snapshot.width} columns.')

    _print_polymorphism(snapshot)
    _print_ledger_row(dataset_root)


if __name__ == '__main__':
    main()
