# scripts/run_geotab_users.py
"""Hand-run driver: prove the GeoTab users snapshot end to end through ``Sync``.

The devices proof re-run against ``User``: the config-driven verb loads
a generated YAML with a geotab section, the session auth stack
Authenticates, the seek-paging ``Get`` walk pulls the full User listing
in id-ascending pages (id-sort proven live for this type 2026-07-16),
the ``GetCountOf`` completeness guard proves the walk lost nothing, and
the snapshot writer replaces ``geotab/users/data.parquet`` wholesale.
The diagnostic reporting is deliberately identifier-free -- counts and
null shares only, never a name, login, phone, or license value -- so
console output stays shareable.

Set GEOTAB_USERNAME, GEOTAB_DATABASE, GEOTAB_PASSWORD, and DATASET_ROOT
in the environment (the password never lands in a file -- ``Sync``'s
credential fallback reads it), then run from the repo root:

    uv run python scripts/run_geotab_users.py

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
    config_path = dataset_root / 'fleetpull-geotab-users-diagnostic.yaml'
    config_path.write_text(
        f'sync:\n'
        f'  default_start_date: 2026-06-01\n'
        f'storage:\n'
        f'  dataset_root: {dataset_root}\n'
        f'logging:\n'
        f'  console_level: INFO\n'
        f'  file_path: {dataset_root / "fleetpull-geotab-users-diagnostic.log"}\n'
        f'  file_level: DEBUG\n'
        f'http:\n'
        f'  use_truststore: {str(USE_TRUSTSTORE).lower()}\n'
        f'providers:\n'
        f'  geotab:\n'
        f'    auth:\n'
        f'      username: {GEOTAB_USERNAME}\n'
        f'      database: {GEOTAB_DATABASE}\n'
        f'      server: {GEOTAB_SERVER}\n'
        f'    endpoints: [users]\n',
        encoding='utf-8',
    )
    return config_path


def _print_population_shape(snapshot: pl.DataFrame) -> None:
    """Make the absence-shaped optionality visible, identifier-free.

    The driver split and per-column null shares reproduce the sweep's
    shape facts (the driver-only block's null share should equal the
    non-driver share exactly); no cell values are printed.
    """
    driver_counts = snapshot.get_column('is_driver').value_counts().sort('is_driver')
    print('\nis_driver split:')
    for row in driver_counts.iter_rows():
        print(f'  {row[0]}: {row[1]} records')
    print('\nNull share per column (absence-shaped optionality, quantified):')
    null_counts = snapshot.null_count().row(0, named=True)
    for column in snapshot.columns:
        null_share = null_counts[column] / snapshot.height
        print(f'  {column}: {null_share:.0%} null')


def _print_ledger_row(dataset_root: Path) -> None:
    """Report the users run's ledger row -- status straight from SQLite."""
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
    """Drive the users snapshot through Sync and report the reads."""
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

    endpoint_dir = endpoint_directory(dataset_root, Provider.GEOTAB.value, 'users')
    snapshot_path = endpoint_dir / 'data.parquet'
    print(f'\nParquet layout: {snapshot_path}')
    snapshot = pl.read_parquet(snapshot_path)
    print(f'Snapshot: {snapshot.height} User records, {snapshot.width} columns.')

    _print_population_shape(snapshot)
    _print_ledger_row(dataset_root)


if __name__ == '__main__':
    main()
