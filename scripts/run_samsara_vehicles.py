# scripts/run_samsara_vehicles.py
"""Hand-run driver: prove the Samsara vehicles snapshot end to end through ``Sync``.

The live proof of the whole Samsara foundation composed at once: the
config-driven verb loads a generated YAML with a samsara section, the
bearer ingress arm authenticates every request, the cursor walk pulls
the full vehicle listing (``limit`` on page one, ``after`` merged
thereafter, terminal on ``hasNextPage: false``), and the snapshot writer
replaces ``samsara/vehicles/data.parquet`` wholesale. The diagnostic
reporting is deliberately identifier-free -- counts and null shares
only, never a name, VIN, serial, or plate -- so console output stays
shareable.

Set SAMSARA_API_KEY and DATASET_ROOT in the environment (the token never
lands in a file -- ``Sync``'s credential fallback reads it), then run
from the repo root:

    uv run python scripts/run_samsara_vehicles.py

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

# Where the parquet and the SQLite operational-state DB land.
DATASET_ROOT: str = os.environ.get('DATASET_ROOT', '')

# True behind the Zscaler-intercepting laptop; False on Colab / a GCP VM.
USE_TRUSTSTORE: bool = True


def _write_config_file(dataset_root: Path) -> Path:
    """Generate the run's YAML config beside the dataset root.

    The credential is deliberately absent: Sync's environment fallback
    resolves SAMSARA_API_KEY into the section's api_key field, so the
    secret never lands in a file.

    Args:
        dataset_root: Where the dataset (and the generated config) live.

    Returns:
        The written config path.
    """
    config_path = dataset_root / 'fleetpull-samsara-vehicles-diagnostic.yaml'
    config_path.write_text(
        f'sync:\n'
        f'  default_start_date: 2026-06-01\n'
        f'storage:\n'
        f'  dataset_root: {dataset_root}\n'
        f'logging:\n'
        f'  console_level: INFO\n'
        f'  file_path: {dataset_root / "fleetpull-samsara-vehicles-diagnostic.log"}\n'
        f'  file_level: DEBUG\n'
        f'http:\n'
        f'  use_truststore: {str(USE_TRUSTSTORE).lower()}\n'
        f'providers:\n'
        f'  samsara:\n'
        f'    endpoints: [vehicles]\n',
        encoding='utf-8',
    )
    return config_path


def _print_population_shape(snapshot: pl.DataFrame) -> None:
    """Make the absence-shaped optionality visible, identifier-free.

    Per-column null shares reproduce the sweep's shape facts (the
    minimal-variant share appears as the gateway-block columns' null
    share); no cell values are printed.
    """
    print('\nNull share per column (absence-shaped optionality, quantified):')
    null_counts = snapshot.null_count().row(0, named=True)
    for column in snapshot.columns:
        null_share = null_counts[column] / snapshot.height
        print(f'  {column}: {null_share:.0%} null')


def _print_ledger_row(dataset_root: Path) -> None:
    """Report the vehicles run's ledger row -- status straight from SQLite."""
    connection = sqlite3.connect(dataset_root / '.fleetpull' / 'state.sqlite3')
    try:
        rows = connection.execute(
            "SELECT endpoint, mode, status FROM runs WHERE provider = 'samsara' "
            'ORDER BY run_id'
        ).fetchall()
    finally:
        connection.close()
    print('\nLedger rows (provider=samsara):')
    for endpoint_name, mode, status in rows:
        print(f'  {endpoint_name} [{mode}]: {status}')


def main() -> None:
    """Drive the vehicles snapshot through Sync and report the reads."""
    for variable_name in ('SAMSARA_API_KEY', 'DATASET_ROOT'):
        if not os.environ.get(variable_name, ''):
            raise SystemExit(f'Set {variable_name} in the environment before running.')

    dataset_root = Path(DATASET_ROOT)
    dataset_root.mkdir(parents=True, exist_ok=True)
    config_path = _write_config_file(dataset_root)
    print(f'Generated config: {config_path}')

    Sync(config_path).run()

    endpoint_dir = endpoint_directory(dataset_root, Provider.SAMSARA.value, 'vehicles')
    snapshot_path = endpoint_dir / 'data.parquet'
    print(f'\nParquet layout: {snapshot_path}')
    snapshot = pl.read_parquet(snapshot_path)
    print(f'Snapshot: {snapshot.height} Vehicle records, {snapshot.width} columns.')

    _print_population_shape(snapshot)
    _print_ledger_row(dataset_root)


if __name__ == '__main__':
    main()
