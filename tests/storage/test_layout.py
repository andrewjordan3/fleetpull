"""Tests for fleetpull.storage.layout."""

from pathlib import Path

import polars as pl

from fleetpull.storage.layout import SingleFileLayout
from fleetpull.storage.merge import merge_snapshot


def _frame() -> pl.DataFrame:
    return pl.DataFrame({'a': [1, 2], 'b': ['x', 'y']})


def test_first_run_writes_the_file(tmp_path: Path) -> None:
    result = SingleFileLayout().write_dataset(tmp_path, _frame(), merge_snapshot)
    assert (tmp_path / 'data.parquet').exists()
    assert result.rows_written == 2
    assert result.files_written == 1


def test_snapshot_overwrites_existing(tmp_path: Path) -> None:
    layout = SingleFileLayout()
    layout.write_dataset(tmp_path, _frame(), merge_snapshot)
    replacement = pl.DataFrame({'a': [9], 'b': ['z']})
    layout.write_dataset(tmp_path, replacement, merge_snapshot)
    assert pl.read_parquet(tmp_path / 'data.parquet').equals(replacement)


def test_reports_dropped_exact_duplicates(tmp_path: Path) -> None:
    duped = pl.DataFrame({'a': [1, 1, 2], 'b': ['x', 'x', 'y']})
    result = SingleFileLayout().write_dataset(tmp_path, duped, merge_snapshot)
    assert result.duplicates_dropped == 1
    assert result.rows_written == 2
