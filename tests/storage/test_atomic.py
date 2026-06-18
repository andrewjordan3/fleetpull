"""Tests for fleetpull.storage.atomic."""

from pathlib import Path

import polars as pl

from fleetpull.storage.atomic import atomic_write_parquet


def _frame() -> pl.DataFrame:
    return pl.DataFrame({'a': [1, 2], 'b': ['x', 'y']})


def test_writes_a_readable_parquet(tmp_path: Path) -> None:
    target = tmp_path / 'data.parquet'
    atomic_write_parquet(_frame(), target)
    assert pl.read_parquet(target).equals(_frame())


def test_creates_missing_parent_directory(tmp_path: Path) -> None:
    target = tmp_path / 'motive' / 'vehicles' / 'data.parquet'
    atomic_write_parquet(_frame(), target)
    assert target.exists()


def test_overwrites_an_existing_target(tmp_path: Path) -> None:
    target = tmp_path / 'data.parquet'
    atomic_write_parquet(_frame(), target)
    replacement = pl.DataFrame({'a': [9], 'b': ['z']})
    atomic_write_parquet(replacement, target)
    assert pl.read_parquet(target).equals(replacement)


def test_leaves_no_temp_file_behind(tmp_path: Path) -> None:
    atomic_write_parquet(_frame(), tmp_path / 'data.parquet')
    leftovers = [p for p in tmp_path.iterdir() if p.name.endswith('.tmp')]
    assert leftovers == []
