"""Tests for fleetpull.storage.read."""

from pathlib import Path

import polars as pl

from fleetpull.storage.read import read_parquet_if_exists


def test_returns_the_frame_when_the_file_exists(tmp_path: Path) -> None:
    target = tmp_path / 'data.parquet'
    frame = pl.DataFrame({'a': [1, 2], 'b': ['x', 'y']})
    frame.write_parquet(target)
    result = read_parquet_if_exists(target)
    assert result is not None
    assert result.equals(frame)


def test_returns_none_when_the_file_is_absent(tmp_path: Path) -> None:
    assert read_parquet_if_exists(tmp_path / 'missing.parquet') is None
