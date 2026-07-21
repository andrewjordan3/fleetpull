"""Tests for fleetpull.storage.atomic."""

from pathlib import Path

import polars as pl
import pytest

from fleetpull.storage import atomic
from fleetpull.storage.atomic import atomic_write_parquet


def _frame() -> pl.DataFrame:
    return pl.DataFrame({'a': [1, 2], 'b': ['x', 'y']})


def _record_fsyncs(monkeypatch: pytest.MonkeyPatch) -> list[Path]:
    """Route ``_fsync_path`` through a recorder that still fsyncs for real."""
    synced: list[Path] = []
    real_fsync_path = atomic._fsync_path

    def recording_fsync_path(path: Path) -> None:
        synced.append(path)
        real_fsync_path(path)

    monkeypatch.setattr(atomic, '_fsync_path', recording_fsync_path)
    return synced


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


def test_a_non_durable_write_never_fsyncs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    synced = _record_fsyncs(monkeypatch)
    atomic_write_parquet(_frame(), tmp_path / 'data.parquet')
    assert synced == []


def test_a_durable_write_into_an_existing_directory_fsyncs_file_then_parent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    synced = _record_fsyncs(monkeypatch)
    target = tmp_path / 'data.parquet'
    atomic_write_parquet(_frame(), target, durable=True)
    assert synced[0].parent == tmp_path
    assert synced[0].name.endswith('.tmp')
    assert synced[1:] == [tmp_path]


def test_a_durable_write_fsyncs_the_whole_newly_created_directory_chain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # THE DURABLE-CHAIN TRIPWIRE (DESIGN section 14, invariant 1, hardened
    # 2026-07-21): each newly created directory's own entry lives one level
    # up, so a chain fsynced only at its deepest link is not durable --
    # power loss could drop the whole new partition directory while the
    # fsynced token commit survived, persisting a cursor past lost data.
    # The write must fsync the parent, EVERY created ancestor, and the
    # first pre-existing directory (here tmp_path, which receives the
    # 'geotab' entry).
    synced = _record_fsyncs(monkeypatch)
    target = tmp_path / 'geotab' / 'log_records' / 'date=2026-07-14' / 'part-1.parquet'
    atomic_write_parquet(_frame(), target, durable=True)
    assert synced[0].name.endswith('.tmp')
    assert synced[1:] == [
        target.parent,
        tmp_path / 'geotab' / 'log_records',
        tmp_path / 'geotab',
        tmp_path,
    ]
