"""Tests for fleetpull.storage.merge."""

import polars as pl

from fleetpull.storage.merge import drop_exact_duplicates, merge_snapshot


def test_snapshot_returns_new_and_ignores_existing() -> None:
    existing = pl.DataFrame({'a': [1, 2, 3]})
    new = pl.DataFrame({'a': [9]})
    assert merge_snapshot(existing, new).equals(new)


def test_snapshot_handles_no_existing() -> None:
    new = pl.DataFrame({'a': [1]})
    assert merge_snapshot(None, new).equals(new)


def test_dedup_drops_byte_identical_rows() -> None:
    frame = pl.DataFrame({'a': [1, 1, 2], 'b': ['x', 'x', 'y']})
    assert drop_exact_duplicates(frame).height == 2


def test_dedup_preserves_order() -> None:
    frame = pl.DataFrame({'a': [2, 1, 2], 'b': ['y', 'x', 'y']})
    assert drop_exact_duplicates(frame)['a'].to_list() == [2, 1]


def test_dedup_keeps_same_key_different_payload() -> None:
    frame = pl.DataFrame({'a': [1, 1], 'b': ['x', 'z']})
    assert drop_exact_duplicates(frame).height == 2
