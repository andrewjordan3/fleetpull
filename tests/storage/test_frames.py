"""Tests for fleetpull.storage.frames."""

from datetime import UTC, datetime

import polars as pl

from fleetpull.incremental import DateWindow
from fleetpull.storage.frames import drop_exact_duplicates, in_window


def test_dedup_drops_byte_identical_rows() -> None:
    frame = pl.DataFrame({'a': [1, 1, 2], 'b': ['x', 'x', 'y']})
    assert drop_exact_duplicates(frame).height == 2


def test_dedup_preserves_order() -> None:
    frame = pl.DataFrame({'a': [2, 1, 2], 'b': ['y', 'x', 'y']})
    assert drop_exact_duplicates(frame)['a'].to_list() == [2, 1]


def test_dedup_keeps_same_key_different_payload() -> None:
    frame = pl.DataFrame({'a': [1, 1], 'b': ['x', 'z']})
    assert drop_exact_duplicates(frame).height == 2


def test_in_window_keeps_rows_inside_half_open_window() -> None:
    window = DateWindow(
        start=datetime(2026, 6, 1, tzinfo=UTC),
        end=datetime(2026, 6, 3, tzinfo=UTC),
    )
    frame = pl.DataFrame(
        {
            'located_at': [
                datetime(2026, 5, 31, 23, tzinfo=UTC),  # before start: out
                datetime(2026, 6, 1, tzinfo=UTC),  # exactly start: in
                datetime(2026, 6, 2, tzinfo=UTC),  # inside: in
                datetime(2026, 6, 3, tzinfo=UTC),  # exactly end: out
            ],
            'id': [1, 2, 3, 4],
        }
    )
    kept = frame.filter(in_window('located_at', window))
    assert kept.get_column('id').to_list() == [2, 3]


def test_in_window_negation_is_the_complement() -> None:
    window = DateWindow(
        start=datetime(2026, 6, 1, tzinfo=UTC),
        end=datetime(2026, 6, 3, tzinfo=UTC),
    )
    frame = pl.DataFrame(
        {
            'located_at': [
                datetime(2026, 5, 31, 23, tzinfo=UTC),
                datetime(2026, 6, 1, tzinfo=UTC),
                datetime(2026, 6, 3, tzinfo=UTC),
            ],
            'id': [1, 2, 3],
        }
    )
    outside = frame.filter(~in_window('located_at', window))
    assert outside.get_column('id').to_list() == [1, 3]
