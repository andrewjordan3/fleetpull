"""Tests for fleetpull.orchestrator.backfill."""

from datetime import UTC, datetime, timedelta, timezone

import pytest

from fleetpull.incremental import DateWindow
from fleetpull.orchestrator.backfill import (
    _date_chunks,
    _is_utc_midnight,
    plan_partitioned_backfill_units,
)
from fleetpull.state import WorkUnitSpec
from fleetpull.vocabulary import Provider

_MIDNIGHT = datetime(2026, 1, 1, tzinfo=UTC)


def _span(days: int) -> DateWindow:
    return DateWindow(start=_MIDNIGHT, end=_MIDNIGHT + timedelta(days=days))


class TestDateChunks:
    def test_exact_multiple_tiles_into_full_chunks(self) -> None:
        chunks = _date_chunks(_span(90), timedelta(days=30))
        assert chunks == [
            (_MIDNIGHT, _MIDNIGHT + timedelta(days=30)),
            (_MIDNIGHT + timedelta(days=30), _MIDNIGHT + timedelta(days=60)),
            (_MIDNIGHT + timedelta(days=60), _MIDNIGHT + timedelta(days=90)),
        ]

    def test_remainder_last_chunk_shorter_but_whole_day(self) -> None:
        chunks = _date_chunks(_span(70), timedelta(days=30))
        assert chunks == [
            (_MIDNIGHT, _MIDNIGHT + timedelta(days=30)),
            (_MIDNIGHT + timedelta(days=30), _MIDNIGHT + timedelta(days=60)),
            (_MIDNIGHT + timedelta(days=60), _MIDNIGHT + timedelta(days=70)),
        ]
        last_start, last_end = chunks[-1]
        assert last_end - last_start == timedelta(days=10)

    def test_span_within_one_chunk_yields_one_chunk(self) -> None:
        chunks = _date_chunks(_span(5), timedelta(days=30))
        assert chunks == [(_MIDNIGHT, _MIDNIGHT + timedelta(days=5))]

    def test_chunks_are_contiguous_and_midnight_aligned(self) -> None:
        span = _span(70)
        chunks = _date_chunks(span, timedelta(days=30))
        assert chunks[0][0] == span.start
        assert chunks[-1][1] == span.end
        for index in range(len(chunks) - 1):
            assert chunks[index][1] == chunks[index + 1][0]
        for chunk_start, chunk_end in chunks:
            assert chunk_start < chunk_end
            assert _is_utc_midnight(chunk_start)
            assert _is_utc_midnight(chunk_end)

    def test_non_whole_day_chunk_raises(self) -> None:
        with pytest.raises(ValueError, match='whole number of days'):
            _date_chunks(_span(30), timedelta(hours=36))
        with pytest.raises(ValueError, match='whole number of days'):
            _date_chunks(_span(30), timedelta(days=1, hours=1))
        assert _date_chunks(_span(2), timedelta(days=1)) == [
            (_MIDNIGHT, _MIDNIGHT + timedelta(days=1)),
            (_MIDNIGHT + timedelta(days=1), _MIDNIGHT + timedelta(days=2)),
        ]

    def test_non_positive_chunk_raises(self) -> None:
        with pytest.raises(ValueError, match='positive whole number of days'):
            _date_chunks(_span(30), timedelta(0))
        with pytest.raises(ValueError, match='positive whole number of days'):
            _date_chunks(_span(30), timedelta(days=-1))

    def test_non_midnight_time_of_day_span_bound_raises(self) -> None:
        span = DateWindow(
            start=datetime(2026, 1, 1, 6, tzinfo=UTC),
            end=datetime(2026, 1, 31, 6, tzinfo=UTC),
        )
        with pytest.raises(ValueError, match='midnight UTC'):
            _date_chunks(span, timedelta(days=30))

    def test_non_utc_offset_span_bound_raises(self) -> None:
        offset = timezone(timedelta(hours=5))
        span = DateWindow(
            start=datetime(2026, 1, 1, tzinfo=offset),
            end=datetime(2026, 1, 31, tzinfo=offset),
        )
        with pytest.raises(ValueError, match='midnight UTC'):
            _date_chunks(span, timedelta(days=30))

    def test_naive_span_bound_raises(self) -> None:
        # Built tz-aware then stripped so the DTZ lint stays satisfied; the point
        # is that ``utcoffset()`` is ``None`` for a naive bound, which the guard
        # rejects just as it rejects a nonzero offset.
        naive_start = datetime(2026, 1, 1, tzinfo=UTC).replace(tzinfo=None)
        naive_end = datetime(2026, 1, 31, tzinfo=UTC).replace(tzinfo=None)
        span = DateWindow(start=naive_start, end=naive_end)
        with pytest.raises(ValueError, match='midnight UTC'):
            _date_chunks(span, timedelta(days=30))


class TestPlanPartitionedBackfillUnits:
    def test_crosses_chunks_with_members(self) -> None:
        units = plan_partitioned_backfill_units(
            Provider.MOTIVE, 'locations', ['v1', 'v2'], _span(90), timedelta(days=30)
        )
        assert len(units) == 6  # three chunks x two members

    def test_is_chunk_major(self) -> None:
        units = plan_partitioned_backfill_units(
            Provider.MOTIVE, 'locations', ['v1', 'v2'], _span(90), timedelta(days=30)
        )
        chunk_zero = _MIDNIGHT
        chunk_one = _MIDNIGHT + timedelta(days=30)
        chunk_two = _MIDNIGHT + timedelta(days=60)
        assert [(unit.partition_key, unit.chunk_start) for unit in units] == [
            ('v1', chunk_zero),
            ('v2', chunk_zero),
            ('v1', chunk_one),
            ('v2', chunk_one),
            ('v1', chunk_two),
            ('v2', chunk_two),
        ]

    def test_propagates_identity_and_chunk_bounds(self) -> None:
        units = plan_partitioned_backfill_units(
            Provider.MOTIVE, 'locations', ['v1'], _span(30), timedelta(days=30)
        )
        assert units == [
            WorkUnitSpec(
                provider=Provider.MOTIVE,
                endpoint='locations',
                partition_key='v1',
                chunk_start=_MIDNIGHT,
                chunk_end=_MIDNIGHT + timedelta(days=30),
            )
        ]

    def test_empty_members_yields_no_units(self) -> None:
        units = plan_partitioned_backfill_units(
            Provider.MOTIVE, 'locations', [], _span(90), timedelta(days=30)
        )
        assert units == []

    def test_invalid_span_propagates_value_error(self) -> None:
        span = DateWindow(
            start=datetime(2026, 1, 1, 6, tzinfo=UTC),
            end=datetime(2026, 1, 31, 6, tzinfo=UTC),
        )
        with pytest.raises(ValueError, match='midnight UTC'):
            plan_partitioned_backfill_units(
                Provider.MOTIVE, 'locations', ['v1'], span, timedelta(days=30)
            )

    def test_invalid_chunk_propagates_value_error(self) -> None:
        with pytest.raises(ValueError, match='whole number of days'):
            plan_partitioned_backfill_units(
                Provider.MOTIVE, 'locations', ['v1'], _span(30), timedelta(hours=36)
            )
