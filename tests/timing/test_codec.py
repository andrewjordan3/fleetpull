# tests/timing/test_codec.py
"""Tests for fleetpull.timing.codec."""

from datetime import UTC, datetime, timedelta, timezone

import pytest

from fleetpull.timing.codec import from_iso8601, to_iso8601, to_utc_date_string

PLUS_FIVE: timezone = timezone(timedelta(hours=5))


class TestToIso8601:
    def test_renders_z_form_at_seconds_precision(self) -> None:
        moment = datetime(2026, 6, 1, 0, 0, 0, tzinfo=UTC)
        assert to_iso8601(moment) == '2026-06-01T00:00:00Z'

    def test_drops_subsecond_precision(self) -> None:
        moment = datetime(2026, 6, 1, 12, 30, 45, 123456, tzinfo=UTC)
        assert to_iso8601(moment) == '2026-06-01T12:30:45Z'

    def test_rejects_naive(self) -> None:
        with pytest.raises(ValueError, match='timezone-aware'):
            to_iso8601(datetime(2026, 6, 1, 0, 0, 0))  # noqa: DTZ001

    def test_rejects_non_utc(self) -> None:
        with pytest.raises(ValueError, match=r'datetime\.UTC'):
            to_iso8601(datetime(2026, 6, 1, 0, 0, 0, tzinfo=PLUS_FIVE))


class TestToUtcDateString:
    def test_renders_iso_date(self) -> None:
        assert (
            to_utc_date_string(datetime(2026, 6, 1, 0, 0, 0, tzinfo=UTC))
            == '2026-06-01'
        )

    def test_uses_the_utc_date(self) -> None:
        # Late in the UTC day, but still that UTC date.
        moment = datetime(2026, 6, 1, 23, 59, 59, tzinfo=UTC)
        assert to_utc_date_string(moment) == '2026-06-01'

    def test_rejects_naive(self) -> None:
        with pytest.raises(ValueError, match='timezone-aware'):
            to_utc_date_string(datetime(2026, 6, 1))  # noqa: DTZ001

    def test_rejects_non_utc(self) -> None:
        with pytest.raises(ValueError, match=r'datetime\.UTC'):
            to_utc_date_string(datetime(2026, 6, 1, tzinfo=PLUS_FIVE))


class TestFromIso8601:
    def test_parses_z_form_to_utc(self) -> None:
        parsed = from_iso8601('2026-06-01T00:00:00Z')
        assert parsed == datetime(2026, 6, 1, 0, 0, 0, tzinfo=UTC)
        assert parsed.tzinfo is UTC

    def test_normalizes_offset_to_utc(self) -> None:
        parsed = from_iso8601('2026-06-01T05:00:00+05:00')
        assert parsed == datetime(2026, 6, 1, 0, 0, 0, tzinfo=UTC)
        assert parsed.tzinfo is UTC

    def test_preserves_subsecond(self) -> None:
        parsed = from_iso8601('2026-06-01T00:00:00.123456Z')
        assert parsed == datetime(2026, 6, 1, 0, 0, 0, 123456, tzinfo=UTC)

    def test_rejects_naive_no_offset(self) -> None:
        with pytest.raises(ValueError, match='offset'):
            from_iso8601('2026-06-01T00:00:00')

    def test_rejects_date_only(self) -> None:
        with pytest.raises(ValueError, match='offset'):
            from_iso8601('2026-06-01')

    def test_rejects_unparseable(self) -> None:
        with pytest.raises(ValueError, match='isoformat'):
            from_iso8601('not a datetime')


class TestRoundTrip:
    @pytest.mark.parametrize(
        'moment',
        [
            datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC),
            datetime(2026, 6, 15, 12, 30, 45, tzinfo=UTC),
            datetime(2026, 12, 31, 23, 59, 59, tzinfo=UTC),
        ],
    )
    def test_iso_round_trip_at_seconds_precision(self, moment: datetime) -> None:
        assert from_iso8601(to_iso8601(moment)) == moment
