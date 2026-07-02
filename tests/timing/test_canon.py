# tests/timing/test_canon.py
"""Tests for fleetpull.timing.canon."""

from datetime import UTC, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from fleetpull.timing.canon import ensure_utc, require_utc

PLUS_FIVE: timezone = timezone(timedelta(hours=5))


class TestEnsureUtc:
    def test_normalizes_zoneinfo_utc_to_canonical(self) -> None:
        # The live-crash fingerprint: zero offset, foreign tzinfo identity.
        moment = datetime(2026, 6, 1, 12, 0, tzinfo=ZoneInfo('UTC'))
        normalized = ensure_utc(moment)
        assert normalized == moment
        assert normalized.tzinfo is UTC

    def test_converts_offset_preserving_the_instant(self) -> None:
        moment = datetime(2026, 6, 1, 17, 0, tzinfo=PLUS_FIVE)
        normalized = ensure_utc(moment)
        assert normalized == datetime(2026, 6, 1, 12, 0, tzinfo=UTC)
        assert normalized.tzinfo is UTC

    def test_rejects_naive(self) -> None:
        with pytest.raises(ValueError, match='timezone-aware'):
            ensure_utc(datetime(2026, 6, 1, 12, 0))  # noqa: DTZ001

    def test_canonical_input_returns_equal_and_canonical(self) -> None:
        moment = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)
        normalized = ensure_utc(moment)
        assert normalized == moment
        assert normalized.tzinfo is UTC


class TestRequireUtc:
    def test_canonical_passes_through_unchanged(self) -> None:
        moment = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)
        assert require_utc(moment) is moment

    def test_rejects_naive(self) -> None:
        with pytest.raises(ValueError, match='timezone-aware'):
            require_utc(datetime(2026, 6, 1, 12, 0))  # noqa: DTZ001

    def test_rejects_foreign_offset_tzinfo(self) -> None:
        with pytest.raises(ValueError, match=r'must use datetime\.UTC'):
            require_utc(datetime(2026, 6, 1, 12, 0, tzinfo=PLUS_FIVE))

    def test_rejects_zero_offset_foreign_tzinfo(self) -> None:
        # Identity, not offset-equality: ZoneInfo('UTC') is the same instant
        # arithmetic but the wrong tzinfo -- the missed-ingress fingerprint.
        with pytest.raises(ValueError, match=r'must use datetime\.UTC'):
            require_utc(datetime(2026, 6, 1, 12, 0, tzinfo=ZoneInfo('UTC')))
