# tests/paths/test_partitions.py
"""Tests for fleetpull.paths.partitions."""

from datetime import date

import pytest

from fleetpull.paths import date_partition_segment, parse_date_partition_segment


class TestDatePartitionSegment:
    def test_formats_iso_date_with_prefix(self) -> None:
        assert date_partition_segment(date(2026, 6, 1)) == 'date=2026-06-01'

    def test_zero_pads_month_and_day(self) -> None:
        assert date_partition_segment(date(2026, 1, 5)) == 'date=2026-01-05'


class TestParseDatePartitionSegment:
    def test_round_trips_with_the_formatter(self) -> None:
        partition_date = date(2026, 6, 1)
        assert (
            parse_date_partition_segment(date_partition_segment(partition_date))
            == partition_date
        )

    def test_parses_a_literal_segment(self) -> None:
        assert parse_date_partition_segment('date=2026-12-31') == date(2026, 12, 31)

    def test_rejects_a_missing_prefix(self) -> None:
        with pytest.raises(ValueError, match='prefix'):
            parse_date_partition_segment('2026-06-01')

    def test_rejects_a_foreign_name(self) -> None:
        with pytest.raises(ValueError, match='prefix'):
            parse_date_partition_segment('metadata.json')

    def test_rejects_a_malformed_date(self) -> None:
        with pytest.raises(ValueError, match='malformed'):
            parse_date_partition_segment('date=2026-13-99')

    def test_rejects_an_empty_date(self) -> None:
        with pytest.raises(ValueError, match='malformed'):
            parse_date_partition_segment('date=')
