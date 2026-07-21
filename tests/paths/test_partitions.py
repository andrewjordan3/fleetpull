# tests/paths/test_partitions.py
"""Tests for fleetpull.paths.partitions."""

from datetime import date

from fleetpull.paths import date_partition_segment


class TestDatePartitionSegment:
    def test_formats_iso_date_with_prefix(self) -> None:
        assert date_partition_segment(date(2026, 6, 1)) == 'date=2026-06-01'

    def test_zero_pads_month_and_day(self) -> None:
        assert date_partition_segment(date(2026, 1, 5)) == 'date=2026-01-05'

    def test_pins_the_segment_grammar(self) -> None:
        # The hive-segment grammar, pinned directly: a 'date=' prefix
        # followed by the zero-padded ISO calendar date, nothing else.
        # BigQuery external tables and any future directory-reading layer
        # stand on exactly this shape.
        assert date_partition_segment(date(2026, 12, 31)) == 'date=2026-12-31'
        assert date_partition_segment(date(1, 2, 3)) == 'date=0001-02-03'
