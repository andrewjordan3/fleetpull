"""Tests for fleetpull.storage.result."""

from fleetpull.storage.result import WriteResult


def test_deleted_partitions_defaults_to_empty_list() -> None:
    result = WriteResult(rows_written=2, duplicates_dropped=0, files_written=1)
    assert result.deleted_partitions == []
