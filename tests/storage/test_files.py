"""Tests for fleetpull.storage.files."""

from datetime import date
from pathlib import Path

from fleetpull.storage.files import (
    data_file,
    partition_part_file,
    temp_sibling_path,
)


def test_data_file_appends_data_parquet() -> None:
    assert data_file(Path('/d/motive/vehicles')) == Path(
        '/d/motive/vehicles/data.parquet'
    )


def test_temp_sibling_is_in_same_directory() -> None:
    target = Path('/d/motive/vehicles/data.parquet')
    assert temp_sibling_path(target).parent == target.parent


def test_temp_sibling_is_unique() -> None:
    target = Path('/d/data.parquet')
    assert temp_sibling_path(target) != temp_sibling_path(target)


def test_temp_sibling_has_tmp_suffix() -> None:
    assert temp_sibling_path(Path('/d/data.parquet')).name.endswith('.tmp')


class TestPartitionPartFile:
    def test_builds_hive_partition_part_path(self) -> None:
        result = partition_part_file(
            Path('/d/motive/vehicle_locations'), date(2026, 6, 1)
        )
        assert result == Path(
            '/d/motive/vehicle_locations/date=2026-06-01/part.parquet'
        )

    def test_part_file_and_partition_dir_names(self) -> None:
        result = partition_part_file(Path('/d/m/e'), date(2026, 1, 5))
        assert result.name == 'part.parquet'
        assert result.parent.name == 'date=2026-01-05'
