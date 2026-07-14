"""Tests for fleetpull.storage.files."""

from datetime import date
from pathlib import Path

from fleetpull.storage.files import (
    data_file,
    partition_dir,
    partition_part_file,
    partition_staging_dir,
    partition_staging_shard,
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


class TestPartitionDir:
    def test_builds_hive_partition_directory(self) -> None:
        result = partition_dir(Path('/d/m/vehicle_locations'), date(2026, 6, 1))
        assert result == Path('/d/m/vehicle_locations/date=2026-06-01')

    def test_directory_name_is_the_date_segment(self) -> None:
        result = partition_dir(Path('/d/m/e'), date(2026, 6, 1))
        assert result.name == 'date=2026-06-01'


class TestPartitionStaging:
    def test_staging_dir_is_inside_the_partition(self) -> None:
        result = partition_staging_dir(Path('/d/m/vehicle_locations'), date(2026, 6, 1))
        assert result == Path('/d/m/vehicle_locations/.staging/date=2026-06-01')

    def test_shard_parent_is_the_staging_dir(self) -> None:
        endpoint_dir = Path('/d/m/vehicle_locations')
        partition_date = date(2026, 6, 1)
        shard = partition_staging_shard(endpoint_dir, partition_date)
        assert shard.parent == partition_staging_dir(endpoint_dir, partition_date)

    def test_shard_name_is_a_shard_file(self) -> None:
        shard = partition_staging_shard(Path('/d/m/e'), date(2026, 6, 1))
        assert shard.name.startswith('shard-')
        assert shard.name.endswith('.shard')

    def test_two_shards_for_one_date_differ(self) -> None:
        endpoint_dir = Path('/d/m/e')
        partition_date = date(2026, 6, 1)
        first = partition_staging_shard(endpoint_dir, partition_date)
        second = partition_staging_shard(endpoint_dir, partition_date)
        assert first != second
