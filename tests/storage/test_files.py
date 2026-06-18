"""Tests for fleetpull.storage.files."""

from pathlib import Path

from fleetpull.storage.files import data_file, temp_sibling_path


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
