"""Tests for fleetpull.paths.datasets."""

from pathlib import Path

from fleetpull.paths import endpoint_directory


def test_joins_root_provider_endpoint() -> None:
    result = endpoint_directory('/data', 'motive', 'vehicles')
    assert result == Path('/data/motive/vehicles')


def test_normalizes_relative_root_to_absolute() -> None:
    result = endpoint_directory('data', 'motive', 'vehicles')
    assert result.is_absolute()
    assert result.parts[-3:] == ('data', 'motive', 'vehicles')


def test_does_not_create_the_directory(tmp_path: Path) -> None:
    result = endpoint_directory(tmp_path, 'motive', 'vehicles')
    assert not result.exists()
