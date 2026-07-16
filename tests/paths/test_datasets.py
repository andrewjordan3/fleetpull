"""Tests for fleetpull.paths.datasets."""

from pathlib import Path

from fleetpull.paths import endpoint_directory


def test_joins_root_provider_endpoint(tmp_path: Path) -> None:
    # A real absolute root, not a POSIX literal: on Windows, resolution
    # anchors '/data' to the drive and the literal never round-trips.
    result = endpoint_directory(str(tmp_path), 'motive', 'vehicles')
    assert result == tmp_path / 'motive' / 'vehicles'


def test_normalizes_relative_root_to_absolute() -> None:
    result = endpoint_directory('data', 'motive', 'vehicles')
    assert result.is_absolute()
    assert result.parts[-3:] == ('data', 'motive', 'vehicles')


def test_does_not_create_the_directory(tmp_path: Path) -> None:
    result = endpoint_directory(tmp_path, 'motive', 'vehicles')
    assert not result.exists()
