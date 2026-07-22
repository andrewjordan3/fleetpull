"""Tests for fleetpull.config.example."""

from pathlib import Path

import pytest

from fleetpull.config import FleetpullConfig
from fleetpull.config.example import (
    EXAMPLE_CONFIG_FILENAME,
    read_example_config,
    write_example_config,
)


def test_read_returns_the_packaged_yaml_document() -> None:
    text = read_example_config()
    # The packaged resource is present and is the config we ship: it has
    # the sections a user edits and parses green through the real loader.
    assert 'providers:' in text
    assert 'storage:' in text


def test_read_is_loadable_by_the_real_config_loader(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The example resolves credentials from the environment (its api_key
    # lines are commented out), so a green load needs them present.
    monkeypatch.setenv('MOTIVE_API_KEY', 'env-synthetic-key')
    monkeypatch.setenv('GEOTAB_PASSWORD', 'env-synthetic-pass')
    monkeypatch.setenv('SAMSARA_API_KEY', 'env-synthetic-token')
    target = tmp_path / EXAMPLE_CONFIG_FILENAME
    text = read_example_config().replace('/data/fleetpull', str(tmp_path))
    target.write_text(text, encoding='utf-8')
    # Loads without raising -- the shipped example is always valid config.
    FleetpullConfig.from_yaml(target)


def test_write_returns_the_written_path(tmp_path: Path) -> None:
    target = tmp_path / 'out.yaml'
    written = write_example_config(target)
    assert written == target
    assert target.read_text(encoding='utf-8') == read_example_config()


def test_write_into_a_directory_appends_the_default_filename(tmp_path: Path) -> None:
    written = write_example_config(tmp_path)
    assert written == tmp_path / EXAMPLE_CONFIG_FILENAME
    assert written.exists()


def test_write_refuses_to_overwrite_without_force(tmp_path: Path) -> None:
    target = tmp_path / 'existing.yaml'
    target.write_text('keep me', encoding='utf-8')
    with pytest.raises(FileExistsError, match='already exists'):
        write_example_config(target)
    assert target.read_text(encoding='utf-8') == 'keep me'


def test_write_force_overwrites(tmp_path: Path) -> None:
    target = tmp_path / 'existing.yaml'
    target.write_text('stale', encoding='utf-8')
    write_example_config(target, force=True)
    assert target.read_text(encoding='utf-8') == read_example_config()
