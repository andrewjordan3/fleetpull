"""Tests for fleetpull.cli."""

from pathlib import Path

import pytest

import fleetpull.cli
from fleetpull.cli import main
from fleetpull.config import read_example_config
from fleetpull.exceptions import ConfigurationError


def test_sync_success_returns_zero_and_passes_the_config_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    received: list[Path] = []

    class RecordingSync:
        def __init__(self, config_path: Path) -> None:
            received.append(config_path)

        def run(self) -> None:
            return None

    monkeypatch.setattr(fleetpull.cli, 'Sync', RecordingSync)
    assert main(['sync', 'fleetpull_config.yaml']) == 0
    assert received == [Path('fleetpull_config.yaml')]


def test_operational_failure_exits_one_with_the_message_on_stderr(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    class FailingSync:
        def __init__(self, config_path: Path) -> None:
            pass

        def run(self) -> None:
            raise ConfigurationError('no providers enabled')

    monkeypatch.setattr(fleetpull.cli, 'Sync', FailingSync)
    assert main(['sync', 'fleetpull_config.yaml']) == 1
    captured = capsys.readouterr()
    assert 'no providers enabled' in captured.err
    assert captured.err.endswith('\n')
    assert captured.out == ''


def test_missing_subcommand_exits_with_argparse_code_two() -> None:
    with pytest.raises(SystemExit) as exit_info:
        main([])
    assert exit_info.value.code == 2


def test_sync_without_a_config_path_exits_with_argparse_code_two() -> None:
    with pytest.raises(SystemExit) as exit_info:
        main(['sync'])
    assert exit_info.value.code == 2


def test_init_config_writes_the_example_and_reports_the_path(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    target = tmp_path / 'my_config.yaml'
    assert main(['init-config', str(target)]) == 0
    assert target.exists()
    # The materialized file is the packaged example verbatim (its
    # green-loading is pinned in tests/config).
    assert target.read_text(encoding='utf-8') == read_example_config()
    captured = capsys.readouterr()
    assert str(target) in captured.out


def test_init_config_defaults_to_the_conventional_filename(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # No path given: the default lands in the current working directory.
    monkeypatch.chdir(tmp_path)
    assert main(['init-config']) == 0
    assert (tmp_path / 'fleetpull_config.yaml').exists()


def test_init_config_writes_into_a_directory(tmp_path: Path) -> None:
    # An existing directory receives the default filename inside it.
    assert main(['init-config', str(tmp_path)]) == 0
    assert (tmp_path / 'fleetpull_config.yaml').exists()


def test_init_config_refuses_to_overwrite_without_force(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    target = tmp_path / 'existing.yaml'
    target.write_text('do not clobber me', encoding='utf-8')
    assert main(['init-config', str(target)]) == 1
    assert target.read_text(encoding='utf-8') == 'do not clobber me'
    captured = capsys.readouterr()
    assert 'already exists' in captured.err


def test_init_config_force_overwrites(tmp_path: Path) -> None:
    target = tmp_path / 'existing.yaml'
    target.write_text('stale', encoding='utf-8')
    assert main(['init-config', str(target), '--force']) == 0
    assert target.read_text(encoding='utf-8') == read_example_config()
