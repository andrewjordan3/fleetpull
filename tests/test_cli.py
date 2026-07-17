"""Tests for fleetpull.cli."""

from pathlib import Path

import pytest

import fleetpull.cli
from fleetpull.cli import main
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
