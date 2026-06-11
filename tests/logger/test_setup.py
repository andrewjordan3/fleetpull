"""Tests for fleetpull.logger.setup.

setup_logger mutates global state (the 'fleetpull' logger), so an
autouse fixture snapshots and restores that logger's handlers, level,
and propagate flag around every test — otherwise these tests would
poison the logging behavior of every other test module.
"""

import logging
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from fleetpull.config.logger import LoggerConfig
from fleetpull.logger.setup import _DATE_FORMAT, setup_logger

__all__: list[str] = []


@pytest.fixture(autouse=True)
def restore_package_logger() -> Iterator[None]:
    package_logger = logging.getLogger('fleetpull')
    saved_handlers = list(package_logger.handlers)
    saved_level = package_logger.level
    saved_propagate = package_logger.propagate
    yield
    for attached_handler in package_logger.handlers:
        if attached_handler not in saved_handlers:
            attached_handler.close()
    package_logger.handlers = saved_handlers
    package_logger.setLevel(saved_level)
    package_logger.propagate = saved_propagate


def package_logger() -> logging.Logger:
    return logging.getLogger('fleetpull')


class TestConsoleOnlySetup:
    def test_single_stream_handler_at_console_level(self) -> None:
        setup_logger(LoggerConfig(console_level=logging.WARNING))
        attached_handlers = package_logger().handlers
        assert len(attached_handlers) == 1
        assert isinstance(attached_handlers[0], logging.StreamHandler)
        assert attached_handlers[0].level == logging.WARNING

    def test_logger_level_equals_console_level(self) -> None:
        setup_logger(LoggerConfig(console_level=logging.WARNING))
        assert package_logger().level == logging.WARNING

    def test_propagate_is_disabled(self) -> None:
        setup_logger(LoggerConfig())
        assert package_logger().propagate is False

    def test_idempotent_handler_count(self) -> None:
        setup_logger(LoggerConfig())
        setup_logger(LoggerConfig())
        assert len(package_logger().handlers) == 1

    def test_file_level_inert_without_file_path(self) -> None:
        setup_logger(
            LoggerConfig(
                console_level=logging.INFO,
                file_level=logging.DEBUG,
                file_path=None,
            )
        )
        assert package_logger().level == logging.INFO


class TestFileSetup:
    def test_file_handler_attached_at_file_level(self, tmp_path: Path) -> None:
        setup_logger(
            LoggerConfig(
                console_level=logging.INFO,
                file_path=tmp_path / 'fleet.log',
                file_level=logging.DEBUG,
            )
        )
        attached_handlers = package_logger().handlers
        assert len(attached_handlers) == 2
        file_handlers = [
            attached_handler
            for attached_handler in attached_handlers
            if isinstance(attached_handler, logging.FileHandler)
        ]
        assert len(file_handlers) == 1
        assert file_handlers[0].level == logging.DEBUG

    def test_logger_level_is_min_of_handler_levels(self, tmp_path: Path) -> None:
        setup_logger(
            LoggerConfig(
                console_level=logging.INFO,
                file_path=tmp_path / 'fleet.log',
                file_level=logging.DEBUG,
            )
        )
        assert package_logger().level == logging.DEBUG

    def test_missing_parent_directory_is_created(self, tmp_path: Path) -> None:
        nested_log_path = tmp_path / 'logs' / 'nested' / 'fleet.log'
        setup_logger(LoggerConfig(file_path=nested_log_path))
        assert nested_log_path.parent.is_dir()


class TestUtcTimestamps:
    def test_formatter_uses_gmtime_and_module_date_format(self) -> None:
        setup_logger(LoggerConfig())
        for attached_handler in package_logger().handlers:
            handler_formatter = attached_handler.formatter
            assert handler_formatter is not None
            assert handler_formatter.converter is time.gmtime
            assert handler_formatter.datefmt == _DATE_FORMAT


class TestConfirmationRecord:
    def test_confirmation_emitted_on_stderr(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # capsys, not caplog: propagate=False means records never reach
        # the root logger that caplog listens on — by design.
        setup_logger(LoggerConfig(console_level=logging.INFO))
        captured_stderr: str = capsys.readouterr().err
        assert 'Logging configured' in captured_stderr
        assert 'INFO' in captured_stderr
