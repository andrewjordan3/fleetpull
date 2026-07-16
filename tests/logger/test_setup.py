"""Tests for fleetpull.logger.setup.

setup_logger mutates global state (the 'fleetpull' logger); the
suite-wide autouse fixture in ``tests/conftest.py`` snapshots and
restores that logger's handlers, level, and propagate flag around
every test (promoted from this module once other modules -- any test
driving ``Sync.run()`` -- proved to carry the same hazard).
"""

import logging
import time
from pathlib import Path

import pytest

from fleetpull.config.logger import LoggerConfig
from fleetpull.logger.setup import _DATE_FORMAT, setup_logger


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

    def test_reconfiguration_closes_previous_file_handler(self, tmp_path: Path) -> None:
        setup_logger(LoggerConfig(file_path=tmp_path / 'first.log'))
        previous_file_handler = next(
            attached_handler
            for attached_handler in package_logger().handlers
            if isinstance(attached_handler, logging.FileHandler)
        )
        # Capture the stream before reconfiguring: closed-ness is the
        # observable handler state (FileHandler.close() closes it).
        previous_stream = previous_file_handler.stream
        assert previous_stream is not None
        setup_logger(LoggerConfig())
        assert previous_stream.closed


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
