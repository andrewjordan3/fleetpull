"""Tests for fleetpull.config.logger."""

import logging
from pathlib import Path

import pytest
from pydantic import ValidationError

from fleetpull.config.logger import LoggerConfig

__all__: list[str] = []


class TestDefaults:
    def test_bare_config_defaults(self) -> None:
        config = LoggerConfig()
        assert config.console_level == logging.INFO
        assert config.file_path is None
        assert config.file_level == logging.DEBUG


class TestLevelNameCoercion:
    @pytest.mark.parametrize(
        ('level_name', 'expected_level'),
        [
            ('debug', logging.DEBUG),
            ('INFO', logging.INFO),
            (' Warning ', logging.WARNING),
        ],
    )
    def test_console_level_accepts_names(
        self, level_name: str, expected_level: int
    ) -> None:
        config = LoggerConfig(console_level=level_name)  # type: ignore[arg-type]
        assert config.console_level == expected_level

    @pytest.mark.parametrize(
        ('level_name', 'expected_level'),
        [
            ('debug', logging.DEBUG),
            ('INFO', logging.INFO),
            (' Warning ', logging.WARNING),
        ],
    )
    def test_file_level_accepts_names(
        self, level_name: str, expected_level: int
    ) -> None:
        config = LoggerConfig(file_level=level_name)  # type: ignore[arg-type]
        assert config.file_level == expected_level

    def test_unknown_level_name_raises_naming_it(self) -> None:
        with pytest.raises(ValidationError, match='verbose'):
            LoggerConfig(console_level='verbose')  # type: ignore[arg-type]

    @pytest.mark.parametrize('garbage_value', [1.5, [10]])
    def test_non_string_non_int_garbage_fails(
        self, garbage_value: float | list[int]
    ) -> None:
        with pytest.raises(ValidationError):
            LoggerConfig(console_level=garbage_value)  # type: ignore[arg-type]


class TestModelConstraints:
    def test_is_frozen(self) -> None:
        config = LoggerConfig()
        with pytest.raises(ValidationError):
            config.console_level = logging.ERROR  # type: ignore[misc]

    def test_unknown_field_raises(self) -> None:
        with pytest.raises(ValidationError):
            LoggerConfig(file_path=Path('fleet.log'), rotation='daily')  # type: ignore[call-arg]
