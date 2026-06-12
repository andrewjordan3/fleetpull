"""Tests for fleetpull.config.logger."""

import logging
from pathlib import Path

import pytest
from pydantic import ValidationError

from fleetpull.config.logger import LoggerConfig

LEVEL_FIELDS: tuple[str, str] = ('console_level', 'file_level')


class TestDefaults:
    def test_bare_config_defaults(self) -> None:
        config = LoggerConfig()
        assert config.console_level == logging.INFO
        assert config.file_path is None
        assert config.file_level == logging.DEBUG


class TestLevelNameCoercion:
    @pytest.mark.parametrize('field_name', LEVEL_FIELDS)
    @pytest.mark.parametrize(
        ('level_name', 'expected_level'),
        [
            ('debug', logging.DEBUG),
            ('INFO', logging.INFO),
            (' Warning ', logging.WARNING),
        ],
    )
    def test_level_fields_accept_names(
        self, field_name: str, level_name: str, expected_level: int
    ) -> None:
        config = LoggerConfig(**{field_name: level_name})
        assert getattr(config, field_name) == expected_level

    def test_unknown_level_name_raises_naming_it(self) -> None:
        with pytest.raises(ValidationError, match='verbose'):
            LoggerConfig(console_level='verbose')

    @pytest.mark.parametrize('field_name', LEVEL_FIELDS)
    @pytest.mark.parametrize('deprecated_name', ['WARN', 'FATAL', 'NOTSET'])
    def test_deprecated_aliases_and_notset_rejected(
        self, field_name: str, deprecated_name: str
    ) -> None:
        with pytest.raises(ValidationError, match='not a recognized log level'):
            LoggerConfig(**{field_name: deprecated_name})


class TestLevelIntegerValidation:
    @pytest.mark.parametrize('field_name', LEVEL_FIELDS)
    @pytest.mark.parametrize('standard_level', [10, 50])
    def test_standard_integers_pass_through_unchanged(
        self, field_name: str, standard_level: int
    ) -> None:
        config = LoggerConfig(**{field_name: standard_level})
        assert getattr(config, field_name) == standard_level

    @pytest.mark.parametrize('field_name', LEVEL_FIELDS)
    @pytest.mark.parametrize('boolean_value', [True, False])
    def test_booleans_rejected_mentioning_bool(
        self, field_name: str, boolean_value: bool
    ) -> None:
        with pytest.raises(ValidationError, match='bool'):
            LoggerConfig(**{field_name: boolean_value})

    @pytest.mark.parametrize('field_name', LEVEL_FIELDS)
    @pytest.mark.parametrize('nonstandard_level', [999, 0])
    def test_nonstandard_integers_rejected_listing_allowed_pairs(
        self, field_name: str, nonstandard_level: int
    ) -> None:
        with pytest.raises(ValidationError, match='WARNING=30'):
            LoggerConfig(**{field_name: nonstandard_level})

    @pytest.mark.parametrize('garbage_value', [1.5, [10]])
    def test_non_string_non_int_garbage_fails(
        self, garbage_value: float | list[int]
    ) -> None:
        with pytest.raises(ValidationError):
            LoggerConfig(console_level=garbage_value)


class TestFilePathNormalization:
    def test_tilde_expands_to_home(self) -> None:
        config = LoggerConfig(file_path='~/x.log')
        assert config.file_path is not None
        assert config.file_path.is_absolute()
        # .resolve() on the expectation too: the production contract is
        # symlink-dereferencing resolution, and home/temp paths traverse
        # symlinks on some platforms (e.g. macOS /var -> /private/var).
        assert config.file_path == (Path.home() / 'x.log').resolve()

    def test_relative_path_resolves_against_cwd(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        # chdir first — without it this test would nondeterministically
        # resolve against wherever pytest was invoked.
        monkeypatch.chdir(tmp_path)
        config = LoggerConfig(file_path='x.log')
        assert config.file_path == (tmp_path / 'x.log').resolve()

    def test_path_instance_gets_same_normalization(self) -> None:
        config = LoggerConfig(file_path=Path('~/x.log'))
        assert config.file_path == (Path.home() / 'x.log').resolve()

    def test_none_stays_none(self) -> None:
        config = LoggerConfig(file_path=None)
        assert config.file_path is None

    def test_non_str_non_path_rejected_naming_type(self) -> None:
        with pytest.raises(ValidationError, match='int'):
            LoggerConfig(file_path=123)


class TestModelConstraints:
    def test_is_frozen(self) -> None:
        config = LoggerConfig()
        with pytest.raises(ValidationError):
            config.console_level = logging.ERROR  # type: ignore[misc]

    def test_unknown_field_raises(self) -> None:
        with pytest.raises(ValidationError):
            LoggerConfig(file_path=Path('fleet.log'), rotation='daily')  # type: ignore[call-arg]
