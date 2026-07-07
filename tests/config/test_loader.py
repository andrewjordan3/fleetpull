"""Tests for fleetpull.config.loader and fleetpull.config.composition.

Every load path exercised against real temp files: the happy round-trip
with each cross-section default composed, every documented error branch
with its message shape, the credential environment fallback, the
enablement rules, secret hygiene, and the committed example file itself.
"""

import logging
from pathlib import Path

import pytest

from fleetpull.config import load_config
from fleetpull.exceptions import ConfigurationError

_SYNTHETIC_KEY = 'synthetic-motive-key-000'

_EXAMPLE_FILE = Path(__file__).parents[2] / 'config.example.yaml'


@pytest.fixture(autouse=True)
def _no_ambient_credential(monkeypatch: pytest.MonkeyPatch) -> None:
    """Strip MOTIVE_API_KEY so a developer's shell never leaks into tests."""
    monkeypatch.delenv('MOTIVE_API_KEY', raising=False)


def _write(tmp_path: Path, text: str) -> Path:
    config_path = tmp_path / 'config.yaml'
    config_path.write_text(text, encoding='utf-8')
    return config_path


def _minimal_yaml(dataset_root: Path) -> str:
    return (
        'sync:\n'
        '  default_start_date: 2026-06-01\n'
        'storage:\n'
        f'  dataset_root: {dataset_root}\n'
        'providers:\n'
        '  motive:\n'
        f"    api_key: '{_SYNTHETIC_KEY}'\n"
        '    endpoints: [vehicles]\n'
    )


class TestHappyPathDefaults:
    def test_minimal_config_parses(self, tmp_path: Path) -> None:
        config = load_config(_write(tmp_path, _minimal_yaml(tmp_path)))
        assert config.sync.default_start_date.isoformat() == '2026-06-01'
        assert config.sync.dataset_root == tmp_path
        assert config.storage.dataset_root == tmp_path

    def test_state_path_defaults_under_dataset_root(self, tmp_path: Path) -> None:
        config = load_config(_write(tmp_path, _minimal_yaml(tmp_path)))
        assert config.state.database_path == tmp_path / '.fleetpull' / 'state.sqlite3'

    def test_explicit_state_path_is_respected(self, tmp_path: Path) -> None:
        text = _minimal_yaml(tmp_path) + 'state:\n  database_path: /el/state.sqlite3\n'
        config = load_config(_write(tmp_path, text))
        assert config.state.database_path == Path('/el/state.sqlite3')

    def test_file_logging_disabled_when_no_file_key(self, tmp_path: Path) -> None:
        config = load_config(_write(tmp_path, _minimal_yaml(tmp_path)))
        assert config.logging.file_path is None
        assert config.logging.console_level == logging.INFO

    def test_file_level_alone_defaults_the_path(self, tmp_path: Path) -> None:
        text = _minimal_yaml(tmp_path) + 'logging:\n  file_level: INFO\n'
        config = load_config(_write(tmp_path, text))
        assert config.logging.file_path == tmp_path / '.fleetpull' / 'fleetpull.log'
        assert config.logging.file_level == logging.INFO

    def test_file_path_alone_defaults_the_level(self, tmp_path: Path) -> None:
        text = _minimal_yaml(tmp_path) + 'logging:\n  file_path: /var/log/fp.log\n'
        config = load_config(_write(tmp_path, text))
        assert config.logging.file_path == Path('/var/log/fp.log')
        assert config.logging.file_level == logging.DEBUG

    def test_window_knobs_fan_into_the_enabled_provider(self, tmp_path: Path) -> None:
        text = _minimal_yaml(tmp_path).replace(
            'sync:\n', 'sync:\n  lookback_days: 3\n  cutoff_days: 1\n'
        )
        config = load_config(_write(tmp_path, text))
        motive = config.providers.motive
        assert motive is not None
        assert motive.lookback_days == 3
        assert motive.cutoff_days == 1

    def test_absent_knobs_leave_provider_defaults(self, tmp_path: Path) -> None:
        config = load_config(_write(tmp_path, _minimal_yaml(tmp_path)))
        motive = config.providers.motive
        assert motive is not None
        assert motive.lookback_days == 7
        assert motive.cutoff_days == 0

    def test_rate_limit_default_is_untouched(self, tmp_path: Path) -> None:
        config = load_config(_write(tmp_path, _minimal_yaml(tmp_path)))
        motive = config.providers.motive
        assert motive is not None
        assert motive.rate_limit.requests_per_period == 60
        assert motive.rate_limit.max_concurrency == 2

    def test_http_and_retry_sections_default_wholesale(self, tmp_path: Path) -> None:
        config = load_config(_write(tmp_path, _minimal_yaml(tmp_path)))
        assert config.http.use_truststore is False
        assert config.retry.transient_max_failures == 3

    def test_string_path_argument_is_accepted(self, tmp_path: Path) -> None:
        config_path = _write(tmp_path, _minimal_yaml(tmp_path))
        assert load_config(str(config_path)).storage.dataset_root == tmp_path


class TestErrorPaths:
    def test_missing_file_names_the_path(self, tmp_path: Path) -> None:
        missing = tmp_path / 'absent.yaml'
        with pytest.raises(ConfigurationError, match='config file not found'):
            load_config(missing)

    def test_parse_error_names_the_line(self, tmp_path: Path) -> None:
        config_path = _write(tmp_path, 'sync:\n  default_start_date: [unclosed\n')
        with pytest.raises(ConfigurationError, match=r'not valid YAML.*line \d+'):
            load_config(config_path)

    def test_non_mapping_top_level_is_rejected(self, tmp_path: Path) -> None:
        config_path = _write(tmp_path, '- a\n- b\n')
        with pytest.raises(ConfigurationError, match='mapping at the top level'):
            load_config(config_path)

    def test_empty_file_names_the_missing_sections(self, tmp_path: Path) -> None:
        with pytest.raises(ConfigurationError) as raised:
            load_config(_write(tmp_path, '\n'))
        message = str(raised.value)
        assert 'sync' in message
        assert 'storage' in message
        assert 'providers' in message

    def test_unknown_top_level_key_is_named(self, tmp_path: Path) -> None:
        text = _minimal_yaml(tmp_path) + 'sink: {}\n'
        with pytest.raises(ConfigurationError, match='sink'):
            load_config(_write(tmp_path, text))

    def test_unknown_nested_key_is_named_with_its_path(self, tmp_path: Path) -> None:
        text = _minimal_yaml(tmp_path).replace(
            '    endpoints:', '    api_keyy: x\n    endpoints:'
        )
        with pytest.raises(ConfigurationError, match=r'providers\.motive\.api_keyy'):
            load_config(_write(tmp_path, text))

    def test_missing_required_key_is_named(self, tmp_path: Path) -> None:
        # The section stays a mapping so the missing KEY is what gets named.
        text = _minimal_yaml(tmp_path).replace(
            '  default_start_date: 2026-06-01\n', '  lookback_days: 3\n'
        )
        with pytest.raises(
            ConfigurationError, match=r'sync\.default_start_date.*required'
        ):
            load_config(_write(tmp_path, text))

    def test_missing_storage_never_mentions_the_injected_key(
        self, tmp_path: Path
    ) -> None:
        text = 'sync:\n  default_start_date: 2026-06-01\nproviders: {}\n'
        with pytest.raises(ConfigurationError, match='storage') as raised:
            load_config(_write(tmp_path, text))
        assert 'sync.dataset_root' not in str(raised.value)

    def test_sync_dataset_root_is_masked(self, tmp_path: Path) -> None:
        text = _minimal_yaml(tmp_path).replace(
            'sync:\n', 'sync:\n  dataset_root: /elsewhere\n'
        )
        with pytest.raises(ConfigurationError, match=r"'sync\.dataset_root'"):
            load_config(_write(tmp_path, text))

    @pytest.mark.parametrize('masked_key', ['lookback_days', 'cutoff_days'])
    def test_per_provider_window_knobs_are_masked(
        self, tmp_path: Path, masked_key: str
    ) -> None:
        text = _minimal_yaml(tmp_path) + f'    {masked_key}: 2\n'
        with pytest.raises(
            ConfigurationError, match=rf"'providers\.motive\.{masked_key}'"
        ):
            load_config(_write(tmp_path, text))


class TestCredentialResolution:
    def test_endpoints_without_credential_names_field_and_env_var(
        self, tmp_path: Path
    ) -> None:
        text = _minimal_yaml(tmp_path).replace(f"    api_key: '{_SYNTHETIC_KEY}'\n", '')
        with pytest.raises(ConfigurationError) as raised:
            load_config(_write(tmp_path, text))
        message = str(raised.value)
        assert 'providers.motive.api_key' in message
        assert 'MOTIVE_API_KEY' in message

    def test_environment_fallback_resolves(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv('MOTIVE_API_KEY', 'env-synthetic-key')
        text = _minimal_yaml(tmp_path).replace(f"    api_key: '{_SYNTHETIC_KEY}'\n", '')
        config = load_config(_write(tmp_path, text))
        motive = config.providers.motive
        assert motive is not None
        assert motive.api_key is not None
        assert motive.api_key.get_secret_value() == 'env-synthetic-key'

    def test_yaml_literal_wins_over_the_environment(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv('MOTIVE_API_KEY', 'env-synthetic-key')
        config = load_config(_write(tmp_path, _minimal_yaml(tmp_path)))
        motive = config.providers.motive
        assert motive is not None
        assert motive.api_key is not None
        assert motive.api_key.get_secret_value() == _SYNTHETIC_KEY

    def test_empty_environment_value_counts_as_unset(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv('MOTIVE_API_KEY', '')
        text = _minimal_yaml(tmp_path).replace(f"    api_key: '{_SYNTHETIC_KEY}'\n", '')
        with pytest.raises(ConfigurationError, match='MOTIVE_API_KEY'):
            load_config(_write(tmp_path, text))

    def test_credential_without_endpoints_warns_and_disables(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        text = _minimal_yaml(tmp_path).replace('    endpoints: [vehicles]\n', '')
        with caplog.at_level(logging.WARNING, logger='fleetpull.config.composition'):
            config = load_config(_write(tmp_path, text))
        warnings = [
            record for record in caplog.records if record.levelno == logging.WARNING
        ]
        assert len(warnings) == 1
        assert 'disabled' in warnings[0].getMessage()
        motive = config.providers.motive
        assert motive is not None
        assert motive.endpoints == ()

    def test_absent_provider_is_silent(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        text = (
            'sync:\n  default_start_date: 2026-06-01\n'
            f'storage:\n  dataset_root: {tmp_path}\n'
            'providers: {}\n'
        )
        with caplog.at_level(logging.WARNING):
            config = load_config(_write(tmp_path, text))
        assert config.providers.motive is None
        assert not caplog.records


class TestSecretHygiene:
    def test_secret_never_appears_in_errors_or_reprs(self, tmp_path: Path) -> None:
        text = _minimal_yaml(tmp_path) + 'sink: {}\n'
        with pytest.raises(ConfigurationError) as raised:
            load_config(_write(tmp_path, text))
        assert _SYNTHETIC_KEY not in str(raised.value)
        assert _SYNTHETIC_KEY not in repr(raised.value)

    def test_secret_never_appears_in_the_loaded_config_repr(
        self, tmp_path: Path
    ) -> None:
        config = load_config(_write(tmp_path, _minimal_yaml(tmp_path)))
        assert _SYNTHETIC_KEY not in repr(config)
        assert _SYNTHETIC_KEY not in str(config)


class TestExampleFile:
    def test_committed_example_loads_green(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv('MOTIVE_API_KEY', 'env-synthetic-key')
        example_text = _EXAMPLE_FILE.read_text(encoding='utf-8')
        pointed = example_text.replace('/data/fleetpull', str(tmp_path))
        config = load_config(_write(tmp_path, pointed))
        motive = config.providers.motive
        assert motive is not None
        assert motive.endpoints == ('vehicles', 'vehicle_locations')
        assert config.state.database_path == tmp_path / '.fleetpull' / 'state.sqlite3'
