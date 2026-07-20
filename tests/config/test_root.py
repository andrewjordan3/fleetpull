"""Tests for fleetpull.config.root -- FleetpullConfig and from_yaml.

The full loading surface against real temp files: the round-trip with
every documented default resolved, the knob-precedence matrix, path
normalization on every path field, the logging either-key matrix,
enablement at validation, the environment fallback, secret hygiene, the
error branches, and the committed example file itself.
"""

import logging
from datetime import date
from pathlib import Path

import pytest

from fleetpull.config import (
    FleetpullConfig,
    MotiveConfig,
    ProvidersConfig,
    StorageConfig,
    SyncConfig,
)
from fleetpull.exceptions import ConfigurationError
from fleetpull.paths import resolve_path

_SYNTHETIC_KEY = 'synthetic-motive-key-000'

_EXAMPLE_FILE = Path(__file__).parents[2] / 'config.example.yaml'


@pytest.fixture(autouse=True)
def _no_ambient_credential(monkeypatch: pytest.MonkeyPatch) -> None:
    """Strip credential variables so a developer's shell never leaks into tests."""
    monkeypatch.delenv('MOTIVE_API_KEY', raising=False)
    monkeypatch.delenv('GEOTAB_PASSWORD', raising=False)
    monkeypatch.delenv('SAMSARA_API_KEY', raising=False)


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


class TestFromYamlDefaults:
    def test_minimal_config_round_trips(self, tmp_path: Path) -> None:
        config = FleetpullConfig.from_yaml(_write(tmp_path, _minimal_yaml(tmp_path)))
        assert config.sync.default_start_date == date(2026, 6, 1)
        assert config.sync.lookback_days is None
        assert config.storage.dataset_root == resolve_path(tmp_path)
        motive = config.providers.motive
        assert motive is not None
        assert motive.endpoints == ('vehicles',)
        assert motive.api_key is not None
        assert motive.api_key.get_secret_value() == _SYNTHETIC_KEY

    def test_state_path_defaults_under_dataset_root(self, tmp_path: Path) -> None:
        config = FleetpullConfig.from_yaml(_write(tmp_path, _minimal_yaml(tmp_path)))
        expected = resolve_path(tmp_path) / '.fleetpull' / 'state.sqlite3'
        assert config.state.database_path == expected

    def test_explicit_state_path_stands(self, tmp_path: Path) -> None:
        # A real absolute path, not a POSIX literal: on Windows, resolution
        # anchors '/el/...' to the drive and the literal never round-trips.
        explicit = tmp_path / 'el' / 'state.sqlite3'
        text = (
            _minimal_yaml(tmp_path)
            + f'state:\n  database_path: {explicit.as_posix()}\n'
        )
        config = FleetpullConfig.from_yaml(_write(tmp_path, text))
        assert config.state.database_path == resolve_path(explicit)

    def test_provider_defaults_stand_without_sync_knobs(self, tmp_path: Path) -> None:
        config = FleetpullConfig.from_yaml(_write(tmp_path, _minimal_yaml(tmp_path)))
        motive = config.providers.motive
        assert motive is not None
        assert motive.lookback_days == 7
        assert motive.cutoff_days == 0
        assert motive.rate_limit.requests_per_period == 60

    def test_optional_sections_default_wholesale(self, tmp_path: Path) -> None:
        config = FleetpullConfig.from_yaml(_write(tmp_path, _minimal_yaml(tmp_path)))
        assert config.http.use_truststore is False
        assert config.retry.transient_max_failures == 3
        assert config.logging.console_level == logging.INFO
        assert config.logging.file_path is None

    def test_string_path_argument_is_accepted(self, tmp_path: Path) -> None:
        config_path = _write(tmp_path, _minimal_yaml(tmp_path))
        config = FleetpullConfig.from_yaml(str(config_path))
        assert config.storage.dataset_root == resolve_path(tmp_path)


class TestKnobPrecedenceMatrix:
    @pytest.mark.parametrize('knob', ['lookback_days', 'cutoff_days'])
    @pytest.mark.parametrize(
        ('sync_value', 'provider_value', 'expected'),
        [
            (None, None, None),  # neither -> the provider model default
            (3, None, 3),  # sync alone fans in
            (None, 9, 9),  # provider alone stands
            (3, 9, 9),  # both -> the provider key wins
        ],
    )
    def test_precedence(
        self,
        tmp_path: Path,
        knob: str,
        sync_value: int | None,
        provider_value: int | None,
        expected: int | None,
    ) -> None:
        text = _minimal_yaml(tmp_path)
        if sync_value is not None:
            text = text.replace('sync:\n', f'sync:\n  {knob}: {sync_value}\n')
        if provider_value is not None:
            text += f'    {knob}: {provider_value}\n'
        config = FleetpullConfig.from_yaml(_write(tmp_path, text))
        motive = config.providers.motive
        assert motive is not None
        default_by_knob = {'lookback_days': 7, 'cutoff_days': 0}
        expected_value = expected if expected is not None else default_by_knob[knob]
        assert getattr(motive, knob) == expected_value


class TestPathNormalization:
    def test_every_path_field_normalizes(self, tmp_path: Path) -> None:
        raw_root = f'{tmp_path}/nested/..'
        text = (
            'sync:\n'
            '  default_start_date: 2026-06-01\n'
            'storage:\n'
            f'  dataset_root: {raw_root}\n'
            'state:\n'
            f'  database_path: {tmp_path}/a/../state.sqlite3\n'
            'logging:\n'
            f'  file_path: {tmp_path}/b/../fp.log\n'
            'providers:\n'
            '  motive:\n'
            f"    api_key: '{_SYNTHETIC_KEY}'\n"
            '    endpoints: [vehicles]\n'
        )
        config = FleetpullConfig.from_yaml(_write(tmp_path, text))
        assert config.storage.dataset_root == resolve_path(raw_root)
        assert config.state.database_path == resolve_path(f'{tmp_path}/state.sqlite3')
        assert config.logging.file_path == resolve_path(f'{tmp_path}/fp.log')

    def test_derived_defaults_are_normalized_too(self, tmp_path: Path) -> None:
        raw_root = f'{tmp_path}/nested/..'
        text = _minimal_yaml(tmp_path).replace(
            f'  dataset_root: {tmp_path}\n', f'  dataset_root: {raw_root}\n'
        )
        config = FleetpullConfig.from_yaml(_write(tmp_path, text))
        expected = resolve_path(raw_root) / '.fleetpull' / 'state.sqlite3'
        assert config.state.database_path == expected


class TestLoggingEitherKey:
    def test_file_level_alone_defaults_the_path(self, tmp_path: Path) -> None:
        text = _minimal_yaml(tmp_path) + 'logging:\n  file_level: INFO\n'
        config = FleetpullConfig.from_yaml(_write(tmp_path, text))
        expected = resolve_path(tmp_path) / '.fleetpull' / 'fleetpull.log'
        assert config.logging.file_path == expected
        assert config.logging.file_level == logging.INFO

    def test_file_path_alone_defaults_the_level(self, tmp_path: Path) -> None:
        explicit = tmp_path / 'log' / 'fp.log'
        text = (
            _minimal_yaml(tmp_path) + f'logging:\n  file_path: {explicit.as_posix()}\n'
        )
        config = FleetpullConfig.from_yaml(_write(tmp_path, text))
        assert config.logging.file_path == resolve_path(explicit)
        assert config.logging.file_level == logging.DEBUG

    def test_neither_key_leaves_file_logging_off(self, tmp_path: Path) -> None:
        text = _minimal_yaml(tmp_path) + 'logging:\n  console_level: WARNING\n'
        config = FleetpullConfig.from_yaml(_write(tmp_path, text))
        assert config.logging.file_path is None
        assert config.logging.console_level == logging.WARNING


class TestEnablement:
    def test_endpoints_without_credential_names_field_and_env_var(
        self, tmp_path: Path
    ) -> None:
        text = _minimal_yaml(tmp_path).replace(f"    api_key: '{_SYNTHETIC_KEY}'\n", '')
        with pytest.raises(ConfigurationError) as raised:
            FleetpullConfig.from_yaml(_write(tmp_path, text))
        message = str(raised.value)
        assert 'providers.motive.api_key' in message
        assert 'MOTIVE_API_KEY' in message

    def test_direct_construction_enforces_the_same_rule(self) -> None:
        with pytest.raises(ConfigurationError, match='MOTIVE_API_KEY'):
            FleetpullConfig(
                sync=SyncConfig(default_start_date=date(2026, 6, 1)),
                storage=StorageConfig(dataset_root='/d'),
                providers=ProvidersConfig(motive=MotiveConfig(endpoints=('vehicles',))),
            )

    def test_credential_without_endpoints_warns_and_disables(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        text = _minimal_yaml(tmp_path).replace('    endpoints: [vehicles]\n', '')
        with caplog.at_level(logging.WARNING, logger='fleetpull.config.loading'):
            config = FleetpullConfig.from_yaml(_write(tmp_path, text))
        warnings = [
            record for record in caplog.records if record.levelno == logging.WARNING
        ]
        assert len(warnings) == 1
        assert 'disabled' in warnings[0].getMessage()
        motive = config.providers.motive
        assert motive is not None
        assert motive.endpoints == ()

    def test_samsara_credential_without_endpoints_warns_and_disables(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        text = (
            'sync:\n'
            '  default_start_date: 2026-06-01\n'
            'storage:\n'
            f'  dataset_root: {tmp_path}\n'
            'providers:\n'
            '  samsara:\n'
            "    api_key: 'synthetic-samsara-token-000'\n"
        )
        with caplog.at_level(logging.WARNING, logger='fleetpull.config.loading'):
            config = FleetpullConfig.from_yaml(_write(tmp_path, text))
        warnings = [
            record for record in caplog.records if record.levelno == logging.WARNING
        ]
        assert len(warnings) == 1
        assert 'samsara' in warnings[0].getMessage()
        assert 'disabled' in warnings[0].getMessage()
        samsara = config.providers.samsara
        assert samsara is not None
        assert samsara.endpoints == ()

    def test_absent_provider_is_silent(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        text = (
            'sync:\n  default_start_date: 2026-06-01\n'
            f'storage:\n  dataset_root: {tmp_path}\n'
            'providers: {}\n'
        )
        with caplog.at_level(logging.WARNING):
            config = FleetpullConfig.from_yaml(_write(tmp_path, text))
        assert config.providers.motive is None
        assert not caplog.records


def _geotab_yaml(tmp_path: Path, *, password_line: str) -> str:
    """A geotab-only document; the password line is the test's variable."""
    return (
        f'sync:\n'
        f'  default_start_date: 2026-06-01\n'
        f'storage:\n'
        f'  dataset_root: {tmp_path / "data"}\n'
        f'providers:\n'
        f'  geotab:\n'
        f'    auth:\n'
        f'      username: user@example.com\n'
        f'      database: exampledb\n'
        f'{password_line}'
        f'    endpoints: [devices, trips]\n'
    )


def _samsara_yaml(tmp_path: Path) -> str:
    """A samsara-only document with no YAML credential (the env is the test's variable)."""
    return (
        f'sync:\n'
        f'  default_start_date: 2026-06-01\n'
        f'storage:\n'
        f'  dataset_root: {tmp_path / "data"}\n'
        f'providers:\n'
        f'  samsara:\n'
        f'    endpoints: [vehicles]\n'
    )


class TestEnvironmentFallback:
    def test_absent_key_with_env_set_resolves(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv('MOTIVE_API_KEY', 'env-synthetic-key')
        text = _minimal_yaml(tmp_path).replace(f"    api_key: '{_SYNTHETIC_KEY}'\n", '')
        config = FleetpullConfig.from_yaml(_write(tmp_path, text))
        motive = config.providers.motive
        assert motive is not None
        assert motive.api_key is not None
        assert motive.api_key.get_secret_value() == 'env-synthetic-key'

    def test_yaml_literal_wins_over_the_environment(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv('MOTIVE_API_KEY', 'env-synthetic-key')
        config = FleetpullConfig.from_yaml(_write(tmp_path, _minimal_yaml(tmp_path)))
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
            FleetpullConfig.from_yaml(_write(tmp_path, text))

    def test_geotab_env_fills_only_the_absent_password(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv('GEOTAB_PASSWORD', 'env-synthetic-pass')
        config = FleetpullConfig.from_yaml(
            _write(tmp_path, _geotab_yaml(tmp_path, password_line=''))
        )
        geotab = config.providers.geotab
        assert geotab is not None
        assert geotab.auth is not None
        assert geotab.auth.password.get_secret_value() == 'env-synthetic-pass'
        # The non-secret fields came from the YAML, never the environment.
        assert geotab.auth.username == 'user@example.com'
        assert geotab.auth.database == 'exampledb'

    def test_geotab_yaml_password_wins_over_the_environment(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv('GEOTAB_PASSWORD', 'env-synthetic-pass')
        yaml_password = "      password: 'yaml-synthetic-pass'\n"
        config = FleetpullConfig.from_yaml(
            _write(tmp_path, _geotab_yaml(tmp_path, password_line=yaml_password))
        )
        geotab = config.providers.geotab
        assert geotab is not None
        assert geotab.auth is not None
        assert geotab.auth.password.get_secret_value() == 'yaml-synthetic-pass'

    def test_samsara_absent_key_with_env_set_resolves(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv('SAMSARA_API_KEY', 'env-synthetic-token')
        config = FleetpullConfig.from_yaml(_write(tmp_path, _samsara_yaml(tmp_path)))
        samsara = config.providers.samsara
        assert samsara is not None
        assert samsara.api_key is not None
        assert samsara.api_key.get_secret_value() == 'env-synthetic-token'

    def test_samsara_endpoints_without_any_credential_raise(
        self, tmp_path: Path
    ) -> None:
        with pytest.raises(ConfigurationError, match='SAMSARA_API_KEY'):
            FleetpullConfig.from_yaml(_write(tmp_path, _samsara_yaml(tmp_path)))

    def test_geotab_endpoints_without_any_credential_raise(
        self, tmp_path: Path
    ) -> None:
        text = (
            f'sync:\n  default_start_date: 2026-06-01\n'
            f'storage:\n  dataset_root: {tmp_path / "data"}\n'
            f'providers:\n  geotab:\n    endpoints: [devices]\n'
        )
        with pytest.raises(ConfigurationError, match='GEOTAB_PASSWORD'):
            FleetpullConfig.from_yaml(_write(tmp_path, text))


class TestErrorBranches:
    def test_missing_file_names_the_path(self, tmp_path: Path) -> None:
        missing = tmp_path / 'absent.yaml'
        with pytest.raises(ConfigurationError, match='config file not found'):
            FleetpullConfig.from_yaml(missing)

    def test_parse_error_names_the_line_without_snippets(self, tmp_path: Path) -> None:
        config_path = _write(
            tmp_path, f'providers:\n  motive:\n    api_key: [{_SYNTHETIC_KEY}\n'
        )
        with pytest.raises(ConfigurationError, match=r'not valid YAML.*line \d+') as e:
            FleetpullConfig.from_yaml(config_path)
        # The parser's problem text only -- never the marked source snippet,
        # which would echo the credential-carrying line.
        assert _SYNTHETIC_KEY not in str(e.value)

    def test_non_mapping_top_level_is_rejected(self, tmp_path: Path) -> None:
        config_path = _write(tmp_path, '- a\n- b\n')
        with pytest.raises(ConfigurationError, match='mapping at the top level'):
            FleetpullConfig.from_yaml(config_path)

    def test_empty_file_names_the_missing_sections(self, tmp_path: Path) -> None:
        with pytest.raises(ConfigurationError) as raised:
            FleetpullConfig.from_yaml(_write(tmp_path, '\n'))
        message = str(raised.value)
        assert 'sync' in message
        assert 'storage' in message
        assert 'providers' in message

    def test_unknown_top_level_key_is_named(self, tmp_path: Path) -> None:
        text = _minimal_yaml(tmp_path) + 'sink: {}\n'
        with pytest.raises(ConfigurationError, match='sink'):
            FleetpullConfig.from_yaml(_write(tmp_path, text))

    def test_duplicate_endpoint_names_surface_with_field_path_and_name(
        self, tmp_path: Path
    ) -> None:
        # The model-tier duplicate rejection surfaces through from_yaml as
        # the usual ConfigurationError, locating the field and naming the
        # duplicated endpoint.
        text = _minimal_yaml(tmp_path).replace(
            'endpoints: [vehicles]', 'endpoints: [vehicles, vehicles]'
        )
        with pytest.raises(ConfigurationError) as raised:
            FleetpullConfig.from_yaml(_write(tmp_path, text))
        message = str(raised.value)
        assert 'providers.motive.endpoints' in message
        assert 'vehicles' in message

    def test_unknown_nested_key_is_named_with_its_path(self, tmp_path: Path) -> None:
        text = _minimal_yaml(tmp_path).replace(
            '    endpoints:', '    api_keyy: x\n    endpoints:'
        )
        with pytest.raises(ConfigurationError, match=r'providers\.motive\.api_keyy'):
            FleetpullConfig.from_yaml(_write(tmp_path, text))

    def test_missing_required_key_is_named(self, tmp_path: Path) -> None:
        # The section stays a mapping so the missing KEY is what gets named.
        text = _minimal_yaml(tmp_path).replace(
            '  default_start_date: 2026-06-01\n', '  lookback_days: 3\n'
        )
        with pytest.raises(
            ConfigurationError, match=r'sync\.default_start_date.*required'
        ):
            FleetpullConfig.from_yaml(_write(tmp_path, text))


class TestSecretHygiene:
    def test_secret_never_appears_in_validation_errors(self, tmp_path: Path) -> None:
        text = _minimal_yaml(tmp_path) + 'sink: {}\n'
        with pytest.raises(ConfigurationError) as raised:
            FleetpullConfig.from_yaml(_write(tmp_path, text))
        assert _SYNTHETIC_KEY not in str(raised.value)
        assert _SYNTHETIC_KEY not in repr(raised.value)

    def test_secret_never_appears_in_the_loaded_config_repr(
        self, tmp_path: Path
    ) -> None:
        config = FleetpullConfig.from_yaml(_write(tmp_path, _minimal_yaml(tmp_path)))
        assert _SYNTHETIC_KEY not in repr(config)
        assert _SYNTHETIC_KEY not in str(config)


class TestExampleFile:
    def test_committed_example_loads_green(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv('MOTIVE_API_KEY', 'env-synthetic-key')
        monkeypatch.setenv('GEOTAB_PASSWORD', 'env-synthetic-pass')
        monkeypatch.setenv('SAMSARA_API_KEY', 'env-synthetic-token')
        example_text = _EXAMPLE_FILE.read_text(encoding='utf-8')
        pointed = example_text.replace('/data/fleetpull', str(tmp_path))
        config = FleetpullConfig.from_yaml(_write(tmp_path, pointed))
        motive = config.providers.motive
        assert motive is not None
        assert motive.endpoints == ('vehicles', 'vehicle_locations')
        samsara = config.providers.samsara
        assert samsara is not None
        assert samsara.endpoints == ('vehicles', 'drivers', 'trips', 'idling_events')
        geotab = config.providers.geotab
        assert geotab is not None
        assert geotab.auth is not None
        assert geotab.endpoints == ()  # devices ships commented out
        expected = resolve_path(tmp_path) / '.fleetpull' / 'state.sqlite3'
        assert config.state.database_path == expected
