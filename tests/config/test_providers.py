"""Tests for fleetpull.config.providers (the provider config family)."""

import pytest
from pydantic import SecretStr, ValidationError

from fleetpull.config import (
    GeotabAuthConfig,
    GeotabConfig,
    MotiveConfig,
    ProvidersConfig,
)
from fleetpull.config.providers import (
    PROVIDER_CREDENTIAL_ENV_VARS,
    require_provider_credentials,
)
from fleetpull.exceptions import ConfigurationError
from fleetpull.vocabulary import QuotaScope

_SYNTHETIC_KEY = 'synthetic-motive-key-000'


class TestMotiveConfig:
    def test_defaults(self) -> None:
        config = MotiveConfig()
        assert config.base_url == 'https://api.gomotive.com'
        assert config.records_per_page == 100
        assert config.lookback_days == 7
        assert config.cutoff_days == 0
        assert config.api_key is None
        assert config.endpoints == ()

    def test_knobs_are_plain_ints_with_overrides(self) -> None:
        config = MotiveConfig(lookback_days=2, cutoff_days=3)
        assert config.lookback_days == 2
        assert config.cutoff_days == 3

    @pytest.mark.parametrize('knob', ['lookback_days', 'cutoff_days'])
    def test_rejects_negative_knobs(self, knob: str) -> None:
        with pytest.raises(ValidationError):
            MotiveConfig(**{knob: -1})

    def test_strips_a_trailing_slash(self) -> None:
        assert MotiveConfig(base_url='https://motive.test/').base_url == (
            'https://motive.test'
        )

    def test_rejects_a_schemeless_base_url(self) -> None:
        with pytest.raises(ValidationError, match='http'):
            MotiveConfig(base_url='motive.test')

    def test_rejects_an_out_of_range_page_size(self) -> None:
        with pytest.raises(ValidationError):
            MotiveConfig(records_per_page=101)

    def test_api_key_string_coerces_to_secret(self) -> None:
        config = MotiveConfig(api_key=_SYNTHETIC_KEY)
        assert config.api_key is not None
        assert config.api_key.get_secret_value() == _SYNTHETIC_KEY

    def test_api_key_is_masked_in_reprs(self) -> None:
        config = MotiveConfig(api_key=_SYNTHETIC_KEY)
        assert _SYNTHETIC_KEY not in repr(config)
        assert _SYNTHETIC_KEY not in str(config)

    def test_endpoints_list_coerces_to_tuple(self) -> None:
        config = MotiveConfig(endpoints=['vehicles', 'vehicle_locations'])
        assert config.endpoints == ('vehicles', 'vehicle_locations')

    def test_rejects_unknown_fields(self) -> None:
        with pytest.raises(ValidationError):
            MotiveConfig(base_ur='https://typo.test')  # type: ignore[call-arg]

    def test_is_frozen(self) -> None:
        config = MotiveConfig()
        with pytest.raises(ValidationError):
            config.base_url = 'https://other.test'  # type: ignore[misc]


class TestProvidersConfig:
    def test_motive_defaults_to_absent(self) -> None:
        assert ProvidersConfig().motive is None

    def test_carries_a_motive_section(self) -> None:
        assert ProvidersConfig(motive=MotiveConfig()).motive is not None

    def test_rejects_unknown_providers(self) -> None:
        with pytest.raises(ValidationError):
            ProvidersConfig(samsara={})  # type: ignore[call-arg]


class TestCredentialContract:
    def test_env_var_convention_names_motive(self) -> None:
        assert PROVIDER_CREDENTIAL_ENV_VARS['motive'] == 'MOTIVE_API_KEY'

    def test_endpoints_without_credential_raise(self) -> None:
        providers = ProvidersConfig(motive=MotiveConfig(endpoints=('vehicles',)))

        with pytest.raises(ConfigurationError) as raised:
            require_provider_credentials(providers)
        message = str(raised.value)
        assert 'providers.motive.api_key' in message
        assert 'MOTIVE_API_KEY' in message

    def test_credentialed_or_endpointless_providers_pass(self) -> None:
        require_provider_credentials(ProvidersConfig())
        require_provider_credentials(
            ProvidersConfig(motive=MotiveConfig(api_key=_SYNTHETIC_KEY))
        )
        require_provider_credentials(
            ProvidersConfig(
                motive=MotiveConfig(api_key=_SYNTHETIC_KEY, endpoints=('vehicles',))
            )
        )


def _geotab_auth() -> GeotabAuthConfig:
    return GeotabAuthConfig(
        username='user@example.com',
        password=SecretStr('synthetic-geotab-pass'),
        database='exampledb',
    )


class TestGeotabConfig:
    def test_defaults(self) -> None:
        config = GeotabConfig()
        assert config.auth is None
        assert config.endpoints == ()
        # The Get method-class budget cites the captured 2026-07-09 header
        # (~650/min, single datum); Authenticate is 10/min (June capture).
        assert config.rate_limit.requests_per_period == 650
        assert config.authenticate_rate_limit.requests_per_period == 10

    def test_quota_scope_binds_the_get_class(self) -> None:
        assert GeotabConfig.quota_scope is QuotaScope.GEOTAB_GET

    def test_password_is_masked_in_reprs(self) -> None:
        config = GeotabConfig(auth=_geotab_auth())
        for rendering in (repr(config), str(config)):
            assert 'synthetic-geotab-pass' not in rendering

    def test_rejects_unknown_fields(self) -> None:
        with pytest.raises(ValidationError):
            GeotabConfig(base_url='https://x.example')  # type: ignore[call-arg]

    def test_rejects_watermark_knobs(self) -> None:
        # lookback/cutoff are watermark-arm knobs; GeoTab's incremental
        # story is token-based feeds, so the section rejects them.
        with pytest.raises(ValidationError):
            GeotabConfig(lookback_days=1)  # type: ignore[call-arg]

    def test_is_frozen(self) -> None:
        config = GeotabConfig()
        with pytest.raises(ValidationError):
            config.endpoints = ('devices',)  # type: ignore[misc]


class TestGeotabCredentialContract:
    def test_env_var_convention_names_geotab_password(self) -> None:
        assert PROVIDER_CREDENTIAL_ENV_VARS['geotab'] == 'GEOTAB_PASSWORD'

    def test_endpoints_without_auth_raise_naming_field_and_env_var(self) -> None:
        providers = ProvidersConfig(geotab=GeotabConfig(endpoints=('devices',)))
        with pytest.raises(ConfigurationError) as raised:
            require_provider_credentials(providers)
        message = str(raised.value)
        assert 'providers.geotab.auth' in message
        assert 'GEOTAB_PASSWORD' in message

    def test_authed_or_endpointless_geotab_passes(self) -> None:
        require_provider_credentials(
            ProvidersConfig(
                geotab=GeotabConfig(auth=_geotab_auth(), endpoints=('devices',))
            )
        )
        require_provider_credentials(ProvidersConfig(geotab=GeotabConfig()))
