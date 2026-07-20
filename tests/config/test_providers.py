"""Tests for fleetpull.config.providers (the provider config family)."""

import pytest
from pydantic import SecretStr, ValidationError

from fleetpull.config import (
    GeotabAuthConfig,
    GeotabConfig,
    MotiveConfig,
    ProvidersConfig,
    SamsaraConfig,
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
        # samsara was this test's unknown example until it became a real
        # section (2026-07-17); any never-a-provider name serves.
        with pytest.raises(ValidationError):
            ProvidersConfig(verizon_connect={})  # type: ignore[call-arg]


class TestEndpointUniqueness:
    """Duplicates are rejected at validation: a duplicated name would run
    twice -- concurrently, under the staged intra-provider queue."""

    @pytest.mark.parametrize(
        ('config_cls', 'endpoint'),
        [
            (MotiveConfig, 'vehicles'),
            (GeotabConfig, 'devices'),
            (SamsaraConfig, 'trips'),
        ],
    )
    def test_a_duplicated_name_is_rejected_and_named(
        self,
        config_cls: type[MotiveConfig | GeotabConfig | SamsaraConfig],
        endpoint: str,
    ) -> None:
        with pytest.raises(ValidationError, match=endpoint):
            config_cls(endpoints=(endpoint, 'other_endpoint', endpoint))

    def test_every_duplicated_name_is_named(self) -> None:
        with pytest.raises(ValidationError, match=r'trips.*vehicles'):
            MotiveConfig(endpoints=('vehicles', 'trips', 'vehicles', 'trips'))

    def test_distinct_names_pass(self) -> None:
        config = MotiveConfig(endpoints=('vehicles', 'vehicle_locations'))
        assert config.endpoints == ('vehicles', 'vehicle_locations')


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
        assert config.lookback_days == 7
        assert config.cutoff_days == 0

    def test_quota_scope_binds_the_get_class(self) -> None:
        assert GeotabConfig.quota_scope is QuotaScope.GEOTAB_GET

    def test_password_is_masked_in_reprs(self) -> None:
        config = GeotabConfig(auth=_geotab_auth())
        for rendering in (repr(config), str(config)):
            assert 'synthetic-geotab-pass' not in rendering

    def test_rejects_unknown_fields(self) -> None:
        with pytest.raises(ValidationError):
            GeotabConfig(base_url='https://x.example')  # type: ignore[call-arg]

    def test_knobs_are_plain_ints_with_overrides(self) -> None:
        # Watermark knobs since the trips vertical (the earlier rejection
        # encoded feeds-only incrementality, superseded 2026-07-13).
        config = GeotabConfig(lookback_days=2, cutoff_days=3)
        assert config.lookback_days == 2
        assert config.cutoff_days == 3

    @pytest.mark.parametrize('knob', ['lookback_days', 'cutoff_days'])
    def test_rejects_negative_knobs(self, knob: str) -> None:
        with pytest.raises(ValidationError):
            GeotabConfig(**{knob: -1})

    def test_is_frozen(self) -> None:
        config = GeotabConfig()
        with pytest.raises(ValidationError):
            config.endpoints = ('devices', 'trips')  # type: ignore[misc]


class TestGeotabCredentialContract:
    def test_env_var_convention_names_geotab_password(self) -> None:
        assert PROVIDER_CREDENTIAL_ENV_VARS['geotab'] == 'GEOTAB_PASSWORD'

    def test_endpoints_without_auth_raise_naming_field_and_env_var(self) -> None:
        providers = ProvidersConfig(geotab=GeotabConfig(endpoints=('devices', 'trips')))
        with pytest.raises(ConfigurationError) as raised:
            require_provider_credentials(providers)
        message = str(raised.value)
        assert 'providers.geotab.auth' in message
        assert 'GEOTAB_PASSWORD' in message

    def test_authed_or_endpointless_geotab_passes(self) -> None:
        require_provider_credentials(
            ProvidersConfig(
                geotab=GeotabConfig(auth=_geotab_auth(), endpoints=('devices', 'trips'))
            )
        )
        require_provider_credentials(ProvidersConfig(geotab=GeotabConfig()))


class TestSamsaraConfig:
    def test_defaults(self) -> None:
        config = SamsaraConfig()
        assert config.base_url == 'https://api.samsara.com'
        assert config.lookback_days == 7
        assert config.cutoff_days == 0
        assert config.api_key is None
        assert config.endpoints == ()

    def test_default_rate_limit_is_the_tightest_documented_tier(self) -> None:
        # 100 requests/minute -- the lowest documented per-endpoint tier;
        # the provider-wide scope self-limits there until the per-endpoint
        # scope split lands (see the default's comment in providers.py).
        config = SamsaraConfig()
        assert config.rate_limit.requests_per_period == 100
        assert config.rate_limit.period_seconds == 60.0

    def test_quota_scope_binds_the_provider_scope(self) -> None:
        assert SamsaraConfig.quota_scope is QuotaScope.SAMSARA

    def test_strips_a_trailing_slash(self) -> None:
        config = SamsaraConfig(base_url='https://api.samsara.com/')
        assert config.base_url == 'https://api.samsara.com'

    def test_rejects_a_schemeless_base_url(self) -> None:
        with pytest.raises(ValidationError):
            SamsaraConfig(base_url='api.samsara.com')

    def test_api_key_is_masked_in_reprs(self) -> None:
        config = SamsaraConfig(api_key=SecretStr('synthetic-samsara-token-000'))
        assert 'synthetic-samsara-token-000' not in repr(config)
        assert 'synthetic-samsara-token-000' not in str(config)

    @pytest.mark.parametrize('knob', ['lookback_days', 'cutoff_days'])
    def test_rejects_negative_knobs(self, knob: str) -> None:
        with pytest.raises(ValidationError):
            SamsaraConfig(**{knob: -1})

    def test_rejects_unknown_fields(self) -> None:
        with pytest.raises(ValidationError):
            SamsaraConfig(records_per_page=100)  # type: ignore[call-arg]

    def test_is_frozen(self) -> None:
        config = SamsaraConfig()
        with pytest.raises(ValidationError):
            config.endpoints = ('vehicles',)  # type: ignore[misc]


class TestSamsaraCredentialContract:
    def test_env_var_convention_names_samsara(self) -> None:
        assert PROVIDER_CREDENTIAL_ENV_VARS['samsara'] == 'SAMSARA_API_KEY'

    def test_endpoints_without_key_raise_naming_field_and_env_var(self) -> None:
        providers = ProvidersConfig(samsara=SamsaraConfig(endpoints=('vehicles',)))
        with pytest.raises(ConfigurationError) as raised:
            require_provider_credentials(providers)
        message = str(raised.value)
        assert 'providers.samsara.api_key' in message
        assert 'SAMSARA_API_KEY' in message

    def test_credentialed_or_endpointless_samsara_passes(self) -> None:
        require_provider_credentials(
            ProvidersConfig(
                samsara=SamsaraConfig(
                    api_key=SecretStr('synthetic-samsara-token-000'),
                    endpoints=('vehicles',),
                )
            )
        )
        require_provider_credentials(ProvidersConfig(samsara=SamsaraConfig()))
