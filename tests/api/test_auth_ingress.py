"""Tests for fleetpull.api.auth_ingress.

Dispatch keys off the endpoint identity's provider, never the auth
value's shape; every accepted shape is coerced into ``SecretStr``
internals at the boundary; and no rejection path ever echoes the value
it rejected -- only its type name. The Motive profile is verified
behaviorally (the header its ``prepare`` injects), not by reaching into
the strategy's private fields; the GeoTab profile is verified all the
way through a mock-transport ``Authenticate`` (the June harness
pattern), proving the ingress composed a stack that actually
authenticates.
"""

import json
import ssl

import httpx
import pytest
from pydantic import SecretStr

from fleetpull.api import Endpoints
from fleetpull.api.auth_ingress import (
    ProviderProfileContext,
    build_provider_profile,
)
from fleetpull.api.identity import SnapshotEndpoint
from fleetpull.config import GeotabAuthConfig, GeotabConfig, HttpConfig
from fleetpull.exceptions import ConfigurationError
from fleetpull.network.auth import GeotabSessionAuth
from fleetpull.network.classifiers import (
    GeotabResponseClassifier,
    MotiveResponseClassifier,
)
from fleetpull.network.contract import HttpMethod, RequestSpec
from fleetpull.network.limits import RateLimiterRegistry, rate_limits_from_configs
from fleetpull.timing import SystemClock
from fleetpull.vocabulary import Provider

_SYNTHETIC_KEY = 'synthetic-motive-key-000'
_SYNTHETIC_PASS = 'synthetic-geotab-pass-000'

# The genuine class, captured before any test monkeypatches httpx.Client
# (the transport-test precedent).
_REAL_CLIENT_CLS = httpx.Client

# Captured: Authenticate success (the June fixture shape).
_AUTHENTICATE_SUCCESS = (
    '{"result": {"credentials": {"database": "exampledb", "sessionId":'
    ' "SyntheticSessionId000001", "userName": "user@example.com"},'
    ' "path": "ThisServer"}, "jsonrpc": "2.0"}'
)


def _bare_spec() -> RequestSpec:
    return RequestSpec(method=HttpMethod.GET, url='https://api.example.test/v1/x')


def _geotab_spec() -> RequestSpec:
    return RequestSpec(
        method=HttpMethod.POST,
        url='https://my.geotab.com/apiv1',
        json_body={'method': 'Get', 'params': {'typeName': 'Device'}},
    )


def _context() -> ProviderProfileContext:
    return ProviderProfileContext(
        http_config=HttpConfig(),
        limiter_registry=RateLimiterRegistry(
            rate_limits_from_configs([GeotabConfig()])
        ),
        clock=SystemClock(),
    )


def _geotab_mapping() -> dict[str, str]:
    return {
        'username': 'user@example.com',
        'password': _SYNTHETIC_PASS,
        'database': 'exampledb',
    }


def test_motive_bare_string_becomes_header_profile() -> None:
    profile = build_provider_profile(
        Endpoints.Motive.vehicles, _SYNTHETIC_KEY, _context()
    )
    prepared = profile.auth.prepare(_bare_spec())
    assert prepared.headers['X-API-Key'] == _SYNTHETIC_KEY
    assert isinstance(profile.classifier, MotiveResponseClassifier)


def test_motive_profile_never_reprs_the_secret() -> None:
    profile = build_provider_profile(
        Endpoints.Motive.vehicles, _SYNTHETIC_KEY, _context()
    )
    for rendering in (repr(profile), str(profile), repr(profile.auth)):
        assert _SYNTHETIC_KEY not in rendering


@pytest.mark.parametrize(
    'wrong_shaped_auth',
    [
        {'api_key': _SYNTHETIC_KEY},
        GeotabAuthConfig(
            username='synthetic-user',
            password=SecretStr('synthetic-pass'),
            database='synthetic-db',
        ),
    ],
    ids=['mapping', 'geotab_config'],
)
def test_wrong_shape_for_motive_names_shape_and_provider(
    wrong_shaped_auth: dict[str, str] | GeotabAuthConfig,
) -> None:
    with pytest.raises(ConfigurationError) as raised:
        build_provider_profile(Endpoints.Motive.vehicles, wrong_shaped_auth, _context())
    message = str(raised.value)
    assert 'motive' in message
    assert 'bare API-key string' in message
    assert type(wrong_shaped_auth).__name__ in message


def test_rejection_never_echoes_the_value() -> None:
    with pytest.raises(ConfigurationError) as raised:
        build_provider_profile(
            Endpoints.Motive.vehicles, {'api_key': _SYNTHETIC_KEY}, _context()
        )
    assert _SYNTHETIC_KEY not in str(raised.value)
    assert _SYNTHETIC_KEY not in repr(raised.value)


def test_unexposed_provider_is_a_configuration_error() -> None:
    # No Samsara catalog identity exists; a hand-built one exercises the arm.
    samsara_identity = SnapshotEndpoint(Provider.SAMSARA, 'vehicles')
    with pytest.raises(ConfigurationError) as raised:
        build_provider_profile(samsara_identity, _SYNTHETIC_KEY, _context())
    assert 'samsara' in str(raised.value)


class TestGeotabArm:
    def test_mapping_coerces_into_a_session_profile(self) -> None:
        profile = build_provider_profile(
            Endpoints.Geotab.devices, _geotab_mapping(), _context()
        )
        assert isinstance(profile.auth, GeotabSessionAuth)
        assert isinstance(profile.classifier, GeotabResponseClassifier)

    def test_geotab_auth_config_passes_through(self) -> None:
        credential = GeotabAuthConfig(
            username='user@example.com',
            password=SecretStr(_SYNTHETIC_PASS),
            database='exampledb',
        )
        profile = build_provider_profile(
            Endpoints.Geotab.devices, credential, _context()
        )
        assert isinstance(profile.auth, GeotabSessionAuth)
        assert isinstance(profile.classifier, GeotabResponseClassifier)

    @pytest.mark.parametrize(
        'wrong_shaped_auth',
        [_SYNTHETIC_KEY, SecretStr(_SYNTHETIC_KEY)],
        ids=['bare_string', 'secret_str'],
    )
    def test_wrong_shape_for_geotab_names_shapes_and_provider(
        self, wrong_shaped_auth: str | SecretStr
    ) -> None:
        with pytest.raises(ConfigurationError) as raised:
            build_provider_profile(
                Endpoints.Geotab.devices, wrong_shaped_auth, _context()
            )
        message = str(raised.value)
        assert 'geotab' in message
        assert 'GeotabAuthConfig or a mapping' in message
        assert type(wrong_shaped_auth).__name__ in message

    def test_invalid_mapping_names_fields_never_values(self) -> None:
        with pytest.raises(ConfigurationError) as raised:
            build_provider_profile(
                Endpoints.Geotab.devices,
                {'username': 'user@example.com', 'password': _SYNTHETIC_PASS},
                _context(),
            )
        message = str(raised.value)
        assert 'database' in message
        assert _SYNTHETIC_PASS not in message
        assert _SYNTHETIC_PASS not in repr(raised.value)

    def test_no_geotab_path_ever_reprs_the_password(self) -> None:
        profile = build_provider_profile(
            Endpoints.Geotab.devices, _geotab_mapping(), _context()
        )
        for rendering in (repr(profile), str(profile), repr(profile.auth)):
            assert _SYNTHETIC_PASS not in rendering

    def test_ingress_composes_a_stack_that_authenticates(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The June harness pattern: MockTransport serves the captured
        # Authenticate-success envelope; the profile built through the
        # ingress prepares a bare GeoTab spec and the prepared request
        # carries the session credentials in params.
        def handler(request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content)
            assert body['method'] == 'Authenticate'
            return httpx.Response(200, text=_AUTHENTICATE_SUCCESS)

        mock_transport = httpx.MockTransport(handler)

        def client_factory(
            *,
            verify: ssl.SSLContext | bool = True,
            timeout: httpx.Timeout | None = None,
        ) -> httpx.Client:
            return _REAL_CLIENT_CLS(transport=mock_transport, timeout=timeout)

        monkeypatch.setattr(httpx, 'Client', client_factory)
        profile = build_provider_profile(
            Endpoints.Geotab.devices, _geotab_mapping(), _context()
        )
        prepared = profile.auth.prepare(_geotab_spec())
        assert prepared.json_body is not None
        params = prepared.json_body['params']
        assert isinstance(params, dict)
        credentials = params['credentials']
        assert isinstance(credentials, dict)
        assert credentials['sessionId'] == 'SyntheticSessionId000001'
        assert credentials['database'] == 'exampledb'
        assert prepared.url == 'https://my.geotab.com/apiv1'
