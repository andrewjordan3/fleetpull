"""Tests for fleetpull.api.auth_ingress.

Dispatch keys off the endpoint identity's provider, never the auth
value's shape; every accepted shape is coerced into ``SecretStr``
internals at the boundary; and no rejection path ever echoes the value
it rejected -- only its type name. The Motive profile is verified
behaviorally (the header its ``prepare`` injects), not by reaching into
the strategy's private fields.
"""

import pytest
from pydantic import SecretStr

from fleetpull.api import Endpoints
from fleetpull.api.auth_ingress import build_provider_profile
from fleetpull.api.identity import SnapshotEndpoint
from fleetpull.config import GeotabAuthConfig
from fleetpull.exceptions import ConfigurationError
from fleetpull.network.classifiers import MotiveResponseClassifier
from fleetpull.network.contract import HttpMethod, RequestSpec
from fleetpull.vocabulary import Provider

_SYNTHETIC_KEY = 'synthetic-motive-key-000'


def _bare_spec() -> RequestSpec:
    return RequestSpec(method=HttpMethod.GET, url='https://api.example.test/v1/x')


def test_motive_bare_string_becomes_header_profile() -> None:
    profile = build_provider_profile(Endpoints.Motive.vehicles, _SYNTHETIC_KEY)
    prepared = profile.auth.prepare(_bare_spec())
    assert prepared.headers['X-API-Key'] == _SYNTHETIC_KEY
    assert isinstance(profile.classifier, MotiveResponseClassifier)


def test_motive_profile_never_reprs_the_secret() -> None:
    profile = build_provider_profile(Endpoints.Motive.vehicles, _SYNTHETIC_KEY)
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
        build_provider_profile(Endpoints.Motive.vehicles, wrong_shaped_auth)
    message = str(raised.value)
    assert 'motive' in message
    assert 'bare API-key string' in message
    assert type(wrong_shaped_auth).__name__ in message


def test_rejection_never_echoes_the_value() -> None:
    with pytest.raises(ConfigurationError) as raised:
        build_provider_profile(Endpoints.Motive.vehicles, {'api_key': _SYNTHETIC_KEY})
    assert _SYNTHETIC_KEY not in str(raised.value)
    assert _SYNTHETIC_KEY not in repr(raised.value)


def test_unexposed_provider_is_a_configuration_error() -> None:
    # No Samsara catalog identity exists; a hand-built one exercises the arm.
    samsara_identity = SnapshotEndpoint(Provider.SAMSARA, 'vehicles')
    with pytest.raises(ConfigurationError) as raised:
        build_provider_profile(samsara_identity, _SYNTHETIC_KEY)
    assert 'samsara' in str(raised.value)
