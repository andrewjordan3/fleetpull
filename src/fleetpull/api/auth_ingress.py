# src/fleetpull/api/auth_ingress.py
"""The auth ingress: the one public ``auth=`` shape, coerced immediately.

The public verbs take a single lax ``auth=`` value -- a bare string for
single-credential providers (Motive, Samsara), named fields (a plain
mapping or ``GeotabAuthConfig``) for GeoTab's four-field credential.
Tuples are rejected in both directions: the 1-tuple requires the
trailing-comma trap and the 4-tuple invites transposed fields discovered
only at auth-failure time (DESIGN §10). Ingress coerces every accepted
shape into the internal ``SecretStr``-carrying auth at this boundary, so
no bare secret survives past it -- not into a repr, an exception
message, or a log line.

Dispatch keys off the endpoint identity's provider, never off the auth
value's shape: a shape that happens to fit another provider is still a
``ConfigurationError`` here. The provider → (header, classifier)
knowledge lives in this module because it can live nowhere lower: the
ingress sits in the top tier and may import both ``config`` and
``network``, while ``config`` cannot import ``network``, so the mapping
cannot ride on provider configs.
"""

import logging
from collections.abc import Mapping

from pydantic import SecretStr

from fleetpull.api.identity import EndpointIdentity
from fleetpull.config import GeotabAuthConfig
from fleetpull.exceptions import ConfigurationError
from fleetpull.network.auth import StaticHeaderAuth
from fleetpull.network.classifiers import MotiveResponseClassifier
from fleetpull.network.client import ProviderProfile
from fleetpull.vocabulary import Provider

__all__: list[str] = ['AuthInput', 'build_provider_profile']

logger = logging.getLogger(__name__)

# The §10 auth union. GeoTab's arms are accepted by the signature today
# so its arrival (roadmap item 7) changes no public signature. SecretStr
# is accepted alongside the bare string so the config path (Sync) hands
# its already-wrapped credential straight through -- the raw value never
# needs unwrapping just to be re-wrapped at the boundary.
type AuthInput = str | SecretStr | Mapping[str, str] | GeotabAuthConfig

# Provider knowledge no consumer should type (AUDIT row 20): the header
# a Motive static key travels in.
_MOTIVE_AUTH_HEADER: str = 'X-API-Key'


def build_provider_profile(
    endpoint: EndpointIdentity, auth: AuthInput
) -> ProviderProfile:
    """Coerce the public ``auth=`` value into the provider's client profile.

    Args:
        endpoint: The identity whose provider selects the credential
            shape and classifier; the auth value's own shape never
            drives dispatch.
        auth: The public credential value. Motive requires a bare
            API-key string.

    Returns:
        The ``ProviderProfile`` (``SecretStr``-carrying auth strategy
        plus response classifier) the transport client is built on.

    Raises:
        ConfigurationError: ``auth``'s shape mismatches the endpoint's
            provider (naming the expected shape and the provider, never
            the value), or the provider has no exposed endpoints yet.
    """
    match endpoint.provider:
        case Provider.MOTIVE:
            if not isinstance(auth, str | SecretStr):
                raise ConfigurationError(
                    'auth shape mismatch',
                    provider=endpoint.provider.value,
                    endpoint=endpoint.name,
                    detail=(
                        f'Motive auth is a bare API-key string (or SecretStr); '
                        f'got {type(auth).__name__}'
                    ),
                )
            secret = auth if isinstance(auth, SecretStr) else SecretStr(auth)
            return ProviderProfile(
                auth=StaticHeaderAuth(_MOTIVE_AUTH_HEADER, secret),
                classifier=MotiveResponseClassifier(),
            )
        case Provider.SAMSARA | Provider.GEOTAB:
            # Signature-ready, unreachable through the catalog today: no
            # Samsara/GeoTab identity exists, so construction code here
            # would be dead. Arms fill in as their endpoints port.
            raise ConfigurationError(
                'provider has no exposed endpoints',
                provider=endpoint.provider.value,
                endpoint=endpoint.name,
                detail='no catalog identity exists for this provider yet',
            )
