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
``ConfigurationError`` here. The provider → (auth strategy, classifier)
knowledge lives in this module because it can live nowhere lower: the
ingress sits in the top tier and may import both ``config`` and
``network``, while ``config`` cannot import ``network``, so the mapping
cannot ride on provider configs.

Profile construction takes a ``ProviderProfileContext`` alongside the
identity and the credential: the composition-root collaborators a
provider's auth machinery draws on. Motive's static header needs none of
them and ignores the context; GeoTab's session stack consumes all three
(the authenticator's HTTP posture and limiter slot, the session
manager's clock). The context grows only when a provider's auth
machinery demands a new collaborator -- it is not a general-purpose bag.
"""

import logging
from collections.abc import Mapping
from dataclasses import dataclass

from pydantic import SecretStr, ValidationError

from fleetpull.api.identity import EndpointIdentity
from fleetpull.config import GeotabAuthConfig, HttpConfig
from fleetpull.exceptions import ConfigurationError
from fleetpull.network.auth import (
    GeotabSessionAuth,
    GeotabSessionManager,
    StaticHeaderAuth,
    build_geotab_authenticator,
)
from fleetpull.network.classifiers import (
    GeotabResponseClassifier,
    MotiveResponseClassifier,
    SamsaraResponseClassifier,
)
from fleetpull.network.client import ProviderProfile
from fleetpull.network.limits import RateLimiterRegistry
from fleetpull.timing import Clock
from fleetpull.vocabulary import Provider, QuotaScope

__all__: list[str] = ['AuthInput', 'ProviderProfileContext', 'build_provider_profile']

logger = logging.getLogger(__name__)

# The §10 auth union. SecretStr is accepted alongside the bare string so
# the config path (Sync) hands its already-wrapped credential straight
# through -- the raw value never needs unwrapping just to be re-wrapped
# at the boundary. GeoTab's arms carry the four-field credential whole.
type AuthInput = str | SecretStr | Mapping[str, str] | GeotabAuthConfig

# Provider knowledge no consumer should type (AUDIT row 20): the header
# a Motive static key travels in.
_MOTIVE_AUTH_HEADER: str = 'X-API-Key'

# Samsara's bearer credential: the header and the value prefix. The
# ingress pre-formats 'Bearer <token>' before wrapping in SecretStr --
# StaticHeaderAuth documents pre-formatting as the composition root's
# job, and this is that site.
_SAMSARA_AUTH_HEADER: str = 'Authorization'
_SAMSARA_BEARER_PREFIX: str = 'Bearer '


@dataclass(frozen=True, slots=True)
class ProviderProfileContext:
    """Composition-root collaborators provider profile construction draws on.

    Grows only when a provider's auth machinery demands a new
    collaborator; this is not a general-purpose bag.

    Attributes:
        http_config: Timeouts and TLS posture for auth-side HTTP calls.
        limiter_registry: The shared registry; auth calls take limiter
            slots like any other request (token-per-attempt).
        clock: The run's shared clock (session-age timestamping). Required
            -- no default -- because clock identity matters (the one-clock
            rule): ``Sync`` passes its shared clock, ``fetch`` constructs
            one at the profile-build site.
    """

    http_config: HttpConfig
    limiter_registry: RateLimiterRegistry
    clock: Clock


def build_provider_profile(
    endpoint: EndpointIdentity, auth: AuthInput, context: ProviderProfileContext
) -> ProviderProfile:
    """Coerce the public ``auth=`` value into the provider's client profile.

    Args:
        endpoint: The identity whose provider selects the credential
            shape and classifier; the auth value's own shape never
            drives dispatch.
        auth: The public credential value. Motive and Samsara each
            require a bare API-key/token string; GeoTab requires a
            ``GeotabAuthConfig`` or a mapping with its named fields.
        context: The composition-root collaborators (HTTP posture,
            limiter registry, clock) a provider's auth machinery draws
            on; the static-key providers ignore it, GeoTab consumes it.

    Returns:
        The ``ProviderProfile`` (``SecretStr``-carrying auth strategy
        plus response classifier) the transport client is built on.

    Raises:
        ConfigurationError: ``auth``'s shape mismatches the endpoint's
            provider (naming the expected shape and the provider, never
            the value).
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
        case Provider.GEOTAB:
            credential = _coerced_geotab_credential(endpoint, auth)
            authenticate_fn = build_geotab_authenticator(
                context.http_config,
                context.limiter_registry,
                QuotaScope.GEOTAB_AUTHENTICATE.value,
            )
            manager = GeotabSessionManager(credential, authenticate_fn, context.clock)
            return ProviderProfile(
                auth=GeotabSessionAuth(manager),
                classifier=GeotabResponseClassifier(),
            )
        case Provider.SAMSARA:
            if not isinstance(auth, str | SecretStr):
                raise ConfigurationError(
                    'auth shape mismatch',
                    provider=endpoint.provider.value,
                    endpoint=endpoint.name,
                    detail=(
                        f'Samsara auth is a bare API-token string (or SecretStr); '
                        f'got {type(auth).__name__}'
                    ),
                )
            raw_token = auth.get_secret_value() if isinstance(auth, SecretStr) else auth
            bearer = SecretStr(f'{_SAMSARA_BEARER_PREFIX}{raw_token}')
            return ProviderProfile(
                auth=StaticHeaderAuth(_SAMSARA_AUTH_HEADER, bearer),
                classifier=SamsaraResponseClassifier(),
            )


def _coerced_geotab_credential(
    endpoint: EndpointIdentity, auth: AuthInput
) -> GeotabAuthConfig:
    """Coerce the GeoTab ``auth=`` shapes into the internal credential.

    A ``GeotabAuthConfig`` passes through as-is; a plain mapping is
    validated into one (Pydantic wraps the password into ``SecretStr``).
    Every rejection names the provider and the expected shapes and the
    received type -- never the value.

    Args:
        endpoint: The GeoTab identity being fetched (error context).
        auth: The public credential value.

    Returns:
        The ``SecretStr``-carrying credential.

    Raises:
        ConfigurationError: The shape is neither accepted form, or the
            mapping's fields fail credential validation.
    """
    if isinstance(auth, GeotabAuthConfig):
        return auth
    shape_detail = (
        'GeoTab auth is a GeotabAuthConfig or a mapping with '
        'username/password/database and optional server; '
        f'got {type(auth).__name__}'
    )
    if not isinstance(auth, Mapping):
        raise ConfigurationError(
            'auth shape mismatch',
            provider=endpoint.provider.value,
            endpoint=endpoint.name,
            detail=shape_detail,
        )
    try:
        return GeotabAuthConfig.model_validate(dict(auth))
    except ValidationError as error:
        field_names = ', '.join(
            '.'.join(str(item) for item in entry['loc']) for entry in error.errors()
        )
        raise ConfigurationError(
            'auth shape mismatch',
            provider=endpoint.provider.value,
            endpoint=endpoint.name,
            detail=f'{shape_detail} (invalid fields: {field_names})',
        ) from None
