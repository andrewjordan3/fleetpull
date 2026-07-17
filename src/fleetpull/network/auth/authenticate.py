# src/fleetpull/network/auth/authenticate.py
"""The real GeoTab ``Authenticate`` call: the manager's injectable.

A single-concern, single-shot, loop-free function behind a factory. The
session manager's injectable type is single-arg
(``Callable[[GeotabAuthConfig], AuthenticationResult]``), so the
transport dependencies — HTTP config, the limiter registry, the quota
scope — close over via a factory returning a named inner function.

This is the one module in ``network/auth/`` that imports httpx: it IS
the HTTP attempt the manager keeps at arm's length so the manager
itself stays pure state and choreography. Two actions only — fix
credentials (``AuthenticationError``) or fail loud
(``ProviderResponseError``); the classifier's vocabulary encodes the
CLIENT's dispatch and is deliberately not reused here. Every inbound
read flows through a private slice model; transport exceptions
propagate raw and untyped, because retry semantics for prepare-time
transport failures are the client's design question, not this
function's.
"""

import json
import logging
from collections.abc import Callable
from http import HTTPStatus
from typing import Final, NoReturn

import httpx
from pydantic import BaseModel, ConfigDict, Field

from fleetpull.config import GeotabAuthConfig, HttpConfig
from fleetpull.exceptions import AuthenticationError, ProviderResponseError
from fleetpull.network.auth.models import AuthenticationResult
from fleetpull.network.contract import body_snippet, validated_envelope_slice
from fleetpull.network.limits import RateLimiterRegistry
from fleetpull.network.posture import client_timeout, client_verify
from fleetpull.vocabulary import JsonValue

__all__: list[str] = ['build_geotab_authenticator']

logger = logging.getLogger(__name__)

# Wire-protocol tokens: Final constants, not an enum — nothing
# dispatches over these. Outbound body keys used in logic and the two
# inbound values we compare against; inbound envelope keys are consumed
# via the slice models' fields/aliases, never walked.
_APIV1_PATH: Final[str] = '/apiv1'
_METHOD_KEY: Final[str] = 'method'
_PARAMS_KEY: Final[str] = 'params'
_DATABASE_KEY: Final[str] = 'database'
_USER_NAME_KEY: Final[str] = 'userName'
_PASSWORD_KEY: Final[str] = 'password'  # the JSON-RPC field name, not a secret
_AUTHENTICATE_METHOD: Final[str] = 'Authenticate'
_THIS_SERVER_PATH: Final[str] = 'ThisServer'
_INVALID_USER_TYPE: Final[str] = 'InvalidUserException'


class _AuthenticateCredentials(BaseModel):
    """The credentials block of a successful Authenticate result.

    Only ``sessionId`` is consumed — ``database`` and ``userName`` are
    carried from config by the manager, so they are deliberately ignored
    here.
    """

    model_config = ConfigDict(frozen=True, extra='ignore', strict=True)

    session_id: str = Field(alias='sessionId')


class _AuthenticateResult(BaseModel):
    """A successful Authenticate ``result``: credentials and the host path."""

    model_config = ConfigDict(frozen=True, extra='ignore', strict=True)

    credentials: _AuthenticateCredentials
    path: str


class _AuthenticateErrorData(BaseModel):
    """The ``error.data`` block; ``type`` is the authoritative discriminator."""

    model_config = ConfigDict(frozen=True, extra='ignore', strict=True)

    type: str


class _AuthenticateError(BaseModel):
    """A failing Authenticate ``error`` envelope (inside HTTP 200)."""

    model_config = ConfigDict(frozen=True, extra='ignore', strict=True)

    message: str | None = None
    data: _AuthenticateErrorData


class _AuthenticateEnvelope(BaseModel):
    """The JSON-RPC envelope slice: exactly one of error or result is meaningful."""

    model_config = ConfigDict(frozen=True, extra='ignore', strict=True)

    error: _AuthenticateError | None = None
    result: _AuthenticateResult | None = None


def _build_authenticate_body(config: GeotabAuthConfig) -> dict[str, JsonValue]:
    """
    Build the JSON-RPC ``Authenticate`` body.

    The ``SecretStr`` is extracted HERE and only here (the manager never
    reads it), placed in the request body, and never logged.

    Args:
        config: Validated GeoTab authentication configuration.

    Returns:
        The JSON-RPC request body.
    """
    return {
        _METHOD_KEY: _AUTHENTICATE_METHOD,
        _PARAMS_KEY: {
            _DATABASE_KEY: config.database,
            _USER_NAME_KEY: config.username,
            _PASSWORD_KEY: config.password.get_secret_value(),
        },
    }


def _raise_for_authenticate_error(error: _AuthenticateError) -> NoReturn:
    """
    Translate a GeoTab Authenticate error envelope into the right raise.

    Args:
        error: The validated error slice.

    Raises:
        AuthenticationError: For ``InvalidUserException`` — bad
            credentials, the context-disambiguation principle (the same
            type on a data call is a dead session, handled by the auth
            strategy, not here).
        ProviderResponseError: For any other error type — fail loud on
            behavior never met on Authenticate. Trigger:
            ``OverLimitException`` observed here despite the local
            limiter would mean revisit.
    """
    if error.data.type == _INVALID_USER_TYPE:
        raise AuthenticationError(detail=error.message)
    detail: str = f'unexpected Authenticate error type: {error.data.type!r}'
    if error.message is not None:
        detail = f'{detail} ({error.message})'
    raise ProviderResponseError(detail=detail)


def _resolve_server(result_path: str, config: GeotabAuthConfig) -> str:
    """
    Resolve the session's server from the Authenticate result path.

    Args:
        result_path: The ``result.path`` value from the envelope.
        config: The configuration whose ``server`` was actually called.

    Returns:
        ``config.server`` when ``path`` is the ``ThisServer`` sentinel;
        otherwise the returned host.
    """
    if result_path == _THIS_SERVER_PATH:
        return config.server
    # Redirects are handled-not-assumed: no capture shows one, but the
    # protocol documents path as a possible alternate host.
    logger.info('GeoTab Authenticate redirected to host %s', result_path)
    return result_path


def _resolve_authenticate_outcome(
    response: httpx.Response, config: GeotabAuthConfig
) -> AuthenticationResult:
    """
    Turn one Authenticate HTTP response into a result or the right raise.

    Args:
        response: The completed Authenticate response.
        config: The configuration that produced the request.

    Returns:
        The authentication result on success.

    Raises:
        AuthenticationError: On ``InvalidUserException`` (bad credentials).
        ProviderResponseError: On a non-200 status, a non-JSON body, a
            malformed envelope, or any unexpected error type.
    """
    if response.status_code != HTTPStatus.OK:
        # v1 posture: Authenticate outcomes arrive in HTTP 200 per
        # verification; anything else is the API not speaking its
        # protocol. Loud-and-typed beats a retry loop against a 10/min
        # auth quota. Re-litigate on the first observed Authenticate 5xx.
        raise ProviderResponseError(
            status_code=response.status_code, detail=body_snippet(response.text)
        )
    try:
        parsed_body: JsonValue = json.loads(response.text)
    except json.JSONDecodeError as error:
        raise ProviderResponseError(
            detail=f'unparseable (non-JSON) Authenticate body: '
            f'{body_snippet(response.text)}'
        ) from error

    envelope = validated_envelope_slice(_AuthenticateEnvelope, parsed_body)
    if envelope.error is not None:
        _raise_for_authenticate_error(envelope.error)
    if envelope.result is not None:
        return AuthenticationResult(
            session_id=envelope.result.credentials.session_id,
            resolved_host=_resolve_server(envelope.result.path, config),
        )
    raise ProviderResponseError(
        detail='malformed Authenticate envelope: neither result nor error present'
    )


def build_geotab_authenticator(
    http_config: HttpConfig,
    limiter_registry: RateLimiterRegistry,
    quota_scope: str,
) -> Callable[[GeotabAuthConfig], AuthenticationResult]:
    """
    Build the real ``authenticate_fn`` the session manager consumes.

    The quota scope arrives as a parameter — the composition root names
    it — preserving the names-at-composition-root rule even inside
    GeoTab-specific machinery. Authenticate is rate-limited at a fixed
    10/min outside tiering; the composition root configures a dedicated
    scope in the registry, and an unconfigured scope propagates the
    registry's ``UnknownQuotaScopeError`` naturally.

    Args:
        http_config: Timeouts and TLS posture for the call.
        limiter_registry: The shared registry; the call takes a slot
            under ``quota_scope``.
        quota_scope: The dedicated Authenticate quota scope.

    Returns:
        A single-arg callable matching the manager's injectable type.
    """

    def authenticate(config: GeotabAuthConfig) -> AuthenticationResult:
        """
        Perform one ``Authenticate`` call and resolve its outcome.

        Args:
            config: Validated GeoTab authentication configuration.

        Returns:
            The authentication result on success.

        Raises:
            AuthenticationError: On bad credentials.
            ProviderResponseError: On a non-200 status, a non-JSON body,
                or a malformed/unexpected envelope.
            UnknownQuotaScopeError: When ``quota_scope`` is unconfigured.
            httpx.TransportError: On a transport failure — propagated raw
                and loop-free; retry is the client's design question.
        """
        limiter = limiter_registry.get(quota_scope)
        url: str = f'https://{config.server}{_APIV1_PATH}'
        request_body: dict[str, JsonValue] = _build_authenticate_body(config)
        # A fresh, context-managed client per call: Authenticate fires
        # rarely behind the manager's single-flight, so connection reuse
        # buys nothing worth a held resource.
        with (
            limiter.request_slot(),
            httpx.Client(
                verify=client_verify(http_config),
                timeout=client_timeout(http_config),
            ) as client,
        ):
            response = client.post(url, json=request_body)
        return _resolve_authenticate_outcome(response, config)

    return authenticate
