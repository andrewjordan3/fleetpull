"""Tests for fleetpull.network.auth.authenticate.

No real network anywhere: every call is served by ``httpx.MockTransport``
injected by monkeypatching ``httpx.Client``. Fixtures are synthetic, in
the captured Authenticate shapes (scrubbed ids, ``exampledb``,
``user@example.com``).
"""

import json
import logging
import ssl
from collections.abc import Callable
from datetime import UTC, datetime

import httpx
import pytest
from pydantic import SecretStr

from fleetpull.config import RateLimitConfig
from fleetpull.config.geotab import GeotabAuthConfig
from fleetpull.config.http import HttpConfig
from fleetpull.exceptions import (
    AuthenticationError,
    ProviderResponseError,
    UnknownQuotaScopeError,
)
from fleetpull.network.auth.authenticate import build_geotab_authenticator
from fleetpull.network.auth.manager import GeotabSessionManager
from fleetpull.network.auth.models import AuthenticationResult
from fleetpull.network.limits.registry import RateLimiterRegistry
from fleetpull.timing.clock import FrozenClock
from fleetpull.vocabulary import JsonObject

TEST_SCOPE = 'geotab_auth'
SYNTHETIC_PASSWORD = 'synthetic-password-123'
SYNTHETIC_SESSION_ID = 'SyntheticSessionId000001'

# The genuine class, captured before any test monkeypatches httpx.Client,
# so a test that builds two authenticators (each re-patching) still wraps
# the real client both times instead of wrapping a prior shim.
_REAL_CLIENT_CLS = httpx.Client


def build_config(server: str = 'my.geotab.com') -> GeotabAuthConfig:
    return GeotabAuthConfig(
        username='user@example.com',
        password=SecretStr(SYNTHETIC_PASSWORD),
        database='exampledb',
        server=server,
    )


def build_registry(clock: FrozenClock | None = None) -> RateLimiterRegistry:
    config = RateLimitConfig(
        requests_per_period=1, period_seconds=60.0, burst=1, max_concurrency=1
    )
    if clock is None:
        return RateLimiterRegistry({TEST_SCOPE: config})
    return RateLimiterRegistry({TEST_SCOPE: config}, clock)


def success_envelope(path: str = 'ThisServer') -> JsonObject:
    return {
        'result': {
            'credentials': {
                'database': 'exampledb',
                'sessionId': SYNTHETIC_SESSION_ID,
                'userName': 'user@example.com',
            },
            'path': path,
        },
        'jsonrpc': '2.0',
    }


def error_envelope(error_type: str, message: str) -> JsonObject:
    return {
        'error': {
            'message': message,
            'code': -32000,
            'data': {'id': '00000000-0000-0000-0000-000000000001', 'type': error_type},
            'name': 'JSONRPCError',
        },
        'jsonrpc': '2.0',
    }


class RecordingHandler:
    """MockTransport handler that records requests and replays a response."""

    def __init__(
        self,
        status_code: int = 200,
        *,
        json_body: JsonObject | None = None,
        text_body: str | None = None,
    ) -> None:
        self.status_code = status_code
        self.json_body = json_body
        self.text_body = text_body
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.json_body is not None:
            return httpx.Response(self.status_code, json=self.json_body)
        return httpx.Response(self.status_code, text=self.text_body or '')


def make_authenticator(
    monkeypatch: pytest.MonkeyPatch,
    handler: RecordingHandler,
    *,
    registry: RateLimiterRegistry,
    quota_scope: str = TEST_SCOPE,
    http_config: HttpConfig | None = None,
) -> Callable[[GeotabAuthConfig], AuthenticationResult]:
    """Build an authenticator whose httpx.Client uses the mock transport."""
    mock_transport = httpx.MockTransport(handler)

    def client_with_mock_transport(
        *, verify: ssl.SSLContext | bool = True, timeout: httpx.Timeout | None = None
    ) -> httpx.Client:
        # verify is ignored — the mock transport short-circuits real TLS.
        return _REAL_CLIENT_CLS(transport=mock_transport, timeout=timeout)

    monkeypatch.setattr(httpx, 'Client', client_with_mock_transport)
    return build_geotab_authenticator(
        http_config or HttpConfig(), registry, quota_scope
    )


class TestSuccess:
    def test_this_server_resolves_to_the_called_host(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        handler = RecordingHandler(json_body=success_envelope(path='ThisServer'))
        authenticate = make_authenticator(
            monkeypatch, handler, registry=build_registry()
        )
        result = authenticate(build_config(server='my.geotab.com'))
        assert result.session_id == SYNTHETIC_SESSION_ID
        assert result.resolved_host == 'my.geotab.com'

    def test_exactly_one_request_with_the_json_rpc_body(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        handler = RecordingHandler(json_body=success_envelope())
        authenticate = make_authenticator(
            monkeypatch, handler, registry=build_registry()
        )
        authenticate(build_config())
        assert len(handler.requests) == 1
        sent_request = handler.requests[0]
        assert str(sent_request.url) == 'https://my.geotab.com/apiv1'
        sent_body = json.loads(sent_request.content)
        params = sent_body['params']
        assert params['database'] == 'exampledb'
        assert params['userName'] == 'user@example.com'
        # Asserting on the OUTBOUND body is not logging: the secret must
        # be in the request payload and nowhere else.
        assert params['password'] == SYNTHETIC_PASSWORD

    def test_redirect_path_becomes_the_server_with_info_log(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        handler = RecordingHandler(
            json_body=success_envelope(path='alternate.geotab.com')
        )
        authenticate = make_authenticator(
            monkeypatch, handler, registry=build_registry()
        )
        with caplog.at_level(
            logging.INFO, logger='fleetpull.network.auth.authenticate'
        ):
            result = authenticate(build_config())
        assert result.resolved_host == 'alternate.geotab.com'
        assert any('redirected' in record.message for record in caplog.records)


class TestFailureEnvelopes:
    def test_invalid_user_exception_is_authentication_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        handler = RecordingHandler(
            json_body=error_envelope('InvalidUserException', 'Incorrect login')
        )
        authenticate = make_authenticator(
            monkeypatch, handler, registry=build_registry()
        )
        with pytest.raises(AuthenticationError) as exception_info:
            authenticate(build_config())
        assert 'Incorrect login' in str(exception_info.value)

    def test_other_error_type_is_provider_response_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        handler = RecordingHandler(
            json_body=error_envelope('SomeOtherException', 'mystery')
        )
        authenticate = make_authenticator(
            monkeypatch, handler, registry=build_registry()
        )
        with pytest.raises(ProviderResponseError, match='SomeOtherException'):
            authenticate(build_config())


class TestMalformedResponses:
    def test_result_missing_path_is_provider_response_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        handler = RecordingHandler(
            json_body={
                'result': {
                    'credentials': {'sessionId': SYNTHETIC_SESSION_ID},
                },
                'jsonrpc': '2.0',
            }
        )
        authenticate = make_authenticator(
            monkeypatch, handler, registry=build_registry()
        )
        with pytest.raises(ProviderResponseError, match='malformed response envelope'):
            authenticate(build_config())

    def test_error_missing_data_type_is_provider_response_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        handler = RecordingHandler(
            json_body={
                'error': {'message': 'broken', 'code': -32000, 'data': {}},
                'jsonrpc': '2.0',
            }
        )
        authenticate = make_authenticator(
            monkeypatch, handler, registry=build_registry()
        )
        with pytest.raises(ProviderResponseError, match='malformed response envelope'):
            authenticate(build_config())

    def test_neither_result_nor_error_is_provider_response_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        handler = RecordingHandler(json_body={'jsonrpc': '2.0'})
        authenticate = make_authenticator(
            monkeypatch, handler, registry=build_registry()
        )
        with pytest.raises(ProviderResponseError, match='neither result nor error'):
            authenticate(build_config())

    def test_non_json_body_is_provider_response_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        handler = RecordingHandler(text_body='<html>not json</html>')
        authenticate = make_authenticator(
            monkeypatch, handler, registry=build_registry()
        )
        with pytest.raises(ProviderResponseError, match='non-JSON'):
            authenticate(build_config())

    def test_non_200_status_is_provider_response_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        handler = RecordingHandler(status_code=503, text_body='Service Unavailable')
        authenticate = make_authenticator(
            monkeypatch, handler, registry=build_registry()
        )
        with pytest.raises(ProviderResponseError) as exception_info:
            authenticate(build_config())
        assert exception_info.value.status_code == 503


class TestLimiterIntegration:
    def test_authenticate_consumes_a_token(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        frozen_clock = FrozenClock(
            start_time_utc=datetime(2026, 1, 1, tzinfo=UTC),
            start_monotonic_seconds=1000.0,
        )
        registry = build_registry(frozen_clock)
        handler = RecordingHandler(json_body=success_envelope())
        authenticate = make_authenticator(monkeypatch, handler, registry=registry)
        authenticate(build_config())
        # burst=1, clock frozen: the single token is gone.
        assert registry.get(TEST_SCOPE)._tokens == pytest.approx(0.0)

    def test_unconfigured_scope_propagates_unknown_quota_scope_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        handler = RecordingHandler(json_body=success_envelope())
        authenticate = make_authenticator(
            monkeypatch,
            handler,
            registry=build_registry(),
            quota_scope='not_configured',
        )
        with pytest.raises(UnknownQuotaScopeError, match='not_configured'):
            authenticate(build_config())
        # The limiter raised before any request was made.
        assert len(handler.requests) == 0


class TestSecrecy:
    def test_password_appears_in_no_log_record(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.DEBUG):
            success_handler = RecordingHandler(json_body=success_envelope())
            authenticate_ok = make_authenticator(
                monkeypatch, success_handler, registry=build_registry()
            )
            authenticate_ok(build_config())

            failure_handler = RecordingHandler(
                json_body=error_envelope('InvalidUserException', 'Incorrect login')
            )
            authenticate_bad = make_authenticator(
                monkeypatch, failure_handler, registry=build_registry()
            )
            with pytest.raises(AuthenticationError):
                authenticate_bad(build_config())
        for record in caplog.records:
            assert SYNTHETIC_PASSWORD not in record.getMessage()


class TestManagerComposition:
    def test_factory_return_satisfies_manager_injectable_type(self) -> None:
        # The annotation is the test: mypy rejects a mismatch.
        authenticate_fn: Callable[[GeotabAuthConfig], AuthenticationResult] = (
            build_geotab_authenticator(HttpConfig(), build_registry(), TEST_SCOPE)
        )
        assert callable(authenticate_fn)

    def test_composes_with_the_real_session_manager(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        handler = RecordingHandler(json_body=success_envelope(path='ThisServer'))
        authenticate = make_authenticator(
            monkeypatch, handler, registry=build_registry()
        )
        config = build_config(server='my.geotab.com')
        manager = GeotabSessionManager(
            config,
            authenticate,
            FrozenClock(start_time_utc=datetime(2026, 1, 1, tzinfo=UTC)),
        )
        session = manager.get_session()
        assert session.session_id == SYNTHETIC_SESSION_ID
        assert session.resolved_host == 'my.geotab.com'
        assert session.database == 'exampledb'
        assert session.username == 'user@example.com'
