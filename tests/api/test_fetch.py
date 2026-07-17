"""Runtime tests for the public ``fetch`` verb, faked at the httpx boundary.

``httpx.MockTransport`` is injected by monkeypatching ``httpx.Client``
(the transport-test seam) so the entire real composition -- registry
discovery, auth ingress, limiter, retry, page decoding, validation,
frame construction -- runs under every test with no live network
anywhere. Responses use each provider's real wire shape: Motive's
``{"vehicles": [{"vehicle": {...}}], "pagination": {...}}`` envelopes
(synthetic identifiers) and GeoTab's committed 2026-07-09 capture set
(``tests/geotab_devices_capture.py``).
"""

import json
import ssl
from collections.abc import Callable

import httpx
import polars as pl
import pytest

from fleetpull import (
    AuthenticationError,
    ConfigurationError,
    Endpoints,
    ProviderResponseError,
    fetch,
)
from fleetpull.vocabulary import JsonObject, JsonValue
from tests.geotab_devices_capture import (
    AUTHENTICATE_SUCCESS_JSON,
    SEEK_PAGE_1_RESPONSE,
    SEEK_PAGE_2_RESPONSE,
    SEEK_TERMINAL_RESPONSE,
)

_SYNTHETIC_KEY = 'synthetic-motive-key-000'
_SYNTHETIC_GEOTAB_PASS = 'synthetic-geotab-pass-000'

# The genuine class, captured before any test monkeypatches httpx.Client,
# so every factory wraps the real client rather than a prior shim (the
# transport-test precedent).
_REAL_CLIENT_CLS = httpx.Client

_Handler = Callable[[httpx.Request], httpx.Response]


def _install_transport(monkeypatch: pytest.MonkeyPatch, handler: _Handler) -> None:
    """Route every httpx.Client the code under test builds through ``handler``."""
    mock_transport = httpx.MockTransport(handler)

    def client_factory(
        *, verify: ssl.SSLContext | bool = True, timeout: httpx.Timeout | None = None
    ) -> httpx.Client:
        # verify is ignored -- the mock transport short-circuits real TLS.
        return _REAL_CLIENT_CLS(transport=mock_transport, timeout=timeout)

    monkeypatch.setattr(httpx, 'Client', client_factory)


def _vehicle_record(vehicle_id: int) -> dict[str, JsonValue]:
    """A minimal valid Motive vehicle wire record, synthetic throughout."""
    return {
        'id': vehicle_id,
        'company_id': 77,
        'number': f'UNIT-{vehicle_id}',
        'status': 'active',
        'ifta': False,
        'created_at': '2026-01-01T00:00:00Z',
        'updated_at': '2026-01-02T00:00:00Z',
    }


def _paged_vehicles_handler(total_pages: int) -> _Handler:
    """One vehicle per page; the pagination echo drives the page loop."""

    def handler(request: httpx.Request) -> httpx.Response:
        page_no = int(request.url.params['page_no'])
        return httpx.Response(
            200,
            json={
                'vehicles': [{'vehicle': _vehicle_record(page_no)}],
                'pagination': {'page_no': page_no, 'per_page': 1, 'total': total_pages},
            },
        )

    return handler


def _empty_vehicles_handler(request: httpx.Request) -> httpx.Response:
    return httpx.Response(
        200,
        json={
            'vehicles': [],
            'pagination': {'page_no': 1, 'per_page': 100, 'total': 0},
        },
    )


def test_happy_path_spans_pages_and_types_the_frame(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_transport(monkeypatch, _paged_vehicles_handler(total_pages=3))
    frame = fetch(Endpoints.Motive.vehicles, auth=_SYNTHETIC_KEY)
    assert frame.height == 3
    assert frame['vehicle_id'].to_list() == [1, 2, 3]
    assert frame.schema['vehicle_id'] == pl.Int64
    assert frame.schema['created_at'] == pl.Datetime(time_unit='us', time_zone='UTC')


def test_credential_header_reaches_the_wire(monkeypatch: pytest.MonkeyPatch) -> None:
    seen_headers: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_headers.append(request.headers['X-API-Key'])
        return _paged_vehicles_handler(total_pages=1)(request)

    _install_transport(monkeypatch, handler)
    fetch(Endpoints.Motive.vehicles, auth=_SYNTHETIC_KEY)
    assert seen_headers == [_SYNTHETIC_KEY]


def test_empty_listing_yields_zero_rows_with_the_full_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_transport(monkeypatch, _paged_vehicles_handler(total_pages=1))
    populated_schema = fetch(Endpoints.Motive.vehicles, auth=_SYNTHETIC_KEY).schema

    _install_transport(monkeypatch, _empty_vehicles_handler)
    empty_frame = fetch(Endpoints.Motive.vehicles, auth=_SYNTHETIC_KEY)
    assert empty_frame.height == 0
    assert empty_frame.schema == populated_schema


def test_auth_provider_mismatch_makes_no_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def no_http_expected(request: httpx.Request) -> httpx.Response:
        raise AssertionError('the mismatch must be rejected before any request')

    _install_transport(monkeypatch, no_http_expected)
    with pytest.raises(ConfigurationError) as raised:
        fetch(Endpoints.Motive.vehicles, auth={'api_key': _SYNTHETIC_KEY})
    assert 'bare API-key string' in str(raised.value)


def test_success_status_with_non_json_body_is_a_provider_response_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A TLS-intercepting proxy's block page: HTTP 200, HTML body.
    def block_page(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            text='<html><body>Access blocked by proxy</body></html>',
            headers={'content-type': 'text/html'},
        )

    _install_transport(monkeypatch, block_page)
    with pytest.raises(ProviderResponseError):
        fetch(Endpoints.Motive.vehicles, auth=_SYNTHETIC_KEY)


def test_no_raise_path_ever_carries_the_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # 401 with Motive's observed body: AuthenticationError whose message and
    # repr must carry provider text only, never the credential fetch was given.
    def rejects_credentials(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={'error_message': 'invalid API key'})

    _install_transport(monkeypatch, rejects_credentials)
    with pytest.raises(AuthenticationError) as raised:
        fetch(Endpoints.Motive.vehicles, auth=_SYNTHETIC_KEY)
    assert _SYNTHETIC_KEY not in str(raised.value)
    assert _SYNTHETIC_KEY not in repr(raised.value)


class _GeotabHandler:
    """One JSON-RPC route: Authenticate, seek Get pages in order, GetCountOf.

    Records every Authenticate body and every data call's injected
    ``params.credentials`` so the e2e test can prove the session stack
    composed: one Authenticate, session credentials on every data call.
    """

    def __init__(self) -> None:
        self.authenticate_bodies: list[JsonObject] = []
        self.data_credentials: list[JsonObject] = []
        self._pages = iter(
            [SEEK_PAGE_1_RESPONSE, SEEK_PAGE_2_RESPONSE, SEEK_TERMINAL_RESPONSE]
        )

    def __call__(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        if body['method'] == 'Authenticate':
            self.authenticate_bodies.append(body)
            return httpx.Response(200, text=AUTHENTICATE_SUCCESS_JSON)
        self.data_credentials.append(body['params']['credentials'])
        if body['method'] == 'GetCountOf':
            # CONSTRUCTED: the count matching the two committed pages (the
            # captured envelope carries the live fleet's 5,666).
            return httpx.Response(200, json={'result': 6, 'jsonrpc': '2.0'})
        assert body['method'] == 'Get'
        return httpx.Response(200, json=next(self._pages))


def test_geotab_devices_end_to_end_through_the_session_stack(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The first live-shaped proof of auth stack + transport POST +
    # classifier + seek decoder + completeness guard, end to end (the
    # once-unwired auth stack's composition gap): Authenticate success, two
    # captured Get pages, the empty terminal page, a matching GetCountOf.
    handler = _GeotabHandler()
    _install_transport(monkeypatch, handler)
    frame = fetch(
        Endpoints.Geotab.devices,
        auth={
            'username': 'user@example.com',
            'password': _SYNTHETIC_GEOTAB_PASS,
            'database': 'exampledb',
        },
    )
    assert frame.height == 6
    assert frame['id'].to_list() == ['b101', 'b102', 'b105', 'b106', 'b107', 'b10A']
    assert frame.schema['active_to'] == pl.Datetime(time_unit='us', time_zone='UTC')
    # Exactly one Authenticate fired for the whole walk.
    assert len(handler.authenticate_bodies) == 1
    # Every data call -- three Get pages and the GetCountOf -- carried the
    # session credentials the strategy injected.
    assert len(handler.data_credentials) == 4
    assert all(
        credentials['sessionId'] == 'SyntheticSessionId000001'
        and credentials['database'] == 'exampledb'
        for credentials in handler.data_credentials
    )


def test_geotab_fetch_failure_never_carries_the_password(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def rejects_everything(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, text='no route')

    _install_transport(monkeypatch, rejects_everything)
    with pytest.raises(ProviderResponseError) as raised:
        fetch(
            Endpoints.Geotab.devices,
            auth={
                'username': 'user@example.com',
                'password': _SYNTHETIC_GEOTAB_PASS,
                'database': 'exampledb',
            },
        )
    assert _SYNTHETIC_GEOTAB_PASS not in str(raised.value)
    assert _SYNTHETIC_GEOTAB_PASS not in repr(raised.value)
