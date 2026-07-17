"""Shared scaffolding for the api test modules.

The transport-test seam: every ``httpx.Client`` the code under test
builds is routed through an ``httpx.MockTransport`` by monkeypatching
``httpx.Client``, so the whole real composition runs with no live
network anywhere. The synthetic secrets and the minimal Motive vehicle
wire record live here because every module in this package fakes the
same providers at the same boundary.
"""

import ssl
from collections.abc import Callable

import httpx
import pytest

from fleetpull.vocabulary import JsonValue

SYNTHETIC_MOTIVE_KEY = 'synthetic-motive-key-000'
SYNTHETIC_GEOTAB_PASS = 'synthetic-geotab-pass-000'
SYNTHETIC_SAMSARA_TOKEN = 'synthetic-samsara-token-000'

# The genuine class, captured before any test monkeypatches httpx.Client,
# so every factory wraps the real client rather than a prior shim (the
# transport-test precedent).
REAL_CLIENT_CLS = httpx.Client

Handler = Callable[[httpx.Request], httpx.Response]


def install_transport(monkeypatch: pytest.MonkeyPatch, handler: Handler) -> None:
    """Route every httpx.Client the code under test builds through ``handler``."""
    mock_transport = httpx.MockTransport(handler)

    def client_factory(
        *, verify: ssl.SSLContext | bool = True, timeout: httpx.Timeout | None = None
    ) -> httpx.Client:
        # verify is ignored -- the mock transport short-circuits real TLS.
        return REAL_CLIENT_CLS(transport=mock_transport, timeout=timeout)

    monkeypatch.setattr(httpx, 'Client', client_factory)


def vehicle_record(vehicle_id: int) -> dict[str, JsonValue]:
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
