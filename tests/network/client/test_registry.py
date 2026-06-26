"""Tests for fleetpull.network.client.registry.

No real network: ``httpx.Client`` is monkeypatched to wrap an
``httpx.MockTransport`` so constructing and closing the registry's clients touches
no TLS and no socket. The registry never calls ``fetch_pages``, so the runtime's
limiter, jitter, and sleeper are never exercised -- only ``http_config`` is read at
client construction -- and a default ``ClientRuntime`` suffices.
"""

import random
import ssl
from collections.abc import Mapping

import httpx
import pytest

from fleetpull.config import HttpConfig, RetryConfig
from fleetpull.exceptions import ConfigurationError
from fleetpull.network.client import (
    ClientRuntime,
    ProviderClientRegistry,
    ProviderProfile,
)
from fleetpull.network.contract import (
    ClassifiedResponse,
    RequestSpec,
    ResponseClassifier,
)
from fleetpull.network.limits import RateLimiterRegistry
from fleetpull.timing import SystemSleeper
from fleetpull.vocabulary import Provider, ResponseCategory

# The genuine class, captured before any test monkeypatches httpx.Client.
_REAL_CLIENT_CLS = httpx.Client


class _NullAuth:
    """AuthStrategy double; never called (the registry does not fetch)."""

    def prepare(self, spec: RequestSpec) -> RequestSpec:
        return spec

    def on_auth_failure(self) -> bool:
        return False


class _NullClassifier(ResponseClassifier):
    """ResponseClassifier double; never called (the registry does not fetch)."""

    def classify_response(
        self, status_code: int, headers: Mapping[str, str], body_text: str
    ) -> ClassifiedResponse:
        return ClassifiedResponse(category=ResponseCategory.SUCCESS)


def _profile() -> ProviderProfile:
    return ProviderProfile(auth=_NullAuth(), classifier=_NullClassifier())


def _runtime() -> ClientRuntime:
    """A real runtime; only ``http_config`` is read at client construction."""
    return ClientRuntime(
        http_config=HttpConfig(),
        retry_config=RetryConfig(),
        limiter_registry=RateLimiterRegistry({}),
        random_source=random.Random(),
        sleeper=SystemSleeper(),
    )


def _patch_httpx_client(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make every internal ``httpx.Client`` wrap a mock transport (no TLS)."""

    def factory(
        *, verify: ssl.SSLContext | bool = True, timeout: httpx.Timeout | None = None
    ) -> httpx.Client:
        return _REAL_CLIENT_CLS(
            transport=httpx.MockTransport(lambda _request: httpx.Response(200)),
            timeout=timeout,
        )

    monkeypatch.setattr(httpx, 'Client', factory)


class TestClientFor:
    def test_returns_the_clients_keyed_by_provider(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _patch_httpx_client(monkeypatch)
        motive_profile = _profile()
        samsara_profile = _profile()
        profiles = {
            Provider.MOTIVE: motive_profile,
            Provider.SAMSARA: samsara_profile,
        }
        with ProviderClientRegistry(profiles, _runtime()) as registry:
            assert registry.client_for(Provider.MOTIVE)._profile is motive_profile
            assert registry.client_for(Provider.SAMSARA)._profile is samsara_profile

    def test_returns_the_same_instance_per_provider(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _patch_httpx_client(monkeypatch)
        profiles = {Provider.MOTIVE: _profile(), Provider.SAMSARA: _profile()}
        with ProviderClientRegistry(profiles, _runtime()) as registry:
            assert registry.client_for(Provider.MOTIVE) is registry.client_for(
                Provider.MOTIVE
            )
            assert registry.client_for(Provider.MOTIVE) is not registry.client_for(
                Provider.SAMSARA
            )

    def test_open_but_unconfigured_provider_raises_configuration_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _patch_httpx_client(monkeypatch)
        profiles = {Provider.MOTIVE: _profile()}
        with (
            ProviderClientRegistry(profiles, _runtime()) as registry,
            pytest.raises(ConfigurationError),
        ):
            registry.client_for(Provider.SAMSARA)

    def test_before_enter_raises_runtime_error(self) -> None:
        registry = ProviderClientRegistry({Provider.MOTIVE: _profile()}, _runtime())
        with pytest.raises(RuntimeError, match='not open'):
            registry.client_for(Provider.MOTIVE)

    def test_after_exit_raises_runtime_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _patch_httpx_client(monkeypatch)
        registry = ProviderClientRegistry({Provider.MOTIVE: _profile()}, _runtime())
        with registry:
            pass
        with pytest.raises(RuntimeError, match='not open'):
            registry.client_for(Provider.MOTIVE)


class TestLifecycle:
    def test_exit_closes_every_pool(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_httpx_client(monkeypatch)
        profiles = {Provider.MOTIVE: _profile(), Provider.SAMSARA: _profile()}
        registry = ProviderClientRegistry(profiles, _runtime())
        with registry:
            clients = [
                registry.client_for(Provider.MOTIVE),
                registry.client_for(Provider.SAMSARA),
            ]
            assert all(not c._http_client.is_closed for c in clients)
        assert all(c._http_client.is_closed for c in clients)

    def test_enter_unwinds_and_publishes_nothing_when_a_client_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        opened: list[httpx.Client] = []
        attempts = {'count': 0}

        def factory(
            *,
            verify: ssl.SSLContext | bool = True,
            timeout: httpx.Timeout | None = None,
        ) -> httpx.Client:
            attempts['count'] += 1
            if attempts['count'] == 2:
                raise RuntimeError('pool open failed')
            client = _REAL_CLIENT_CLS(
                transport=httpx.MockTransport(lambda _request: httpx.Response(200)),
                timeout=timeout,
            )
            opened.append(client)
            return client

        monkeypatch.setattr(httpx, 'Client', factory)
        profiles = {Provider.MOTIVE: _profile(), Provider.SAMSARA: _profile()}
        registry = ProviderClientRegistry(profiles, _runtime())
        with pytest.raises(RuntimeError, match='pool open failed'), registry:
            pass
        # The first pool opened, then closed on the unwind.
        assert len(opened) == 1
        assert opened[0].is_closed
        # The failed enter published no client map and left the registry unentered,
        # so a lookup raises "not open" rather than returning the closed client.
        with pytest.raises(RuntimeError, match='not open'):
            registry.client_for(Provider.MOTIVE)
