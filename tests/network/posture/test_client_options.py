"""Tests for fleetpull.network.posture.client_options.

These pin the single config -> httpx mapping both client-construction
sites (the transport pool, the GeoTab authenticator) consume. The
truststore context itself is covered by the tls tests; here the
truststore arm asserts wiring through a stub, not OS-store behavior.
"""

import ssl

import pytest

from fleetpull.config import HttpConfig
from fleetpull.network.posture.client_options import (
    _client_timeout,
    _client_verify,
)


class TestClientVerify:
    def test_bundled_ca_verification_by_default(self) -> None:
        assert _client_verify(HttpConfig()) is True

    def test_truststore_opt_in_returns_the_os_backed_context(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sentinel_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

        def fake_build() -> ssl.SSLContext:
            return sentinel_context

        monkeypatch.setattr(
            'fleetpull.network.posture.client_options.build_truststore_ssl_context',
            fake_build,
        )
        assert _client_verify(HttpConfig(use_truststore=True)) is sentinel_context


class TestClientTimeout:
    def test_read_backs_read_write_and_pool(self) -> None:
        # The settled policy: one slow-is-broken budget (read) for every
        # wait on an established connection; connect is its own knob.
        # This is the assertion that fails loudly if either construction
        # site's semantics are ever hand-rolled apart again.
        timeout = _client_timeout(
            HttpConfig(connect_timeout_seconds=7.0, read_timeout_seconds=21.0)
        )
        assert timeout.connect == 7.0
        assert timeout.read == 21.0
        assert timeout.write == 21.0
        assert timeout.pool == 21.0
