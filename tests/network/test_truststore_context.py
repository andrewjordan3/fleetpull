"""Tests for fleetpull.network.truststore_context."""

import ssl

from fleetpull.network.truststore_context import build_truststore_ssl_context

__all__: list[str] = []


def test_returns_ssl_context_instance() -> None:
    ssl_context: ssl.SSLContext = build_truststore_ssl_context()
    assert isinstance(ssl_context, ssl.SSLContext)


def test_protocol_is_tls_client() -> None:
    ssl_context: ssl.SSLContext = build_truststore_ssl_context()
    assert ssl_context.protocol == ssl.PROTOCOL_TLS_CLIENT


def test_secure_client_defaults_hold() -> None:
    ssl_context: ssl.SSLContext = build_truststore_ssl_context()
    assert ssl_context.check_hostname is True
    assert ssl_context.verify_mode == ssl.CERT_REQUIRED


def test_each_call_returns_distinct_context() -> None:
    first_context: ssl.SSLContext = build_truststore_ssl_context()
    second_context: ssl.SSLContext = build_truststore_ssl_context()
    assert first_context is not second_context
