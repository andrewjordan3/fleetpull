"""TLS plumbing: SSL-context construction for the OS trust store."""

from fleetpull.network.tls.truststore_context import build_truststore_ssl_context

__all__: list[str] = ['build_truststore_ssl_context']
