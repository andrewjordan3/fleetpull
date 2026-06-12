# src/fleetpull/network/truststore_context.py
"""SSL context factory using the operating system trust store.

Builds SSL contexts that verify certificates against the OS-native
trust store rather than Python's bundled certifi certificates. The
primary use case is corporate environments using aggressive proxies
(like Zscaler) that act as man-in-the-middle inspectors: these proxies
re-encrypt traffic with a private root CA that is installed in the
Windows/macOS system store but is unknown to standard Python
libraries. Without this module, requests fail with
``SSLCertVerificationError``; with it, Python trusts the proxy's root
CA, enabling secure connectivity without disabling SSL verification.
"""

import ssl
from ssl import SSLContext

import truststore

__all__: list[str] = ['build_truststore_ssl_context']


def build_truststore_ssl_context() -> SSLContext:
    """
    Create an SSLContext using truststore for system certificate validation.

    Explicitly uses ``PROTOCOL_TLS_CLIENT`` — secure defaults for
    client-side TLS with automatic protocol negotiation and certificate
    verification. Safe for library code: no global monkey-patching.

    Returns:
        A new context that validates certificates against the OS trust
        store.
    """
    return truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
