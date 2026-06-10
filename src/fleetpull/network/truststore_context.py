# fleetpull/network/truststore_context.py
"""SSL context factory using the operating system trust store.

Builds SSL contexts that verify certificates against the OS-native trust
store rather than Python's bundled certifi certificates.

Primary Use Case:
    Corporate environments using aggressive proxies (like Zscaler) that act as
    Man-in-the-Middle (MITM) inspectors. These proxies re-encrypt traffic using
    a private Root CA that is installed in the Windows/macOS system store but
    is unknown to standard Python libraries.

    Without this module, requests fail with `SSLCertVerificationError`.
    With this module, Python trusts the Zscaler Root CA, enabling secure
    connectivity without disabling SSL verification.
"""

import ssl
from ssl import SSLContext

import truststore

__all__: list[str] = ['build_truststore_ssl_context']


def build_truststore_ssl_context() -> SSLContext:
    """Create an SSLContext using truststore for system certificate validation.

    Explicitly uses PROTOCOL_TLS_CLIENT for secure client-side TLS with
    automatic protocol negotiation and certificate verification.

    Returns:
        SSLContext: A new context that validates certificates against the
            OS trust store.

    Notes:
        - PROTOCOL_TLS_CLIENT: Secure defaults for client connections
        - Safe for library code (no global monkey-patching)
    """
    return truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
