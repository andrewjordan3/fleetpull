# src/fleetpull/network/posture/client_options.py
"""The one construction of an httpx client from ``HttpConfig``.

Every fleetpull ``httpx.Client`` — the transport's held pool and the
GeoTab authenticator's fresh per-call client — is constructed from the
same two-knob transport posture (DESIGN §10), so the verify/timeout
composition is decided exactly once, here (``new_http_client``). Before
this module existed the two construction sites hand-rolled the mapping
independently and drifted on the pool-timeout member; a single owner is
what keeps that class of divergence structurally impossible.
"""

import ssl

import httpx

from fleetpull.config import HttpConfig
from fleetpull.network.tls import build_truststore_ssl_context

__all__: list[str] = ['new_http_client']


def _client_verify(http_config: HttpConfig) -> ssl.SSLContext | bool:
    """Resolve the TLS verification argument for an ``httpx.Client``.

    OS trust store behind a TLS-intercepting proxy; httpx's bundled CA
    store otherwise (the proxy is the exception, not the rule).

    Args:
        http_config: The transport posture; ``use_truststore`` selects
            the OS trust store.

    Returns:
        A truststore-backed ``SSLContext`` when ``use_truststore`` is
        set; ``True`` (httpx's bundled CA verification) otherwise.

    Raises:
        None.

    Side Effects:
        None beyond constructing the SSL context.
    """
    if http_config.use_truststore:
        return build_truststore_ssl_context()
    return True


def _client_timeout(http_config: HttpConfig) -> httpx.Timeout:
    """Resolve the timeout policy for an ``httpx.Client``.

    The two-knob posture maps onto httpx's four members as: read backs
    read, write, and pool (one slow-is-broken budget for every wait on
    an established connection), while connect — the handshake — is its
    own knob.

    Args:
        http_config: The transport posture supplying the two knobs.

    Returns:
        The ``httpx.Timeout`` carrying the policy above.

    Raises:
        None.

    Side Effects:
        None.
    """
    return httpx.Timeout(
        http_config.read_timeout_seconds,
        connect=http_config.connect_timeout_seconds,
    )


def new_http_client(http_config: HttpConfig) -> httpx.Client:
    """Construct an ``httpx.Client`` carrying the configured transport posture.

    The one place the two-knob ``HttpConfig`` becomes httpx client options:
    TLS verification (OS trust store behind a TLS-intercepting proxy, the
    bundled CA store otherwise) and the timeout policy. Both fleetpull
    construction sites — the transport's held pool and the GeoTab
    authenticator's fresh per-call client — call this, so the composition
    can never drift between them.

    Args:
        http_config: The transport posture.

    Returns:
        A new, open ``httpx.Client``; the caller owns its lifecycle.

    Side Effects:
        Constructs an httpx connection pool (opened lazily on first use).
    """
    return httpx.Client(
        verify=_client_verify(http_config),
        timeout=_client_timeout(http_config),
    )
