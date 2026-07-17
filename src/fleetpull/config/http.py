# src/fleetpull/config/http.py
"""HTTP transport configuration: timeouts and TLS posture.

Every network call in the package requires explicit timeouts (house
rule); this model is their single YAML-facing source.
"""

from pydantic import Field

from fleetpull.config.base import ConfigModel

__all__: list[str] = ['HttpConfig']


class HttpConfig(ConfigModel):
    """
    User-facing HTTP transport settings, one instance per run.

    Attributes:
        connect_timeout_seconds: Timeout for establishing a connection.
        read_timeout_seconds: Timeout for reading a response.
        use_truststore: Build SSL contexts from the operating system's
            trust store (``network/tls/``) — required behind
            TLS-intercepting corporate proxies. Default False:
            unproxied environments (production deployment targets) use
            httpx's bundled CA store; the trust-store path is opt-in
            where the proxy is the exception, not the rule.
    """

    connect_timeout_seconds: float = Field(default=10.0, gt=0)
    read_timeout_seconds: float = Field(default=30.0, gt=0)
    use_truststore: bool = False
