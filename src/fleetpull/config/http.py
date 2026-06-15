# src/fleetpull/config/http.py
"""HTTP transport configuration: timeouts and TLS posture.

Every network call in the package requires explicit timeouts (house
rule); this model is their single YAML-facing source.
"""

import logging

from pydantic import BaseModel, ConfigDict, Field

__all__: list[str] = ['HttpConfig']

logger = logging.getLogger(__name__)


class HttpConfig(BaseModel):
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

    model_config = ConfigDict(frozen=True, extra='forbid', validate_default=True)

    connect_timeout_seconds: float = Field(default=10.0, gt=0)
    read_timeout_seconds: float = Field(default=30.0, gt=0)
    use_truststore: bool = False
