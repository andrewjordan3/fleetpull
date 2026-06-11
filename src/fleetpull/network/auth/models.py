# src/fleetpull/network/auth/models.py
"""
Runtime session state for GeoTab authentication.

Internal runtime state, not user YAML — hence frozen dataclasses rather
than Pydantic models. Pure data: assembling request credentials from a
session is the auth strategy's job, so nothing here knows JSON-RPC
payload shapes.
"""

from dataclasses import dataclass
from datetime import datetime

__all__: list[str] = ['AuthenticationResult', 'GeotabSession']


@dataclass(frozen=True, slots=True)
class AuthenticationResult:
    """
    What an ``authenticate_fn`` returns to the session manager.

    GeoTab's ``Authenticate`` returns either a server path or the
    literal ``'ThisServer'``; RESOLVING that to a concrete host is the
    ``authenticate_fn``'s job (it knows which host it called), so by the
    time a result reaches the manager, ``resolved_host`` is always a
    real host.

    Attributes:
        session_id: The session credential returned by ``Authenticate``.
        resolved_host: The host all subsequent API calls must target.
    """

    session_id: str
    resolved_host: str


@dataclass(frozen=True, slots=True)
class GeotabSession:
    """
    The session the manager hands to callers.

    Attributes:
        session_id: The session credential for request payloads.
        resolved_host: The host all subsequent API calls must target.
        database: GeoTab database name, carried from config.
        username: GeoTab username, carried from config.
        generation: Monotonically increasing per manager instance; the
            single-flight staleness check compares generations.
        acquired_at_utc: When authentication was initiated, from the
            injected clock's ``now_utc()``.
    """

    session_id: str
    resolved_host: str
    database: str
    username: str
    generation: int
    acquired_at_utc: datetime
