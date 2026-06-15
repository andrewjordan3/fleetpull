# src/fleetpull/network/auth/manager.py
"""GeoTab session lifecycle manager.

GeoTab authenticates by session: an ``Authenticate`` call returns a
session id and a resolved host; the session lives ~14 days but can die
early (password change; a 100-concurrent-session LRU cap per account).
This module is pure state and thread choreography — it knows nothing
about HTTP. The actual ``Authenticate`` call is injected as
``authenticate_fn``; in production that implementation is an HTTP
attempt and passes through the rate limiter like any other
(token-per-attempt has no exceptions).

Sessions are never persisted to disk — a deliberate non-goal. A session
id is a bearer-equivalent secret, and at one process per scheduled run,
in-memory sessions stay far below GeoTab's 100-session cap.
"""

import logging
import threading
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Final

from fleetpull.config.geotab import GeotabAuthConfig
from fleetpull.network.auth.models import AuthenticationResult, GeotabSession
from fleetpull.timing.clock import Clock, SystemClock

__all__: list[str] = ['GeotabSessionManager']

logger = logging.getLogger(__name__)

# Documented GeoTab policy, not a server-returned contract — there is no
# expires_in in the Authenticate response, and the docs reserve the
# right to change the lifetime.
_SESSION_LIFETIME: Final[timedelta] = timedelta(days=14)

# Proactive refresh threshold. Generous (a day, not seconds) because the
# lifetime is assumed, not negotiated: refreshing early costs one
# Authenticate call; trusting an assumed lifetime to the minute risks a
# mid-run failure.
_REFRESH_MARGIN: Final[timedelta] = timedelta(days=1)


class GeotabSessionManager:
    """
    Single-flight GeoTab session cache: one session per process, shared
    by all threads.

    Reactive invalidation (a caller reports a server-rejected session
    via :meth:`invalidate`) is the primary mechanism; proactive refresh
    near the assumed lifetime is insurance. A generation counter guards
    against invalidation stampedes: ten workers hitting expiry
    simultaneously produce one ``Authenticate`` call, not ten.

    The lock is deliberately held across the ``authenticate_fn`` call.
    That is the single-flight design: concurrent callers needing a
    refresh block on the lock and receive the just-refreshed session
    when they acquire it. This is the opposite of the SQLite
    never-hold-a-transaction-across-HTTP rule, and the difference is
    the point — do not "fix" it.

    This class never logs ``session_id`` and never touches
    ``config.password``; only ``authenticate_fn`` reads the secret.
    """

    def __init__(
        self,
        config: GeotabAuthConfig,
        authenticate_fn: Callable[[GeotabAuthConfig], AuthenticationResult],
        clock: Clock = SystemClock(),  # noqa: B008 — SystemClock is frozen and stateless; one shared default instance is intentional
    ) -> None:
        """
        Initialize the manager with no cached session.

        Args:
            config: Validated GeoTab authentication configuration.
            authenticate_fn: Performs the actual ``Authenticate`` call
                and resolves the returned host. Injected so the manager
                stays pure state and choreography (the real
                implementation in ``network/auth/authenticate.py`` is the
                one place httpx is imported); in tests it is a stub.
            clock: Time source; injected so tests can be deterministic.
        """
        self._config: GeotabAuthConfig = config
        self._authenticate_fn: Callable[[GeotabAuthConfig], AuthenticationResult] = (
            authenticate_fn
        )
        self._clock: Clock = clock
        self._lock: threading.Lock = threading.Lock()
        self._current_session: GeotabSession | None = None
        self._generation_counter: int = 0

    def get_session(self) -> GeotabSession:
        """
        Return a usable session, authenticating or refreshing as needed.

        Returns:
            The cached session, or a freshly authenticated one when no
            session exists yet or the cached session has crossed the
            proactive refresh threshold.

        Raises:
            AuthenticationError: Bad credentials (from the real
                ``authenticate_fn``).
            ProviderResponseError: A malformed or non-200 Authenticate
                response.
            UnknownQuotaScopeError: The Authenticate quota scope is
                unconfigured.
            httpx.TransportError: A transport failure during
                authentication. Cached state is untouched on any raise.

        Side Effects:
            May call ``authenticate_fn`` (blocking other threads on the
            internal lock for the duration) and replace the cached
            session.
        """
        with self._lock:
            if self._current_session is None:
                logger.debug('No GeoTab session cached; initial authentication.')
                return self._refresh_holding_lock()

            now_utc: datetime = self._clock.now_utc()
            refresh_due_at: datetime = self._current_session.acquired_at_utc + (
                _SESSION_LIFETIME - _REFRESH_MARGIN
            )
            # Inclusive >= so the boundary instant refreshes rather than
            # living in an untestable gap.
            if now_utc >= refresh_due_at:
                session_age: timedelta = now_utc - self._current_session.acquired_at_utc
                logger.debug(
                    'GeoTab session past proactive refresh threshold '
                    '(age %s); refreshing.',
                    session_age,
                )
                return self._refresh_holding_lock()

            logger.debug(
                'GeoTab session cache hit (generation %d).',
                self._current_session.generation,
            )
            return self._current_session

    def invalidate(self, stale_session: GeotabSession) -> GeotabSession:
        """
        Report a server-rejected session and receive a replacement.

        Called by a caller whose request failed with a session-invalid
        error. If another thread already refreshed since the caller read
        its session (the reported generation is older than the current
        one), the current session is returned without authenticating —
        the single-flight stampede guard.

        Args:
            stale_session: The session the server rejected.

        Returns:
            The current session: freshly authenticated, or the newer
            one another thread already obtained.

        Raises:
            AuthenticationError: Bad credentials (from the real
                ``authenticate_fn``).
            ProviderResponseError: A malformed or non-200 Authenticate
                response.
            UnknownQuotaScopeError: The Authenticate quota scope is
                unconfigured.
            httpx.TransportError: A transport failure during
                authentication. Cached state is untouched on any raise.

        Side Effects:
            May call ``authenticate_fn`` and replace the cached
            session. Logs a WARNING when the current session was
            genuinely rejected (it died before its assumed lifetime).
        """
        with self._lock:
            current_session: GeotabSession | None = self._current_session
            if (
                current_session is not None
                and stale_session.generation < current_session.generation
            ):
                logger.debug(
                    'Stale invalidation for generation %d; current '
                    'generation %d is already newer.',
                    stale_session.generation,
                    current_session.generation,
                )
                return current_session

            # The reported session IS current and the server rejected it:
            # it died before its assumed lifetime — password change or
            # LRU eviction, something that happened outside this process.
            logger.warning(
                'GeoTab session (generation %d) rejected by the server '
                'before its assumed lifetime; re-authenticating.',
                stale_session.generation,
            )
            return self._refresh_holding_lock()

    def _refresh_holding_lock(self) -> GeotabSession:
        """
        Authenticate and replace the cached session. Caller holds the lock.

        ``issued_at`` is read BEFORE calling ``authenticate_fn`` —
        pessimistic timestamping, so network latency counts against the
        session lifetime rather than silently extending it. If
        ``authenticate_fn`` raises, cached state is untouched and the
        exception propagates.
        """
        issued_at: datetime = self._clock.now_utc()
        authentication_result: AuthenticationResult = self._authenticate_fn(
            self._config
        )
        self._generation_counter += 1
        new_session = GeotabSession(
            session_id=authentication_result.session_id,
            resolved_host=authentication_result.resolved_host,
            database=self._config.database,
            username=self._config.username,
            generation=self._generation_counter,
            acquired_at_utc=issued_at,
        )
        self._current_session = new_session
        return new_session
