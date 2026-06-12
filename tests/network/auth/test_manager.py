"""Tests for fleetpull.network.auth.manager.

Deterministic tests use FrozenClock on a single thread; concurrency
tests use real threads with tiny waits and generous ceilings (the
``tests/network/limits/test_limiter.py`` precedent). No HTTP anywhere:
authenticate_fn is a stub.
"""

import logging
import threading
import time
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import SecretStr

from fleetpull.config.geotab import GeotabAuthConfig
from fleetpull.network.auth.manager import GeotabSessionManager
from fleetpull.network.auth.models import AuthenticationResult, GeotabSession
from fleetpull.timing.clock import FrozenClock

FROZEN_START_TIME: datetime = datetime(2026, 1, 1, tzinfo=UTC)

# The proactive refresh threshold is lifetime (14d) - margin (1d) = 13 days.
REFRESH_THRESHOLD: timedelta = timedelta(days=13)

STUB_RESULT = AuthenticationResult(
    session_id='synthetic-session-id', resolved_host='resolved.geotab.com'
)


def build_auth_config() -> GeotabAuthConfig:
    return GeotabAuthConfig(
        username='synthetic-user',
        password=SecretStr('synthetic-password'),
        database='synthetic_db',
    )


def build_frozen_clock() -> FrozenClock:
    return FrozenClock(start_time_utc=FROZEN_START_TIME)


class StubAuthenticator:
    """Counting authenticate_fn stub; can be told to fail."""

    def __init__(self) -> None:
        self.call_count = 0
        self.fail = False

    def __call__(self, config: GeotabAuthConfig) -> AuthenticationResult:
        self.call_count += 1
        if self.fail:
            raise ConnectionError('auth endpoint unreachable')
        return STUB_RESULT


class SlowAuthenticator:
    """Advances the frozen clock inside the call, simulating slow auth."""

    def __init__(self, clock: FrozenClock, delay: timedelta) -> None:
        self.clock = clock
        self.delay = delay

    def __call__(self, config: GeotabAuthConfig) -> AuthenticationResult:
        self.clock.advance(self.delay)
        return STUB_RESULT


class TestGetSessionDeterministic:
    def test_first_call_authenticates_and_populates_fields(self) -> None:
        stub = StubAuthenticator()
        manager = GeotabSessionManager(build_auth_config(), stub, build_frozen_clock())
        session = manager.get_session()
        assert stub.call_count == 1
        assert session.session_id == 'synthetic-session-id'
        assert session.resolved_host == 'resolved.geotab.com'
        assert session.database == 'synthetic_db'
        assert session.username == 'synthetic-user'
        assert session.generation == 1
        assert session.acquired_at_utc == FROZEN_START_TIME

    def test_second_call_is_a_cache_hit(self) -> None:
        stub = StubAuthenticator()
        manager = GeotabSessionManager(build_auth_config(), stub, build_frozen_clock())
        first_session = manager.get_session()
        second_session = manager.get_session()
        assert stub.call_count == 1
        assert second_session is first_session

    def test_just_below_threshold_is_a_cache_hit(self) -> None:
        stub = StubAuthenticator()
        frozen_clock = build_frozen_clock()
        manager = GeotabSessionManager(build_auth_config(), stub, frozen_clock)
        manager.get_session()
        frozen_clock.advance(REFRESH_THRESHOLD - timedelta(minutes=1))
        session = manager.get_session()
        assert stub.call_count == 1
        assert session.generation == 1

    def test_exact_threshold_refreshes_inclusively(self) -> None:
        stub = StubAuthenticator()
        frozen_clock = build_frozen_clock()
        manager = GeotabSessionManager(build_auth_config(), stub, frozen_clock)
        manager.get_session()
        frozen_clock.advance(REFRESH_THRESHOLD)
        session = manager.get_session()
        assert stub.call_count == 2
        assert session.generation == 2

    def test_pessimistic_timestamping_stamps_before_the_call(self) -> None:
        frozen_clock = build_frozen_clock()
        slow_stub = SlowAuthenticator(frozen_clock, delay=timedelta(minutes=10))
        manager = GeotabSessionManager(build_auth_config(), slow_stub, frozen_clock)
        pre_call_instant = frozen_clock.now_utc()
        session = manager.get_session()
        assert session.acquired_at_utc == pre_call_instant
        assert frozen_clock.now_utc() == pre_call_instant + timedelta(minutes=10)


class TestInvalidateDeterministic:
    def test_invalidating_current_session_reauthenticates_with_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        stub = StubAuthenticator()
        manager = GeotabSessionManager(build_auth_config(), stub, build_frozen_clock())
        current_session = manager.get_session()
        with caplog.at_level(logging.WARNING, logger='fleetpull.network.auth.manager'):
            replacement_session = manager.invalidate(current_session)
        assert stub.call_count == 2
        assert replacement_session.generation == 2
        assert any(
            record.levelno == logging.WARNING and 'rejected' in record.message
            for record in caplog.records
        )

    def test_stale_invalidation_returns_current_without_authenticating(self) -> None:
        stub = StubAuthenticator()
        manager = GeotabSessionManager(build_auth_config(), stub, build_frozen_clock())
        first_session = manager.get_session()
        second_session = manager.invalidate(first_session)
        assert second_session.generation == 2
        # Another caller still holding generation 1 reports it stale:
        # a refresh already happened, so no new authentication occurs.
        third_session = manager.invalidate(first_session)
        assert third_session is second_session
        assert stub.call_count == 2


class TestAuthenticationFailure:
    def test_cold_failure_propagates_and_cache_stays_none(self) -> None:
        stub = StubAuthenticator()
        stub.fail = True
        manager = GeotabSessionManager(build_auth_config(), stub, build_frozen_clock())
        with pytest.raises(ConnectionError, match='unreachable'):
            manager.get_session()
        assert manager._current_session is None

    def test_failed_refresh_leaves_cached_session_untouched(self) -> None:
        stub = StubAuthenticator()
        manager = GeotabSessionManager(build_auth_config(), stub, build_frozen_clock())
        primed_session = manager.get_session()
        stub.fail = True
        with pytest.raises(ConnectionError, match='unreachable'):
            manager.invalidate(primed_session)
        assert manager._current_session is primed_session
        # The cached session is still served once authentication is no
        # longer being attempted (cache hit path — clock never moved).
        stub.fail = False
        assert manager.get_session() is primed_session
        assert stub.call_count == 2


class ThreadSafeSleepyAuthenticator:
    """Counts calls under a lock and sleeps to widen race windows."""

    def __init__(self) -> None:
        self._count_lock = threading.Lock()
        self.call_count = 0

    def __call__(self, config: GeotabAuthConfig) -> AuthenticationResult:
        with self._count_lock:
            self.call_count += 1
        time.sleep(0.05)
        return STUB_RESULT


class TestConcurrency:
    def test_invalidation_stampede_authenticates_once(self) -> None:
        stub = ThreadSafeSleepyAuthenticator()
        manager = GeotabSessionManager(build_auth_config(), stub)
        primed_session: GeotabSession = manager.get_session()
        thread_count = 8
        start_barrier = threading.Barrier(thread_count)
        results_lock = threading.Lock()
        received_sessions: list[GeotabSession] = []

        def report_stale() -> None:
            start_barrier.wait()
            replacement = manager.invalidate(primed_session)
            with results_lock:
                received_sessions.append(replacement)

        worker_threads = [
            threading.Thread(target=report_stale) for _ in range(thread_count)
        ]
        for worker_thread in worker_threads:
            worker_thread.start()
        for worker_thread in worker_threads:
            worker_thread.join()

        # One call primed the session; exactly one more refreshed it.
        assert stub.call_count == 2
        assert len(received_sessions) == thread_count
        assert all(received.generation == 2 for received in received_sessions)

    def test_cold_concurrent_get_session_authenticates_once(self) -> None:
        stub = ThreadSafeSleepyAuthenticator()
        manager = GeotabSessionManager(build_auth_config(), stub)
        thread_count = 8
        start_barrier = threading.Barrier(thread_count)
        results_lock = threading.Lock()
        received_sessions: list[GeotabSession] = []

        def fetch_session() -> None:
            start_barrier.wait()
            session = manager.get_session()
            with results_lock:
                received_sessions.append(session)

        worker_threads = [
            threading.Thread(target=fetch_session) for _ in range(thread_count)
        ]
        for worker_thread in worker_threads:
            worker_thread.start()
        for worker_thread in worker_threads:
            worker_thread.join()

        assert stub.call_count == 1
        assert len(received_sessions) == thread_count
        assert all(received.generation == 1 for received in received_sessions)
