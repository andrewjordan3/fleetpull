"""Tests for fleetpull.network.auth.strategies."""

import threading
from datetime import UTC, datetime

import pytest
from pydantic import SecretStr

from fleetpull.network.auth.models import GeotabSession
from fleetpull.network.auth.strategies import GeotabSessionAuth, StaticHeaderAuth
from fleetpull.network.contract.request import HttpMethod, RequestSpec

SYNTHETIC_TOKEN = 'synthetic-token-plaintext'


def build_session(generation: int) -> GeotabSession:
    return GeotabSession(
        session_id=f'SyntheticSessionId-gen{generation}',
        resolved_host='resolved.example.geotab.com',
        database='exampledb',
        username='user@example.com',
        generation=generation,
        acquired_at_utc=datetime(2026, 6, 1, tzinfo=UTC),
    )


class StubSessionManager:
    """Scripted GeotabSessionManager stand-in.

    Hands out sessions from a queue (last one repeats) and records
    which sessions were invalidated, tagged with the calling thread.
    """

    def __init__(self, sessions: list[GeotabSession]) -> None:
        self._sessions = list(sessions)
        self._stub_lock = threading.Lock()
        self.invalidated: list[tuple[int, GeotabSession]] = []
        self.get_session_calls: int = 0

    def get_session(self) -> GeotabSession:
        with self._stub_lock:
            self.get_session_calls += 1
            if len(self._sessions) > 1:
                return self._sessions.pop(0)
            return self._sessions[0]

    def invalidate(self, stale_session: GeotabSession) -> GeotabSession:
        with self._stub_lock:
            self.invalidated.append((threading.get_ident(), stale_session))
            return self._sessions[0]


def build_geotab_spec() -> RequestSpec:
    return RequestSpec(
        method=HttpMethod.POST,
        url='https://my.geotab.com/apiv1?probe=1',
        json_body={'method': 'Get', 'params': {'typeName': 'Device'}},
    )


def build_strategy(manager: StubSessionManager) -> GeotabSessionAuth:
    """Construct the strategy around a stub manager (single ignore site)."""
    return GeotabSessionAuth(manager)  # type: ignore[arg-type]


class TestStaticHeaderAuth:
    def test_header_injected_and_existing_preserved(self) -> None:
        strategy = StaticHeaderAuth('Authorization', SecretStr(SYNTHETIC_TOKEN))
        spec = RequestSpec(
            method=HttpMethod.GET,
            url='https://api.example.com/v1/vehicles',
            headers={'Accept': 'application/json'},
        )
        prepared_spec = strategy.prepare(spec)
        assert prepared_spec.headers == {
            'Accept': 'application/json',
            'Authorization': SYNTHETIC_TOKEN,
        }

    def test_secret_extracted_only_into_returned_spec(self) -> None:
        strategy = StaticHeaderAuth('X-Api-Key', SecretStr(SYNTHETIC_TOKEN))
        assert SYNTHETIC_TOKEN not in repr(strategy)
        prepared_spec = strategy.prepare(
            RequestSpec(method=HttpMethod.GET, url='https://api.example.com')
        )
        assert prepared_spec.headers['X-Api-Key'] == SYNTHETIC_TOKEN
        # Still masked after use: the extracted string is never stored.
        assert SYNTHETIC_TOKEN not in repr(strategy)

    def test_on_auth_failure_is_false(self) -> None:
        strategy = StaticHeaderAuth('Authorization', SecretStr(SYNTHETIC_TOKEN))
        assert strategy.on_auth_failure() is False


class TestGeotabSessionAuthPrepare:
    def test_credentials_injected_with_exactly_three_keys(self) -> None:
        stub_manager = StubSessionManager([build_session(generation=1)])
        strategy = build_strategy(stub_manager)
        prepared_spec = strategy.prepare(build_geotab_spec())
        assert prepared_spec.json_body is not None
        prepared_params = prepared_spec.json_body['params']
        assert isinstance(prepared_params, dict)
        assert prepared_params['credentials'] == {
            'database': 'exampledb',
            'sessionId': 'SyntheticSessionId-gen1',
            'userName': 'user@example.com',
        }
        # Existing params survive alongside the injected credentials.
        assert prepared_params['typeName'] == 'Device'

    def test_incoming_json_body_not_mutated(self) -> None:
        stub_manager = StubSessionManager([build_session(generation=1)])
        strategy = build_strategy(stub_manager)
        original_body: dict[str, object] = {
            'method': 'Get',
            'params': {'typeName': 'Device'},
        }
        spec = RequestSpec(
            method=HttpMethod.POST,
            url='https://my.geotab.com/apiv1',
            json_body=original_body,  # type: ignore[arg-type]
        )
        strategy.prepare(spec)
        assert original_body == {'method': 'Get', 'params': {'typeName': 'Device'}}

    def test_netloc_rewritten_scheme_path_query_intact(self) -> None:
        stub_manager = StubSessionManager([build_session(generation=1)])
        strategy = build_strategy(stub_manager)
        prepared_spec = strategy.prepare(build_geotab_spec())
        assert prepared_spec.url == 'https://resolved.example.geotab.com/apiv1?probe=1'

    def test_missing_params_key_creates_credentials_only_params(self) -> None:
        stub_manager = StubSessionManager([build_session(generation=1)])
        strategy = build_strategy(stub_manager)
        spec = RequestSpec(
            method=HttpMethod.POST,
            url='https://my.geotab.com/apiv1',
            json_body={'method': 'GetSystemTimeUtc'},
        )
        prepared_spec = strategy.prepare(spec)
        assert prepared_spec.json_body is not None
        prepared_params = prepared_spec.json_body['params']
        assert isinstance(prepared_params, dict)
        assert set(prepared_params) == {'credentials'}

    def test_stale_caller_credentials_overwritten(self) -> None:
        stub_manager = StubSessionManager([build_session(generation=1)])
        strategy = build_strategy(stub_manager)
        spec = RequestSpec(
            method=HttpMethod.POST,
            url='https://my.geotab.com/apiv1',
            json_body={'method': 'Get', 'params': {'credentials': {'stale': 'value'}}},
        )
        prepared_spec = strategy.prepare(spec)
        assert prepared_spec.json_body is not None
        prepared_params = prepared_spec.json_body['params']
        assert isinstance(prepared_params, dict)
        assert prepared_params['credentials'] == {
            'database': 'exampledb',
            'sessionId': 'SyntheticSessionId-gen1',
            'userName': 'user@example.com',
        }

    def test_body_less_spec_raises_before_fetching_a_session(self) -> None:
        stub_manager = StubSessionManager([build_session(generation=1)])
        strategy = build_strategy(stub_manager)
        body_less_spec = RequestSpec(
            method=HttpMethod.POST, url='https://my.geotab.com/apiv1'
        )
        with pytest.raises(ValueError, match='JSON-RPC body'):
            strategy.prepare(body_less_spec)
        # The guard must fire before the session fetch: a malformed
        # spec is a programming error and must not trigger a real
        # Authenticate call.
        assert stub_manager.get_session_calls == 0

    def test_non_mapping_params_raises_value_error(self) -> None:
        stub_manager = StubSessionManager([build_session(generation=1)])
        strategy = build_strategy(stub_manager)
        spec = RequestSpec(
            method=HttpMethod.POST,
            url='https://my.geotab.com/apiv1',
            json_body={'method': 'Get', 'params': ['not', 'a', 'mapping']},
        )
        with pytest.raises(ValueError, match='must be a mapping'):
            strategy.prepare(spec)


class TestGeotabSessionAuthFailure:
    def test_on_auth_failure_invalidates_and_returns_true(self) -> None:
        stub_manager = StubSessionManager([build_session(generation=1)])
        strategy = build_strategy(stub_manager)
        strategy.prepare(build_geotab_spec())
        assert strategy.on_auth_failure() is True
        assert len(stub_manager.invalidated) == 1
        assert stub_manager.invalidated[0][1].generation == 1

    def test_on_auth_failure_before_prepare_raises_runtime_error(self) -> None:
        stub_manager = StubSessionManager([build_session(generation=1)])
        strategy = build_strategy(stub_manager)
        with pytest.raises(RuntimeError, match='before any prepare'):
            strategy.on_auth_failure()

    def test_each_thread_invalidates_its_own_session(self) -> None:
        # The stub hands generation 1 to the first preparing thread and
        # generation 2 to the second; which thread gets which is
        # irrelevant — each must invalidate the one IT prepared with.
        stub_manager = StubSessionManager(
            [build_session(generation=1), build_session(generation=2)]
        )
        strategy = build_strategy(stub_manager)
        both_prepared = threading.Barrier(2)
        results_lock = threading.Lock()
        prepared_by_thread: dict[int, str] = {}

        def prepare_then_fail() -> None:
            prepared_spec = strategy.prepare(build_geotab_spec())
            assert prepared_spec.json_body is not None
            prepared_params = prepared_spec.json_body['params']
            assert isinstance(prepared_params, dict)
            credentials = prepared_params['credentials']
            assert isinstance(credentials, dict)
            session_id = credentials['sessionId']
            assert isinstance(session_id, str)
            with results_lock:
                prepared_by_thread[threading.get_ident()] = session_id
            # Both threads prepare before either fails: a shared
            # instance attribute would make both failures invalidate
            # the last-prepared session.
            both_prepared.wait()
            strategy.on_auth_failure()

        worker_threads = [threading.Thread(target=prepare_then_fail) for _ in range(2)]
        for worker_thread in worker_threads:
            worker_thread.start()
        for worker_thread in worker_threads:
            worker_thread.join()

        assert len(stub_manager.invalidated) == 2
        invalidated_by_thread = {
            thread_ident: invalidated_session.session_id
            for thread_ident, invalidated_session in stub_manager.invalidated
        }
        assert invalidated_by_thread == prepared_by_thread
        assert sorted(prepared_by_thread.values()) == [
            'SyntheticSessionId-gen1',
            'SyntheticSessionId-gen2',
        ]
