"""Tests for fleetpull.network.auth.models."""

import dataclasses
from datetime import UTC, datetime

import pytest

from fleetpull.network.auth.models import AuthenticationResult, GeotabSession


def build_result() -> AuthenticationResult:
    return AuthenticationResult(
        session_id='synthetic-session-id', resolved_host='resolved.geotab.com'
    )


def build_session() -> GeotabSession:
    return GeotabSession(
        session_id='synthetic-session-id',
        resolved_host='resolved.geotab.com',
        database='synthetic_db',
        username='synthetic-user',
        generation=1,
        acquired_at_utc=datetime(2026, 1, 1, tzinfo=UTC),
    )


class TestAuthenticationResult:
    def test_is_frozen(self) -> None:
        result = build_result()
        with pytest.raises(dataclasses.FrozenInstanceError):
            result.session_id = 'other'  # type: ignore[misc]

    def test_has_slots_no_dict(self) -> None:
        assert not hasattr(build_result(), '__dict__')


class TestGeotabSession:
    def test_is_frozen(self) -> None:
        session = build_session()
        with pytest.raises(dataclasses.FrozenInstanceError):
            session.generation = 2  # type: ignore[misc]

    def test_has_slots_no_dict(self) -> None:
        assert not hasattr(build_session(), '__dict__')
