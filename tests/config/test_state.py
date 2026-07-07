"""Tests for fleetpull.config.state."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from fleetpull.config import StateConfig


class TestStateConfig:
    def test_database_path_defaults_to_none(self) -> None:
        assert StateConfig().database_path is None

    def test_string_path_coerces(self) -> None:
        config = StateConfig(database_path='/state/db.sqlite3')
        assert config.database_path == Path('/state/db.sqlite3')

    def test_rejects_unknown_fields(self) -> None:
        with pytest.raises(ValidationError):
            StateConfig(unknown='x')  # type: ignore[call-arg]

    def test_is_frozen(self) -> None:
        config = StateConfig()
        with pytest.raises(ValidationError):
            config.database_path = Path('/x')  # type: ignore[misc]
