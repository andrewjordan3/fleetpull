"""Tests for fleetpull.config.http."""

import pytest
from pydantic import ValidationError

from fleetpull.config.http import HttpConfig


class TestDefaults:
    def test_bare_config_defaults(self) -> None:
        config = HttpConfig()
        assert config.connect_timeout_seconds == 10.0
        assert config.read_timeout_seconds == 30.0
        assert config.use_truststore is False


class TestFieldValidation:
    @pytest.mark.parametrize(
        ('field_name', 'invalid_value'),
        [
            ('connect_timeout_seconds', 0.0),
            ('connect_timeout_seconds', -1.0),
            ('read_timeout_seconds', 0.0),
            ('read_timeout_seconds', -5.0),
        ],
    )
    def test_rejects_non_positive_timeouts(
        self, field_name: str, invalid_value: float
    ) -> None:
        with pytest.raises(ValidationError):
            HttpConfig(**{field_name: invalid_value})

    def test_is_frozen(self) -> None:
        config = HttpConfig()
        with pytest.raises(ValidationError):
            config.read_timeout_seconds = 99.0  # type: ignore[misc]

    def test_unknown_field_raises(self) -> None:
        with pytest.raises(ValidationError):
            HttpConfig(retries=3)  # type: ignore[call-arg]
