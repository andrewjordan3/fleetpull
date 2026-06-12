"""Tests for fleetpull.config.geotab."""

from collections.abc import Callable

import pytest
from pydantic import SecretStr, ValidationError

from fleetpull.config.geotab import GeotabAuthConfig

SYNTHETIC_PASSWORD = 'synthetic-password-123'


def build_config() -> GeotabAuthConfig:
    return GeotabAuthConfig(
        username='synthetic-user',
        password=SecretStr(SYNTHETIC_PASSWORD),
        database='synthetic_db',
    )


class TestFieldValidation:
    def test_server_defaults_to_my_geotab(self) -> None:
        assert build_config().server == 'my.geotab.com'

    @pytest.mark.parametrize('empty_field', ['username', 'database', 'server'])
    def test_empty_required_strings_rejected(self, empty_field: str) -> None:
        config_kwargs: dict[str, str | SecretStr] = {
            'username': 'synthetic-user',
            'password': SecretStr(SYNTHETIC_PASSWORD),
            'database': 'synthetic_db',
            empty_field: '',
        }
        with pytest.raises(ValidationError):
            GeotabAuthConfig(**config_kwargs)

    def test_is_frozen(self) -> None:
        config = build_config()
        with pytest.raises(ValidationError):
            config.database = 'other_db'  # type: ignore[misc]

    def test_unknown_field_raises(self) -> None:
        with pytest.raises(ValidationError):
            GeotabAuthConfig(
                username='synthetic-user',
                password=SecretStr(SYNTHETIC_PASSWORD),
                database='synthetic_db',
                api_key='nope',  # type: ignore[call-arg]
            )


class TestSecretMasking:
    @pytest.mark.parametrize('render', [repr, str])
    def test_password_plaintext_absent_and_masked(
        self, render: Callable[[GeotabAuthConfig], str]
    ) -> None:
        rendered_config: str = render(build_config())
        assert SYNTHETIC_PASSWORD not in rendered_config
        assert '**********' in rendered_config
