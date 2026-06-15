# tests/vocabulary/test_provider.py
"""Tests for fleetpull.vocabulary.provider."""

from fleetpull.vocabulary.provider import Provider


class TestProvider:
    def test_is_str_enum(self) -> None:
        assert issubclass(Provider, str)

    def test_member_values_are_lowercase_keys(self) -> None:
        assert Provider.GEOTAB.value == 'geotab'
        assert Provider.MOTIVE.value == 'motive'
        assert Provider.SAMSARA.value == 'samsara'

    def test_closed_at_three_providers(self) -> None:
        assert set(Provider) == {Provider.GEOTAB, Provider.MOTIVE, Provider.SAMSARA}
