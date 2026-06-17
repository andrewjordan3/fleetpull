# tests/vocabulary/test_quota_scope.py
"""Tests for fleetpull.vocabulary.quota_scope."""

from fleetpull.vocabulary.quota_scope import QuotaScope


class TestQuotaScope:
    def test_is_str_enum(self) -> None:
        assert issubclass(QuotaScope, str)

    def test_member_values_are_lowercase_keys(self) -> None:
        assert QuotaScope.GEOTAB.value == 'geotab'
        assert QuotaScope.MOTIVE.value == 'motive'
        assert QuotaScope.SAMSARA.value == 'samsara'

    def test_closed_at_three_scopes(self) -> None:
        assert set(QuotaScope) == {
            QuotaScope.GEOTAB,
            QuotaScope.MOTIVE,
            QuotaScope.SAMSARA,
        }
