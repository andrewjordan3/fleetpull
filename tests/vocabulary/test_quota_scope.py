# tests/vocabulary/test_quota_scope.py
"""Tests for fleetpull.vocabulary.quota_scope."""

from fleetpull.vocabulary.quota_scope import QuotaScope


class TestQuotaScope:
    def test_is_str_enum(self) -> None:
        assert issubclass(QuotaScope, str)

    def test_member_values_are_lowercase_keys(self) -> None:
        assert QuotaScope.GEOTAB_AUTHENTICATE.value == 'geotab_authenticate'
        assert QuotaScope.GEOTAB_FEED.value == 'geotab_feed'
        assert QuotaScope.GEOTAB_GET.value == 'geotab_get'
        assert QuotaScope.MOTIVE.value == 'motive'
        assert QuotaScope.SAMSARA.value == 'samsara'

    def test_closed_at_the_declared_scopes(self) -> None:
        # The closed set: two provider scopes (Motive, Samsara) plus
        # GeoTab's three method-class scopes (DESIGN section 8 -- GeoTab
        # meters per method class; GetFeed is its own class, probed
        # 2026-07-21). A new member lands here deliberately, never as a
        # side effect.
        assert set(QuotaScope) == {
            QuotaScope.GEOTAB_AUTHENTICATE,
            QuotaScope.GEOTAB_FEED,
            QuotaScope.GEOTAB_GET,
            QuotaScope.MOTIVE,
            QuotaScope.SAMSARA,
        }
