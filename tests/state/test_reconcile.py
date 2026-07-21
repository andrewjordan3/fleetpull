"""Tests for fleetpull.state.reconcile."""

from datetime import UTC, datetime, timedelta

from fleetpull.state.reconcile import RosterDelta, is_roster_stale, reconcile

NOW = datetime(2026, 6, 16, 12, 0, tzinfo=UTC)


class TestReconcile:
    def test_new_keys_go_to_zero(self) -> None:
        delta = reconcile({}, ['a', 'b'], eviction_threshold=3)
        assert delta == RosterDelta(
            to_zero=frozenset({'a', 'b'}),
            to_increment=frozenset(),
            to_evict=frozenset(),
        )

    def test_reappeared_nonzero_resets_present_zero_does_not(self) -> None:
        delta = reconcile({'a': 2, 'b': 0}, ['a', 'b'], eviction_threshold=3)
        assert delta.to_zero == frozenset({'a'})
        assert delta.to_increment == frozenset()
        assert delta.to_evict == frozenset()

    def test_absent_under_threshold_increments(self) -> None:
        delta = reconcile({'a': 1}, [], eviction_threshold=3)
        assert delta.to_increment == frozenset({'a'})
        assert delta.to_evict == frozenset()

    def test_absent_at_threshold_evicts(self) -> None:
        delta = reconcile({'a': 3}, [], eviction_threshold=3)
        assert delta.to_increment == frozenset()
        assert delta.to_evict == frozenset({'a'})

    def test_none_threshold_never_evicts(self) -> None:
        delta = reconcile({'a': 999}, [], eviction_threshold=None)
        assert delta.to_increment == frozenset({'a'})
        assert delta.to_evict == frozenset()

    def test_duplicate_listed_keys_collapse(self) -> None:
        delta = reconcile({}, ['a', 'a'], eviction_threshold=3)
        assert delta.to_zero == frozenset({'a'})


class TestIsRosterStale:
    def test_none_last_success_is_stale(self) -> None:
        assert is_roster_stale(None, NOW, timedelta(days=1)) is True

    def test_within_max_age_is_fresh(self) -> None:
        last = NOW - timedelta(hours=12)
        assert is_roster_stale(last, NOW, timedelta(days=1)) is False

    def test_beyond_max_age_is_stale(self) -> None:
        last = NOW - timedelta(days=2)
        assert is_roster_stale(last, NOW, timedelta(days=1)) is True
