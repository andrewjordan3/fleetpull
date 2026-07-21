"""Tests for fleetpull.state.rosters."""

from pathlib import Path

import pytest

from fleetpull.roster import RosterKey
from fleetpull.state.database import StateDatabase
from fleetpull.state.migrations import migrate_to_head
from fleetpull.state.reconcile import RosterDelta
from fleetpull.state.rosters import RosterStore
from fleetpull.vocabulary import Provider


def _zero_delta(*members: str) -> RosterDelta:
    """A delta that only upserts ``members`` at absence-count zero (the seed case)."""
    return RosterDelta(
        to_zero=frozenset(members),
        to_increment=frozenset(),
        to_evict=frozenset(),
    )


@pytest.fixture
def roster_store(database_path: Path) -> RosterStore:
    """A RosterStore over a freshly initialized, migrated state database."""
    database = StateDatabase(database_path)
    database.initialize()
    migrate_to_head(database)
    return RosterStore(database)


KEY = RosterKey(Provider.MOTIVE, 'vehicle_ids')


class TestRosterStore:
    def test_apply_reflects_zeros_increments_and_evicts(
        self, roster_store: RosterStore
    ) -> None:
        roster_store.apply(KEY, _zero_delta('a', 'b', 'c'))
        roster_store.apply(
            KEY,
            RosterDelta(
                to_zero=frozenset({'d'}),
                to_increment=frozenset({'a', 'b'}),
                to_evict=frozenset({'c'}),
            ),
        )
        assert roster_store.read_counts(KEY) == {'a': 1, 'b': 1, 'd': 0}

    def test_to_zero_resets_an_incremented_count(
        self, roster_store: RosterStore
    ) -> None:
        roster_store.apply(KEY, _zero_delta('a'))
        roster_store.apply(
            KEY,
            RosterDelta(
                to_zero=frozenset(),
                to_increment=frozenset({'a'}),
                to_evict=frozenset(),
            ),
        )
        assert roster_store.read_counts(KEY) == {'a': 1}
        roster_store.apply(KEY, _zero_delta('a'))
        assert roster_store.read_counts(KEY) == {'a': 0}

    def test_read_members_returns_live_members_ascending(
        self, roster_store: RosterStore
    ) -> None:
        roster_store.apply(KEY, _zero_delta('c', 'a', 'b'))
        assert roster_store.read_members(KEY) == ['a', 'b', 'c']

    def test_reads_are_scoped_by_roster_key(self, roster_store: RosterStore) -> None:
        roster_store.apply(KEY, _zero_delta('a'))
        roster_store.apply(RosterKey(Provider.MOTIVE, 'driver_ids'), _zero_delta('b'))
        roster_store.apply(RosterKey(Provider.SAMSARA, 'vehicle_ids'), _zero_delta('c'))
        assert roster_store.read_members(KEY) == ['a']
        assert roster_store.read_counts(KEY) == {'a': 0}

    def test_empty_delta_is_a_no_op(self, roster_store: RosterStore) -> None:
        roster_store.apply(
            KEY,
            RosterDelta(
                to_zero=frozenset(),
                to_increment=frozenset(),
                to_evict=frozenset(),
            ),
        )
        assert roster_store.read_counts(KEY) == {}
