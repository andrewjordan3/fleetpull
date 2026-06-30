"""Tests for fleetpull.roster.key."""

import dataclasses

import pytest

from fleetpull.roster.key import RosterKey
from fleetpull.vocabulary import Provider


class TestRosterKey:
    def test_holds_provider_and_name(self) -> None:
        key = RosterKey(provider=Provider.MOTIVE, name='vehicle_ids')
        assert key.provider is Provider.MOTIVE
        assert key.name == 'vehicle_ids'

    def test_equality_is_provider_and_name(self) -> None:
        assert RosterKey(Provider.MOTIVE, 'vehicle_ids') == RosterKey(
            Provider.MOTIVE, 'vehicle_ids'
        )
        assert RosterKey(Provider.MOTIVE, 'vehicle_ids') != RosterKey(
            Provider.SAMSARA, 'vehicle_ids'
        )
        assert RosterKey(Provider.MOTIVE, 'vehicle_ids') != RosterKey(
            Provider.MOTIVE, 'driver_ids'
        )

    def test_is_hashable(self) -> None:
        assert {RosterKey(Provider.MOTIVE, 'vehicle_ids')} == {
            RosterKey(Provider.MOTIVE, 'vehicle_ids')
        }

    def test_is_frozen(self) -> None:
        key = RosterKey(Provider.MOTIVE, 'vehicle_ids')
        with pytest.raises(dataclasses.FrozenInstanceError):
            key.name = 'other'  # type: ignore[misc]
