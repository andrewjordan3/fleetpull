"""Tests for fleetpull.endpoints.shared.fan_out."""

import dataclasses

import pytest

from fleetpull.endpoints.shared.fan_out import FanOutBinding
from fleetpull.roster import RosterKey
from fleetpull.vocabulary import Provider


def _binding() -> FanOutBinding:
    return FanOutBinding(
        roster=RosterKey(Provider.MOTIVE, 'vehicle_ids'),
        path_placeholder='vehicle_id',
    )


class TestFanOutBinding:
    def test_holds_roster_and_placeholder(self) -> None:
        binding = _binding()
        assert binding.roster == RosterKey(Provider.MOTIVE, 'vehicle_ids')
        assert binding.path_placeholder == 'vehicle_id'

    def test_is_frozen(self) -> None:
        with pytest.raises(dataclasses.FrozenInstanceError):
            _binding().path_placeholder = 'other'  # type: ignore[misc]
