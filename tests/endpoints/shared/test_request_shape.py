"""Tests for fleetpull.endpoints.shared.request_shape."""

import dataclasses
from datetime import timedelta

import pytest

from fleetpull.endpoints.shared.request_shape import (
    BisectedWindowFetch,
    ParamSweep,
    RosterFanOut,
    SingleFetch,
)
from fleetpull.roster import RosterKey
from fleetpull.vocabulary import Provider


class TestSingleFetch:
    def test_is_an_equal_slotted_marker(self) -> None:
        shape = SingleFetch()
        assert not hasattr(shape, '__dict__')
        assert shape == SingleFetch()


class TestRosterFanOut:
    def test_holds_roster_and_member_key(self) -> None:
        shape = RosterFanOut(
            roster=RosterKey(Provider.MOTIVE, 'vehicle_ids'),
            member_key='vehicle_id',
        )
        assert shape.roster == RosterKey(Provider.MOTIVE, 'vehicle_ids')
        assert shape.member_key == 'vehicle_id'

    def test_is_frozen(self) -> None:
        shape = RosterFanOut(
            roster=RosterKey(Provider.MOTIVE, 'vehicle_ids'),
            member_key='vehicle_id',
        )
        with pytest.raises(dataclasses.FrozenInstanceError):
            shape.member_key = 'other'  # type: ignore[misc]


class TestBisectedWindowFetch:
    def test_holds_the_declared_facts(self) -> None:
        shape = BisectedWindowFetch(
            results_limit=5000,
            floor=timedelta(minutes=1),
            event_time_wire_key='activeFrom',
        )
        assert shape.results_limit == 5000
        assert shape.floor == timedelta(minutes=1)
        assert shape.event_time_wire_key == 'activeFrom'

    def test_is_frozen(self) -> None:
        shape = BisectedWindowFetch(
            results_limit=5000,
            floor=timedelta(minutes=1),
            event_time_wire_key='activeFrom',
        )
        with pytest.raises(dataclasses.FrozenInstanceError):
            shape.results_limit = 1  # type: ignore[misc]


class TestParamSweep:
    def test_holds_param_and_ordered_values(self) -> None:
        sweep = ParamSweep(
            param='driverActivationStatus', values=('active', 'deactivated')
        )
        assert sweep.param == 'driverActivationStatus'
        assert sweep.values == ('active', 'deactivated')

    def test_is_frozen(self) -> None:
        sweep = ParamSweep(param='status', values=('active',))
        with pytest.raises(dataclasses.FrozenInstanceError):
            sweep.param = 'other'  # type: ignore[misc]

    def test_empty_values_raise(self) -> None:
        # A sweep over nothing would silently emit an empty dataset -- a
        # wiring bug, rejected at construction.
        with pytest.raises(ValueError, match='must not be empty'):
            ParamSweep(param='status', values=())

    def test_duplicate_values_raise(self) -> None:
        # The same partition fetched twice is a declaration typo, not a
        # wider sweep.
        with pytest.raises(ValueError, match='duplicate'):
            ParamSweep(param='status', values=('active', 'active'))
