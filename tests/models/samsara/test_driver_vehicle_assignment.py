"""Tests for fleetpull.models.samsara.driver_vehicle_assignment.

Every fixture is the committed 2026-07-20 capture set
(``tests/samsara_driver_vehicle_assignments_capture.py``): five fully
synthetic assignment records shaped by the total 216/216 census (every
key on every record across the full two-sweep 24-hour walk). The
census-preserved shapes (the string-shaped party ids, the LITERAL
DOTTED ``externalIds`` wire keys on the NESTED vehicle ref, both
observed ``assignmentType`` values, a passenger row, midnight
spanners) are asserted here beside the model that mirrors them;
requiredness carries drop-key rejection teeth at every level (the
asset_locations precedent) -- the structural core (driver, vehicle,
startTime, endTime, and the refs' ids) is required by structural
judgment, not whole-population census (module docstring of the model),
while everything else demotes to None per the conservative posture,
and only a loud rejection here keeps a future optional-demotion of the
core from passing every gate.
"""

from datetime import UTC, datetime
from enum import Enum

import pytest
from pydantic import ValidationError

from fleetpull.models.samsara import (
    AssignmentDriverRef,
    AssignmentVehicleExternalIds,
    AssignmentVehicleRef,
    DriverVehicleAssignment,
)
from fleetpull.vocabulary import JsonObject
from tests.samsara_driver_vehicle_assignments_capture import (
    DRIVER_VEHICLE_ASSIGNMENT_RECORDS,
)

# The record's full census key set -- 216/216 on EVERY key, so every
# fixture record carries all seven.
_CENSUS_KEYS = frozenset(
    {
        'driver',
        'vehicle',
        'startTime',
        'endTime',
        'assignedAtTime',
        'assignmentType',
        'isPassenger',
    }
)

# The structural core: required by structural judgment (an assignment
# without its parties or bounds is structurally meaningless), NOT by
# the one-day census -- the conservative posture covers the rest.
_REQUIRED_KEYS = frozenset({'driver', 'vehicle', 'startTime', 'endTime'})

# The conservative optionals: census-always-present, modeled optional
# because one day's walk is not a whole-population oath.
_OPTIONAL_KEYS = frozenset(_CENSUS_KEYS - _REQUIRED_KEYS)

# The dotted external-id wire keys, verbatim on the NESTED vehicle ref
# (the wire's own keys -- not the stats triple's decoder-synthesized
# flat ones).
_DOTTED_EXTERNAL_ID_KEYS = frozenset({'samsara.serial', 'samsara.vin'})


def _driver_block(record: JsonObject) -> JsonObject:
    driver = record['driver']
    assert isinstance(driver, dict)
    return driver


def _vehicle_block(record: JsonObject) -> JsonObject:
    vehicle = record['vehicle']
    assert isinstance(vehicle, dict)
    return vehicle


def _external_ids_block(record: JsonObject) -> JsonObject:
    external_ids = _vehicle_block(record)['externalIds']
    assert isinstance(external_ids, dict)
    return external_ids


class TestFixtureProperties:
    """The variant coverage the capture module promises."""

    def test_every_record_carries_the_full_census_key_set(self) -> None:
        # The census was TOTAL: every key on 216/216 records, so every
        # fixture record carries all seven -- no partial-presence key
        # exists on this surface.
        assert len(DRIVER_VEHICLE_ASSIGNMENT_RECORDS) == 5
        for record in DRIVER_VEHICLE_ASSIGNMENT_RECORDS:
            assert set(record) == _CENSUS_KEYS
            assert set(_driver_block(record)) == {'id', 'name'}
            assert set(_vehicle_block(record)) == {'id', 'name', 'externalIds'}

    def test_the_dotted_external_id_keys_are_wire_verbatim(self) -> None:
        for record in DRIVER_VEHICLE_ASSIGNMENT_RECORDS:
            external_ids = _external_ids_block(record)
            assert set(external_ids) == _DOTTED_EXTERNAL_ID_KEYS
            assert all(isinstance(value, str) for value in external_ids.values())

    def test_the_party_ids_are_strings(self) -> None:
        for record in DRIVER_VEHICLE_ASSIGNMENT_RECORDS:
            assert isinstance(_driver_block(record)['id'], str)
            assert isinstance(_vehicle_block(record)['id'], str)

    def test_both_assignment_type_values_appear(self) -> None:
        # The observed vocabulary, exactly: {'static': 158, 'HOS': 58}
        # in census -- census-closed only, NOT API-enforced on output.
        observed = {
            record['assignmentType'] for record in DRIVER_VEHICLE_ASSIGNMENT_RECORDS
        }
        assert observed == {'static', 'HOS'}

    def test_a_passenger_row_appears(self) -> None:
        passenger_flags = [
            record['isPassenger'] for record in DRIVER_VEHICLE_ASSIGNMENT_RECORDS
        ]
        assert True in passenger_flags
        assert False in passenger_flags

    def test_a_midnight_spanning_assignment_appears(self) -> None:
        # The overlap-anchoring evidence: the probe's adjacent day
        # windows shared 5 midnight spanners as identical tuples, so
        # the fixtures preserve the interval-crosses-midnight shape.
        spans_midnight = []
        for record in DRIVER_VEHICLE_ASSIGNMENT_RECORDS:
            start = record['startTime']
            end = record['endTime']
            assert isinstance(start, str)
            assert isinstance(end, str)
            spans_midnight.append(start[:10] != end[:10])
        assert True in spans_midnight
        assert False in spans_midnight


class TestDriverVehicleAssignmentValidation:
    @pytest.mark.parametrize('required_key', sorted(_REQUIRED_KEYS))
    def test_each_structural_core_key_rejects_absence(self, required_key: str) -> None:
        # The structural-judgment requiredness with teeth: an
        # assignment missing a party or a bound must fail loudly,
        # never land an all-null row.
        record = {
            key: value
            for key, value in DRIVER_VEHICLE_ASSIGNMENT_RECORDS[0].items()
            if key != required_key
        }
        with pytest.raises(ValidationError):
            DriverVehicleAssignment.model_validate(record)

    def test_a_driver_ref_without_id_rejects(self) -> None:
        record = dict(DRIVER_VEHICLE_ASSIGNMENT_RECORDS[0])
        record['driver'] = {'name': 'Synthetic Driver One'}
        with pytest.raises(ValidationError):
            DriverVehicleAssignment.model_validate(record)

    def test_a_vehicle_ref_without_id_rejects(self) -> None:
        record = dict(DRIVER_VEHICLE_ASSIGNMENT_RECORDS[0])
        record['vehicle'] = {'name': 'SYNTH-TRUCK-001'}
        with pytest.raises(ValidationError):
            DriverVehicleAssignment.model_validate(record)

    @pytest.mark.parametrize('optional_key', sorted(_OPTIONAL_KEYS))
    def test_each_conservative_optional_demotes_to_none(
        self, optional_key: str
    ) -> None:
        # 216/216 in a one-day census is not a whole-population oath:
        # dropping any non-core key validates and lands None.
        record = {
            key: value
            for key, value in DRIVER_VEHICLE_ASSIGNMENT_RECORDS[0].items()
            if key != optional_key
        }
        assignment = DriverVehicleAssignment.model_validate(record)
        field_name = {
            'assignedAtTime': 'assigned_at_time',
            'assignmentType': 'assignment_type',
            'isPassenger': 'is_passenger',
        }[optional_key]
        assert getattr(assignment, field_name) is None

    def test_ref_names_and_external_ids_are_optional(self) -> None:
        # The conservative posture inside the refs: only the ids are
        # structural; a bare-id party ref validates.
        record = dict(DRIVER_VEHICLE_ASSIGNMENT_RECORDS[0])
        record['driver'] = {'id': '51000001'}
        record['vehicle'] = {'id': '281474981110001'}
        assignment = DriverVehicleAssignment.model_validate(record)
        assert assignment.driver.name is None
        assert assignment.vehicle.name is None
        assert assignment.vehicle.external_ids is None

    def test_each_dotted_external_id_is_independently_optional(self) -> None:
        # The vehicles surface proved serial-only carriers exist in this
        # fleet; a single-key block must validate with the present key
        # landing on its dotted alias and the absent one None.
        record = dict(DRIVER_VEHICLE_ASSIGNMENT_RECORDS[0])
        record['vehicle'] = {
            'id': '281474981110001',
            'externalIds': {'samsara.serial': 'SYNTH-SER-001'},
        }
        assignment = DriverVehicleAssignment.model_validate(record)
        external_ids = assignment.vehicle.external_ids
        assert external_ids is not None
        assert external_ids.samsara_serial == 'SYNTH-SER-001'
        assert external_ids.samsara_vin is None

    def test_every_record_validates_with_aware_ordered_bounds(self) -> None:
        validated = [
            DriverVehicleAssignment.model_validate(record)
            for record in DRIVER_VEHICLE_ASSIGNMENT_RECORDS
        ]
        assert len(validated) == 5
        for assignment in validated:
            assert assignment.start_time.tzinfo is not None
            assert assignment.end_time.tzinfo is not None
            assert assignment.assigned_at_time == ''
            assert assignment.start_time < assignment.end_time
            assert isinstance(assignment.driver, AssignmentDriverRef)
            assert isinstance(assignment.vehicle, AssignmentVehicleRef)
            assert isinstance(
                assignment.vehicle.external_ids, AssignmentVehicleExternalIds
            )

    def test_the_first_record_pins_the_wire_values(self) -> None:
        assignment = DriverVehicleAssignment.model_validate(
            DRIVER_VEHICLE_ASSIGNMENT_RECORDS[0]
        )
        assert assignment.driver.id == '51000001'
        assert assignment.driver.name == 'Synthetic Driver One'
        assert assignment.vehicle.id == '281474981110001'
        assert assignment.vehicle.name == 'SYNTH-TRUCK-001'
        assert assignment.vehicle.external_ids is not None
        assert assignment.vehicle.external_ids.samsara_serial == 'SYNTH-SER-001'
        assert assignment.vehicle.external_ids.samsara_vin == 'SYNTHVIN000000001'
        assert assignment.start_time == datetime(2026, 1, 1, 22, 0, tzinfo=UTC)
        assert assignment.end_time == datetime(2026, 1, 2, 6, 0, tzinfo=UTC)
        assert assignment.assigned_at_time == ''
        assert assignment.assignment_type == 'static'
        assert assignment.is_passenger is False

    def test_assignment_type_is_a_plain_str_not_an_enum(self) -> None:
        # The vocabulary {'static', 'HOS'} is census-closed only, NOT
        # API-enforced on output (the eldExemptReason lesson): a novel
        # value must validate as a plain string, never crash an enum.
        record = dict(DRIVER_VEHICLE_ASSIGNMENT_RECORDS[0])
        record['assignmentType'] = 'someFutureType'
        assignment = DriverVehicleAssignment.model_validate(record)
        assert assignment.assignment_type == 'someFutureType'
        assert not isinstance(assignment.assignment_type, Enum)
