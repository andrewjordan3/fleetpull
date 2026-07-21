"""Tests for fleetpull.models.motive.vehicle_utilization.

Every fixture is the committed 2026-07-21 capture set
(``tests/motive_vehicle_utilizations_capture.py``): three fully
synthetic window-stamped rollup records shaped by the 120-record
structurally uniform census. The census-preserved shapes (the float
metric core, the whole-fleet population's inactive zeroed rows with
their ``message`` status string, the shared ``VehicleSummary`` ref with
this surface's null-``vin`` arm, the verbatim-str ``last_located_at``)
are asserted here beside the model that mirrors them; requiredness
carries drop-key rejection teeth -- the window stamps and the vehicle
ref are required STRUCTURALLY, and the metric core plus ``message`` on
the rollup-surface posture (no absence arm exists; the model module
docstring states the judgment) -- only a loud rejection here keeps a
future optional-demotion from passing every gate. The window stamps are
DATE LABELS lifted to UTC midnight by ``MotiveWindowStamp`` -- company-
local day labels, never converted (the documented caveat).
"""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from fleetpull.models.motive import VehicleSummary, VehicleUtilization
from fleetpull.vocabulary import JsonObject
from tests.motive_vehicle_utilizations_capture import (
    VEHICLE_UTILIZATION_RECORDS,
)

# The stamped record's full key set: the structurally uniform wire
# census (ten keys) plus the two decoder-synthesized window stamps.
_STAMPED_KEYS = frozenset(
    {
        'windowStartDate',
        'windowEndDate',
        'vehicle',
        'driving_fuel',
        'driving_time',
        'idle_fuel',
        'idle_time',
        'total_distance',
        'total_fuel',
        'utilization_percentage',
        'last_located_at',
        'message',
    }
)

# Required with teeth: the stamps and the ref structurally, the metric
# core and `message` on the rollup-surface posture. `last_located_at`
# is the one nullable key and stays out of the drop-key sweep.
_REQUIRED_KEYS = _STAMPED_KEYS - {'last_located_at'}

# The vehicle ref's wire key set -- exactly the shared VehicleSummary
# shape, its third carrying surface.
_VEHICLE_REF_KEYS = frozenset(
    {'id', 'make', 'metric_units', 'model', 'number', 'vin', 'year'}
)


def _vehicle_block(record: JsonObject) -> JsonObject:
    vehicle = record['vehicle']
    assert isinstance(vehicle, dict)
    return vehicle


class TestFixtureProperties:
    """The variant coverage the capture module promises."""

    def test_every_record_carries_the_full_stamped_key_set(self) -> None:
        # The census was structurally uniform (every key on every
        # sampled record) and the decoder stamps every row, so every
        # fixture record carries all twelve keys, and every vehicle ref
        # the exact shared 7-key shape.
        assert len(VEHICLE_UTILIZATION_RECORDS) == 3
        for record in VEHICLE_UTILIZATION_RECORDS:
            assert set(record) == _STAMPED_KEYS
            assert set(_vehicle_block(record)) == _VEHICLE_REF_KEYS

    def test_the_wire_shape_carries_no_row_time_identity(self) -> None:
        # The probe's central fact: rollup rows carry NO date or time
        # identity of any kind. The `*_time` keys are duration metrics
        # and `last_located_at` is a vehicle attribute, not the row's
        # time -- the row's only time-identity keys are the decoder's
        # stamps.
        for record in VEHICLE_UTILIZATION_RECORDS:
            wire_keys = set(record) - {'windowStartDate', 'windowEndDate'}
            assert not any('date' in key.lower() for key in wire_keys)
            timestamp_shaped = {key for key in wire_keys if key.endswith('_at')}
            assert timestamp_shaped == {'last_located_at'}

    def test_an_inactive_zeroed_vehicle_with_its_message_appears(self) -> None:
        # The whole-fleet population shape: inactive vehicles ride in
        # every window with zeroed metrics, a populated message, and a
        # null last_located_at.
        inactive = VEHICLE_UTILIZATION_RECORDS[1]
        assert inactive['total_distance'] == 0.0
        assert inactive['utilization_percentage'] == 0.0
        message = inactive['message']
        assert isinstance(message, str)
        assert message
        assert inactive['last_located_at'] is None

    def test_a_null_vin_vehicle_appears(self) -> None:
        assert _vehicle_block(VEHICLE_UTILIZATION_RECORDS[2])['vin'] is None


class TestVehicleUtilizationValidation:
    @pytest.mark.parametrize('required_key', sorted(_REQUIRED_KEYS))
    def test_each_required_key_rejects_absence(self, required_key: str) -> None:
        # Requiredness with teeth: the window stamps and the ref
        # structurally, the metric core and message per the
        # rollup-surface posture -- a record missing any must fail
        # loudly, never land nulls.
        record = {
            key: value
            for key, value in VEHICLE_UTILIZATION_RECORDS[0].items()
            if key != required_key
        }
        with pytest.raises(ValidationError):
            VehicleUtilization.model_validate(record)

    def test_every_record_validates_with_aware_window_stamps(self) -> None:
        validated = [
            VehicleUtilization.model_validate(record)
            for record in VEHICLE_UTILIZATION_RECORDS
        ]
        assert len(validated) == 3
        for utilization in validated:
            assert utilization.window_start.tzinfo is not None
            assert utilization.window_end.tzinfo is not None
            assert isinstance(utilization.vehicle, VehicleSummary)

    def test_the_date_labels_lift_to_their_utc_midnight_instants(self) -> None:
        # The stamps are the sent window's INCLUSIVE date labels
        # ('2026-01-05' both, at the fixed 1-day unit), lifted to UTC
        # midnight -- the label's day preserved exactly, never a
        # timezone conversion (the company-local caveat rides the
        # docstrings).
        utilization = VehicleUtilization.model_validate(VEHICLE_UTILIZATION_RECORDS[0])
        assert utilization.window_start == datetime(2026, 1, 5, tzinfo=UTC)
        assert utilization.window_end == datetime(2026, 1, 5, tzinfo=UTC)
        assert utilization.window_start == utilization.window_end

    def test_the_first_record_pins_the_wire_values(self) -> None:
        utilization = VehicleUtilization.model_validate(VEHICLE_UTILIZATION_RECORDS[0])
        assert utilization.vehicle.vehicle_id == 9900101
        assert utilization.vehicle.number == 'TRK-0101'
        assert utilization.vehicle.make == 'Kenworth'
        assert utilization.vehicle.model == 'T680'
        assert utilization.vehicle.vin == '4SYNTHV1N00000101'
        assert utilization.vehicle.year == 2020
        assert utilization.vehicle.metric_units is False
        assert utilization.driving_fuel == 42.7
        assert utilization.driving_time == 21540.0
        assert utilization.idle_fuel == 3.2
        assert utilization.idle_time == 1860.0
        assert utilization.total_distance == 512.6
        assert utilization.total_fuel == 45.9
        assert utilization.utilization_percentage == 87.3
        assert utilization.last_located_at == '2026-01-05T16:42:11-05:00'
        assert utilization.message == ''

    def test_the_durations_are_floats_on_this_arm(self) -> None:
        # Floats on the vehicle arm, bare ints on the driver arm --
        # each arm mirrors its own wire.
        utilization = VehicleUtilization.model_validate(VEHICLE_UTILIZATION_RECORDS[0])
        assert isinstance(utilization.driving_time, float)
        assert isinstance(utilization.idle_time, float)

    def test_a_null_vin_lands_none_through_the_shared_ref(self) -> None:
        # This surface's vin null arm on the shared VehicleSummary (the
        # union-lax widening, captured 2026-07-21).
        utilization = VehicleUtilization.model_validate(VEHICLE_UTILIZATION_RECORDS[2])
        assert utilization.vehicle.vin is None
        assert utilization.vehicle.vehicle_id == 9900103

    def test_last_located_at_mirrors_verbatim_as_a_string(self) -> None:
        # The value format and zone semantics are unprobed and the
        # provider documents company-local rollup timestamps, so the
        # model deliberately mirrors the raw string -- any string
        # validates, nothing is parsed.
        record = dict(VEHICLE_UTILIZATION_RECORDS[0])
        record['last_located_at'] = 'whatever the provider sends'
        utilization = VehicleUtilization.model_validate(record)
        assert utilization.last_located_at == 'whatever the provider sends'

    def test_message_is_free_text_plain_str(self) -> None:
        # Free text -- no vocabulary claim: a novel message validates as
        # a plain string.
        record = dict(VEHICLE_UTILIZATION_RECORDS[0])
        record['message'] = 'Some new provider notice'
        utilization = VehicleUtilization.model_validate(record)
        assert utilization.message == 'Some new provider notice'

    def test_a_non_label_window_stamp_rejects(self) -> None:
        # The stamp lift is strict: the builder only renders date
        # labels, so an RFC3339 datetime string arriving as a stamp is
        # a wiring drift that must fail loudly, not pass mangled.
        record = dict(VEHICLE_UTILIZATION_RECORDS[0])
        record['windowStartDate'] = '2026-01-05T00:00:00Z'
        with pytest.raises(ValidationError):
            VehicleUtilization.model_validate(record)
