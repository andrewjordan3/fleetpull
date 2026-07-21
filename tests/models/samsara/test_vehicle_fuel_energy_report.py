"""Tests for fleetpull.models.samsara.vehicle_fuel_energy_report.

Every fixture is the committed 2026-07-20/21 capture set
(``tests/samsara_vehicle_fuel_energy_reports_capture.py``): four fully
synthetic window-stamped report records shaped by the 71/71 total
census. The census-preserved shapes (the mixed int|float
``efficiencyMpge``/``estCarbonEmissionsKg``/cost ``amount`` modeled
float, the census-open ``energyType``/``currencyCode`` staying plain
strs, the LITERAL DOTTED ``externalIds`` wire keys on the NESTED
vehicle ref) are asserted here beside the model that mirrors them;
requiredness carries drop-key rejection teeth at every level -- the
window stamps and the vehicle ref (and its id) are required
STRUCTURALLY (a rollup row without its window or its entity is
meaningless), and the metric core is required on the WHOLE-WALK
posture (71/71 total census; the model module docstring states the
judgment) -- only a loud rejection here keeps a future
optional-demotion from passing every gate.
"""

from datetime import UTC, datetime
from enum import Enum

import pytest
from pydantic import ValidationError

from fleetpull.models.samsara import (
    VehicleFuelEnergyCost,
    VehicleFuelEnergyExternalIds,
    VehicleFuelEnergyReport,
    VehicleFuelEnergyVehicleRef,
)
from fleetpull.vocabulary import JsonObject
from tests.samsara_vehicle_fuel_energy_reports_capture import (
    VEHICLE_FUEL_ENERGY_REPORT_RECORDS,
)

# The stamped record's full key set: the 71/71 wire census (nine keys)
# plus the two decoder-synthesized window stamps.
_STAMPED_KEYS = frozenset(
    {
        'windowStartDate',
        'windowEndDate',
        'vehicle',
        'distanceTraveledMeters',
        'efficiencyMpge',
        'energyUsedKwh',
        'engineIdleTimeDurationMs',
        'engineRunTimeDurationMs',
        'estCarbonEmissionsKg',
        'estFuelEnergyCost',
        'fuelConsumedMl',
    }
)

# EVERY key is required on this model: the window stamps and the ref
# structurally, the metric core on the whole-walk posture (module
# docstring of the model states the judgment).
_REQUIRED_KEYS = _STAMPED_KEYS

# The dotted external-id wire keys, verbatim on the NESTED vehicle ref.
_DOTTED_EXTERNAL_ID_KEYS = frozenset({'samsara.serial', 'samsara.vin'})


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

    def test_every_record_carries_the_full_stamped_key_set(self) -> None:
        # The wire census was TOTAL (71/71 on every key) and the
        # decoder stamps every row, so every fixture record carries all
        # eleven keys.
        assert len(VEHICLE_FUEL_ENERGY_REPORT_RECORDS) == 4
        for record in VEHICLE_FUEL_ENERGY_REPORT_RECORDS:
            assert set(record) == _STAMPED_KEYS
            assert set(_vehicle_block(record)) == {
                'id',
                'name',
                'energyType',
                'externalIds',
            }

    def test_no_event_time_key_exists_on_the_wire_shape(self) -> None:
        # The probe's central fact: report rows carry NO event-time key
        # of any kind ('Time' appears only inside duration metric
        # names, e.g. engineRunTimeDurationMs) -- the only
        # timestamp-shaped keys are the decoder's stamps.
        for record in VEHICLE_FUEL_ENERGY_REPORT_RECORDS:
            wire_keys = set(record) - {'windowStartDate', 'windowEndDate'}
            assert not any(
                key.endswith(('Time', 'Date', 'At', 'Ts')) for key in wire_keys
            )

    def test_the_dotted_external_id_keys_are_wire_verbatim(self) -> None:
        for record in VEHICLE_FUEL_ENERGY_REPORT_RECORDS:
            external_ids = _external_ids_block(record)
            assert set(external_ids) == _DOTTED_EXTERNAL_ID_KEYS
            assert all(isinstance(value, str) for value in external_ids.values())

    def test_an_int_valued_efficiency_mpge_row_appears(self) -> None:
        # The wire's mixed int|float shape: at least one row carries a
        # bare-int efficiencyMpge (and one a float), so the model's
        # float coercion is exercised by fixture, not by accident.
        shapes = {
            type(record['efficiencyMpge'])
            for record in VEHICLE_FUEL_ENERGY_REPORT_RECORDS
        }
        assert shapes == {int, float}

    def test_energy_type_is_the_observed_fuel_value(self) -> None:
        # 'fuel' is the ONLY observed value on a 100-report sample --
        # census-open, so the model keeps a plain str.
        for record in VEHICLE_FUEL_ENERGY_REPORT_RECORDS:
            assert _vehicle_block(record)['energyType'] == 'fuel'


class TestVehicleFuelEnergyReportValidation:
    @pytest.mark.parametrize('required_key', sorted(_REQUIRED_KEYS))
    def test_each_required_key_rejects_absence(self, required_key: str) -> None:
        # Requiredness with teeth: the window stamps and the ref
        # structurally, the metric core per the whole-walk posture --
        # a record missing any must fail loudly, never land nulls.
        record = {
            key: value
            for key, value in VEHICLE_FUEL_ENERGY_REPORT_RECORDS[0].items()
            if key != required_key
        }
        with pytest.raises(ValidationError):
            VehicleFuelEnergyReport.model_validate(record)

    def test_a_vehicle_ref_without_id_rejects(self) -> None:
        record = dict(VEHICLE_FUEL_ENERGY_REPORT_RECORDS[0])
        record['vehicle'] = {'name': 'SYNTH-TRUCK-001'}
        with pytest.raises(ValidationError):
            VehicleFuelEnergyReport.model_validate(record)

    @pytest.mark.parametrize('cost_key', ['amount', 'currencyCode'])
    def test_a_cost_block_missing_either_key_rejects(self, cost_key: str) -> None:
        record = dict(VEHICLE_FUEL_ENERGY_REPORT_RECORDS[0])
        cost_block = record['estFuelEnergyCost']
        assert isinstance(cost_block, dict)
        record['estFuelEnergyCost'] = {
            key: value for key, value in cost_block.items() if key != cost_key
        }
        with pytest.raises(ValidationError):
            VehicleFuelEnergyReport.model_validate(record)

    def test_ref_name_energy_type_and_external_ids_are_optional(self) -> None:
        # The conservative posture inside the ref: only the id is
        # structural; a bare-id vehicle ref validates.
        record = dict(VEHICLE_FUEL_ENERGY_REPORT_RECORDS[0])
        record['vehicle'] = {'id': '281474981110001'}
        report = VehicleFuelEnergyReport.model_validate(record)
        assert report.vehicle.name is None
        assert report.vehicle.energy_type is None
        assert report.vehicle.external_ids is None

    def test_each_dotted_external_id_is_independently_optional(self) -> None:
        # The vehicles surface proves serial-only carriers exist in
        # this fleet; a single-key block must validate with the present
        # key landing on its dotted alias and the absent one None.
        record = dict(VEHICLE_FUEL_ENERGY_REPORT_RECORDS[0])
        record['vehicle'] = {
            'id': '281474981110001',
            'externalIds': {'samsara.serial': 'SYNTH-SER-001'},
        }
        report = VehicleFuelEnergyReport.model_validate(record)
        external_ids = report.vehicle.external_ids
        assert external_ids is not None
        assert external_ids.samsara_serial == 'SYNTH-SER-001'
        assert external_ids.samsara_vin is None

    def test_every_record_validates_with_aware_window_bounds(self) -> None:
        validated = [
            VehicleFuelEnergyReport.model_validate(record)
            for record in VEHICLE_FUEL_ENERGY_REPORT_RECORDS
        ]
        assert len(validated) == 4
        for report in validated:
            assert report.window_start.tzinfo is not None
            assert report.window_end.tzinfo is not None
            assert report.window_start < report.window_end
            assert isinstance(report.vehicle, VehicleFuelEnergyVehicleRef)
            assert isinstance(report.vehicle.external_ids, VehicleFuelEnergyExternalIds)
            assert isinstance(report.est_fuel_energy_cost, VehicleFuelEnergyCost)

    def test_an_int_valued_efficiency_mpge_coerces_to_float(self) -> None:
        # The mixed int|float wire shape lifted by lax coercion: the
        # int-shaped fixture row lands as a genuine float, alongside
        # its int cost amount.
        report = VehicleFuelEnergyReport.model_validate(
            VEHICLE_FUEL_ENERGY_REPORT_RECORDS[1]
        )
        assert isinstance(report.efficiency_mpge, float)
        assert report.efficiency_mpge == 8.0
        assert isinstance(report.est_carbon_emissions_kg, float)
        assert report.est_carbon_emissions_kg == 31.0
        assert isinstance(report.est_fuel_energy_cost.amount, float)
        assert report.est_fuel_energy_cost.amount == 42.0

    def test_the_first_record_pins_the_wire_values(self) -> None:
        report = VehicleFuelEnergyReport.model_validate(
            VEHICLE_FUEL_ENERGY_REPORT_RECORDS[0]
        )
        assert report.window_start == datetime(2026, 1, 2, tzinfo=UTC)
        assert report.window_end == datetime(2026, 1, 3, tzinfo=UTC)
        assert report.vehicle.id == '281474981110001'
        assert report.vehicle.name == 'SYNTH-TRUCK-001'
        assert report.vehicle.energy_type == 'fuel'
        assert report.vehicle.external_ids is not None
        assert report.vehicle.external_ids.samsara_serial == 'SYNTH-SER-001'
        assert report.vehicle.external_ids.samsara_vin == 'SYNTHVIN000000001'
        assert report.distance_traveled_meters == 482301
        assert report.efficiency_mpge == 7.42
        assert report.energy_used_kwh == 0
        assert report.engine_idle_time_duration_ms == 1860000
        assert report.engine_run_time_duration_ms == 21540000
        assert report.est_carbon_emissions_kg == 152.7
        assert report.est_fuel_energy_cost.amount == 214.53
        assert report.est_fuel_energy_cost.currency_code == 'USD'
        assert report.fuel_consumed_ml == 168220

    def test_energy_type_and_currency_code_are_plain_strs_not_enums(self) -> None:
        # 'fuel' and 'USD' are census-open (100-report samples, never
        # API-enforced on output): a novel value must validate as a
        # plain string, never crash an enum.
        record = dict(VEHICLE_FUEL_ENERGY_REPORT_RECORDS[0])
        record['vehicle'] = {'id': 'v-1', 'energyType': 'electric'}
        record['estFuelEnergyCost'] = {'amount': 1.5, 'currencyCode': 'CAD'}
        report = VehicleFuelEnergyReport.model_validate(record)
        assert report.vehicle.energy_type == 'electric'
        assert report.est_fuel_energy_cost.currency_code == 'CAD'
        assert not isinstance(report.vehicle.energy_type, Enum)
        assert not isinstance(report.est_fuel_energy_cost.currency_code, Enum)
