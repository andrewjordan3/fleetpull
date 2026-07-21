"""Tests for fleetpull.models.samsara.driver_fuel_energy_report.

Every fixture is the committed 2026-07-20/21 capture set
(``tests/samsara_driver_fuel_energy_reports_capture.py``): three fully
synthetic window-stamped report records shaped by the 47/47 total
census. The driver arm is the vehicle arm's metric core attributed to
a ``driver {id, name}`` ref -- and NO ``externalIds`` anywhere on this
arm (never observed; the fixtures preserve the absence). Requiredness
carries drop-key rejection teeth -- the window stamps and the driver
ref (and its id) are required STRUCTURALLY, the metric core on the
WHOLE-WALK posture (the model module docstring states the judgment) --
so a future optional-demotion cannot pass every gate silently.
"""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from fleetpull.models.samsara import (
    DriverFuelEnergyCost,
    DriverFuelEnergyDriverRef,
    DriverFuelEnergyReport,
)
from fleetpull.vocabulary import JsonObject
from tests.samsara_driver_fuel_energy_reports_capture import (
    DRIVER_FUEL_ENERGY_REPORT_RECORDS,
)

# The stamped record's full key set: the 47/47 wire census (nine keys,
# the metric core shared with the vehicle arm plus the driver ref)
# plus the two decoder-synthesized window stamps.
_STAMPED_KEYS = frozenset(
    {
        'windowStartDate',
        'windowEndDate',
        'driver',
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
# structurally, the metric core on the whole-walk posture.
_REQUIRED_KEYS = _STAMPED_KEYS


def _driver_block(record: JsonObject) -> JsonObject:
    driver = record['driver']
    assert isinstance(driver, dict)
    return driver


class TestFixtureProperties:
    """The variant coverage the capture module promises."""

    def test_every_record_carries_the_full_stamped_key_set(self) -> None:
        assert len(DRIVER_FUEL_ENERGY_REPORT_RECORDS) == 3
        for record in DRIVER_FUEL_ENERGY_REPORT_RECORDS:
            assert set(record) == _STAMPED_KEYS
            assert set(_driver_block(record)) == {'id', 'name'}

    def test_no_external_ids_appears_anywhere_on_the_driver_arm(self) -> None:
        # Never observed across the whole 47/47 census -- the driver
        # ref carries exactly {id, name}, and no record nests an
        # externalIds block at any level.
        for record in DRIVER_FUEL_ENERGY_REPORT_RECORDS:
            assert 'externalIds' not in record
            assert 'externalIds' not in _driver_block(record)

    def test_no_event_time_key_exists_on_the_wire_shape(self) -> None:
        # Report rows carry NO event-time key of any kind ('Time'
        # appears only inside duration metric names, e.g.
        # engineRunTimeDurationMs) -- the only timestamp-shaped keys
        # are the decoder's stamps.
        for record in DRIVER_FUEL_ENERGY_REPORT_RECORDS:
            wire_keys = set(record) - {'windowStartDate', 'windowEndDate'}
            assert not any(
                key.endswith(('Time', 'Date', 'At', 'Ts')) for key in wire_keys
            )

    def test_an_int_valued_efficiency_mpge_row_appears(self) -> None:
        # The wire's mixed int|float shape, exercised by fixture.
        shapes = {
            type(record['efficiencyMpge'])
            for record in DRIVER_FUEL_ENERGY_REPORT_RECORDS
        }
        assert shapes == {int, float}


class TestDriverFuelEnergyReportValidation:
    @pytest.mark.parametrize('required_key', sorted(_REQUIRED_KEYS))
    def test_each_required_key_rejects_absence(self, required_key: str) -> None:
        # Requiredness with teeth: the window stamps and the ref
        # structurally, the metric core per the whole-walk posture.
        record = {
            key: value
            for key, value in DRIVER_FUEL_ENERGY_REPORT_RECORDS[0].items()
            if key != required_key
        }
        with pytest.raises(ValidationError):
            DriverFuelEnergyReport.model_validate(record)

    def test_a_driver_ref_without_id_rejects(self) -> None:
        record = dict(DRIVER_FUEL_ENERGY_REPORT_RECORDS[0])
        record['driver'] = {'name': 'Synthetic Driver One'}
        with pytest.raises(ValidationError):
            DriverFuelEnergyReport.model_validate(record)

    def test_the_ref_name_is_optional(self) -> None:
        # The conservative posture inside the ref: only the id is
        # structural; a bare-id driver ref validates.
        record = dict(DRIVER_FUEL_ENERGY_REPORT_RECORDS[0])
        record['driver'] = {'id': '51000001'}
        report = DriverFuelEnergyReport.model_validate(record)
        assert report.driver.id == '51000001'
        assert report.driver.name is None

    def test_every_record_validates_with_aware_window_bounds(self) -> None:
        validated = [
            DriverFuelEnergyReport.model_validate(record)
            for record in DRIVER_FUEL_ENERGY_REPORT_RECORDS
        ]
        assert len(validated) == 3
        for report in validated:
            assert report.window_start.tzinfo is not None
            assert report.window_end.tzinfo is not None
            assert report.window_start < report.window_end
            assert isinstance(report.driver, DriverFuelEnergyDriverRef)
            assert isinstance(report.est_fuel_energy_cost, DriverFuelEnergyCost)

    def test_an_int_valued_efficiency_mpge_coerces_to_float(self) -> None:
        # The mixed int|float wire shape lifted by lax coercion.
        report = DriverFuelEnergyReport.model_validate(
            DRIVER_FUEL_ENERGY_REPORT_RECORDS[1]
        )
        assert isinstance(report.efficiency_mpge, float)
        assert report.efficiency_mpge == 9.0
        assert isinstance(report.est_carbon_emissions_kg, float)
        assert report.est_carbon_emissions_kg == 15.0
        assert isinstance(report.est_fuel_energy_cost.amount, float)
        assert report.est_fuel_energy_cost.amount == 20.0

    def test_the_first_record_pins_the_wire_values(self) -> None:
        report = DriverFuelEnergyReport.model_validate(
            DRIVER_FUEL_ENERGY_REPORT_RECORDS[0]
        )
        assert report.window_start == datetime(2026, 1, 2, tzinfo=UTC)
        assert report.window_end == datetime(2026, 1, 3, tzinfo=UTC)
        assert report.driver.id == '51000001'
        assert report.driver.name == 'Synthetic Driver One'
        assert report.distance_traveled_meters == 388404
        assert report.efficiency_mpge == 6.85
        assert report.energy_used_kwh == 0
        assert report.engine_idle_time_duration_ms == 2280000
        assert report.engine_run_time_duration_ms == 18660000
        assert report.est_carbon_emissions_kg == 131.9
        assert report.est_fuel_energy_cost.amount == 186.9
        assert report.est_fuel_energy_cost.currency_code == 'USD'
        assert report.fuel_consumed_ml == 146540
