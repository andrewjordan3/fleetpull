"""Tests for fleetpull.models.geotab.fuel_and_energy_used.

Every fixture is the committed synthetic 2026-07-21 fixture set
(``tests/geotab_fuel_and_energy_used_capture.py``), shaped by the
whole-page census (2,000/2,000 records carried all seven keys, on the
estimates-only tenant), so every modeled key is required. Both observed
``confidence`` tokens and both mixed-numeric arms are exercised.
"""

import polars as pl
import pytest
from pydantic import ValidationError

from fleetpull.models.geotab import FuelAndEnergyUsed
from fleetpull.records import models_to_dataframe
from tests.geotab_fuel_and_energy_used_capture import (
    FUEL_AND_ENERGY_USED_FULL_RECORD,
    FUEL_AND_ENERGY_USED_RECORDS,
)

# The whole-page census observed every key on all 2,000 records, so
# every modeled wire key is required.
_REQUIRED_KEYS = frozenset(
    {
        'confidence',
        'dateTime',
        'device',
        'id',
        'totalFuelUsed',
        'totalIdlingFuelUsedL',
        'version',
    }
)


class TestFixtureProperties:
    """The variant coverage the fixture module promises."""

    def test_every_record_carries_the_required_keys(self) -> None:
        assert len(FUEL_AND_ENERGY_USED_RECORDS) == 3
        for record in FUEL_AND_ENERGY_USED_RECORDS:
            assert set(record) >= _REQUIRED_KEYS

    def test_both_observed_confidence_tokens_appear(self) -> None:
        tokens = {record['confidence'] for record in FUEL_AND_ENERGY_USED_RECORDS}
        assert tokens == {'None', 'FuelUsedInconsistent'}

    def test_both_numeric_arms_ride_the_fixtures(self) -> None:
        fuel_types = {
            type(record['totalFuelUsed']).__name__
            for record in FUEL_AND_ENERGY_USED_RECORDS
        }
        idling_types = {
            type(record['totalIdlingFuelUsedL']).__name__
            for record in FUEL_AND_ENERGY_USED_RECORDS
        }
        assert fuel_types == {'float', 'int'}
        assert idling_types == {'float', 'int'}


class TestFuelAndEnergyUsedValidation:
    @pytest.mark.parametrize('required_key', sorted(_REQUIRED_KEYS))
    def test_each_required_key_rejects_absence(self, required_key: str) -> None:
        record = {
            key: value
            for key, value in FUEL_AND_ENERGY_USED_FULL_RECORD.items()
            if key != required_key
        }
        with pytest.raises(ValidationError):
            FuelAndEnergyUsed.model_validate(record)

    def test_the_device_id_rejects_absence(self) -> None:
        with pytest.raises(ValidationError):
            FuelAndEnergyUsed.model_validate(
                {**FUEL_AND_ENERGY_USED_FULL_RECORD, 'device': {}}
            )

    def test_every_record_validates(self) -> None:
        totals = [
            FuelAndEnergyUsed.model_validate(record)
            for record in FUEL_AND_ENERGY_USED_RECORDS
        ]
        assert [total.id for total in totals] == ['b17d301', 'b17d302', 'b17d303']

    def test_full_record_populates_every_field(self) -> None:
        # The alias-trap closure, mechanically (the Trip pattern).
        full = FuelAndEnergyUsed.model_validate(FUEL_AND_ENERGY_USED_FULL_RECORD)
        for field_name in FuelAndEnergyUsed.model_fields:
            assert getattr(full, field_name) is not None, field_name
        assert full.confidence == 'None'
        assert full.total_fuel_used == 12.4

    def test_the_int_arms_land_as_floats(self) -> None:
        mixed = FuelAndEnergyUsed.model_validate(FUEL_AND_ENERGY_USED_RECORDS[1])
        assert mixed.total_fuel_used == 8.0
        assert isinstance(mixed.total_fuel_used, float)
        int_idling = FuelAndEnergyUsed.model_validate(FUEL_AND_ENERGY_USED_RECORDS[0])
        assert int_idling.total_idling_fuel_used_l == 1.0
        assert isinstance(int_idling.total_idling_fuel_used_l, float)

    def test_an_unobserved_confidence_token_validates(self) -> None:
        # The census-open vocabulary posture with teeth: confidence is a
        # plain str mirror ('None' on 1,994/2,000 is a census fact, not
        # a closed set), so an unobserved token must validate.
        total = FuelAndEnergyUsed.model_validate(
            {
                **FUEL_AND_ENERGY_USED_FULL_RECORD,
                'confidence': 'UnobservedFutureConfidence',
            }
        )
        assert total.confidence == 'UnobservedFutureConfidence'


class TestFuelAndEnergyUsedFrame:
    def test_all_records_build_one_typed_frame(self) -> None:
        frame = models_to_dataframe(
            [
                FuelAndEnergyUsed.model_validate(record)
                for record in FUEL_AND_ENERGY_USED_RECORDS
            ],
            FuelAndEnergyUsed,
        )
        assert frame.height == 3
        assert frame.schema['date_time'] == pl.Datetime(time_unit='us', time_zone='UTC')
        assert frame.schema['total_fuel_used'] == pl.Float64
        assert frame.schema['total_idling_fuel_used_l'] == pl.Float64
        assert frame.schema['device__id'] == pl.String
        assert frame['total_fuel_used'].to_list() == [12.4, 8.0, 21.75]

    def test_empty_input_carries_the_full_schema(self) -> None:
        populated = models_to_dataframe(
            [
                FuelAndEnergyUsed.model_validate(record)
                for record in FUEL_AND_ENERGY_USED_RECORDS
            ],
            FuelAndEnergyUsed,
        )
        empty = models_to_dataframe([], FuelAndEnergyUsed)
        assert empty.height == 0
        assert empty.schema == populated.schema
