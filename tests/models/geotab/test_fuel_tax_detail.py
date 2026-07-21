"""Tests for fleetpull.models.geotab.fuel_tax_detail.

Every fixture is the committed synthetic 2026-07-21 fixture set
(``tests/geotab_fuel_tax_details_capture.py``), shaped by the
whole-page census (every key on all sampled records, on the
estimates-only tenant), so every modeled key is required. The observed
arms are all exercised: both driver shapes, populated AND empty hourly
arrays, the mixed odometer arms, and the list-shaped ``versions``
identity.
"""

import polars as pl
import pytest
from pydantic import ValidationError

from fleetpull.models.geotab import FuelTaxDetail, FuelTaxDetailDriverRef
from fleetpull.records import models_to_dataframe
from tests.geotab_fuel_tax_details_capture import (
    FUEL_TAX_DETAIL_EMPTY_HOURLY_RECORD,
    FUEL_TAX_DETAIL_FULL_RECORD,
    FUEL_TAX_DETAIL_RECORDS,
)

# The whole-page census observed every key on all sampled records, so
# every modeled wire key is required.
_REQUIRED_KEYS = frozenset(
    {
        'authority',
        'device',
        'driver',
        'enterGpsOdometer',
        'enterLatitude',
        'enterLongitude',
        'enterOdometer',
        'enterTime',
        'exitGpsOdometer',
        'exitLatitude',
        'exitLongitude',
        'exitOdometer',
        'exitTime',
        'hasHourlyData',
        'hourlyGpsOdometer',
        'hourlyIsOdometerInterpolated',
        'hourlyLatitude',
        'hourlyLongitude',
        'hourlyOdometer',
        'id',
        'isClusterOdometer',
        'isEnterOdometerInterpolated',
        'isExitOdometerInterpolated',
        'isNegligible',
        'jurisdiction',
        'versions',
    }
)

_HOURLY_KEYS = (
    'hourlyGpsOdometer',
    'hourlyIsOdometerInterpolated',
    'hourlyLatitude',
    'hourlyLongitude',
    'hourlyOdometer',
)


class TestFixtureProperties:
    """The variant coverage the fixture module promises."""

    def test_every_record_carries_the_required_keys(self) -> None:
        assert len(FUEL_TAX_DETAIL_RECORDS) == 3
        for record in FUEL_TAX_DETAIL_RECORDS:
            assert set(record) >= _REQUIRED_KEYS

    def test_both_driver_variants_ride_the_fixtures(self) -> None:
        wire_shapes = {
            type(record['driver']).__name__ for record in FUEL_TAX_DETAIL_RECORDS
        }
        assert wire_shapes == {'str', 'dict'}

    def test_the_empty_hourly_record_carries_every_array_empty(self) -> None:
        assert FUEL_TAX_DETAIL_EMPTY_HOURLY_RECORD['hasHourlyData'] is False
        for hourly_key in _HOURLY_KEYS:
            assert FUEL_TAX_DETAIL_EMPTY_HOURLY_RECORD[hourly_key] == []

    def test_every_versions_element_is_a_sixteen_hex_token(self) -> None:
        for record in FUEL_TAX_DETAIL_RECORDS:
            versions = record['versions']
            assert isinstance(versions, list)
            assert versions
            for token in versions:
                assert isinstance(token, str)
                assert len(token) == 16
                assert token == token.lower()
                int(token, 16)  # raises if not hex


class TestFuelTaxDetailValidation:
    @pytest.mark.parametrize('required_key', sorted(_REQUIRED_KEYS))
    def test_each_required_key_rejects_absence(self, required_key: str) -> None:
        record = {
            key: value
            for key, value in FUEL_TAX_DETAIL_FULL_RECORD.items()
            if key != required_key
        }
        with pytest.raises(ValidationError):
            FuelTaxDetail.model_validate(record)

    def test_every_record_validates(self) -> None:
        segments = [
            FuelTaxDetail.model_validate(record) for record in FUEL_TAX_DETAIL_RECORDS
        ]
        assert [segment.id for segment in segments] == [
            'b18e401',
            'b18e402',
            'b18e403',
        ]

    def test_full_record_populates_every_field(self) -> None:
        # The alias-trap closure, mechanically (the Trip pattern).
        full = FuelTaxDetail.model_validate(FUEL_TAX_DETAIL_FULL_RECORD)
        for field_name in FuelTaxDetail.model_fields:
            assert getattr(full, field_name) is not None, field_name
        assert isinstance(full.driver, FuelTaxDetailDriverRef)
        assert full.driver.id == 'b4B82'
        assert full.driver.is_driver is True
        assert full.versions == ['00000000000018e1', '00000000000018e2']
        # The int arm inside hourlyOdometer lands as a float element.
        assert full.hourly_odometer == [118102.7, 118171.0, 118253.1]

    def test_empty_hourly_record_lands_the_sentinel_arms(self) -> None:
        segment = FuelTaxDetail.model_validate(FUEL_TAX_DETAIL_EMPTY_HOURLY_RECORD)
        # The UnknownDriver string arm: the shared coercion lifts the
        # bare string, so is_driver nulls exactly here.
        assert segment.driver.id == 'UnknownDriverId'
        assert segment.driver.is_driver is None
        # Empty arrays mirror as empty lists, never as nulls.
        assert segment.has_hourly_data is False
        assert segment.hourly_gps_odometer == []
        assert segment.hourly_is_odometer_interpolated == []
        assert segment.hourly_latitude == []
        assert segment.hourly_longitude == []
        assert segment.hourly_odometer == []
        # The int odometer arms land as floats.
        assert segment.enter_odometer == 411902.0
        assert segment.exit_odometer == 411904.0

    def test_unobserved_vocabulary_tokens_validate(self) -> None:
        # The census-open vocabulary posture with teeth: authority and
        # jurisdiction are plain str mirrors, so tokens the census never
        # showed must validate.
        segment = FuelTaxDetail.model_validate(
            {
                **FUEL_TAX_DETAIL_FULL_RECORD,
                'authority': 'UnobservedFutureAuthority',
                'jurisdiction': 'UnobservedFutureJurisdiction',
            }
        )
        assert segment.authority == 'UnobservedFutureAuthority'
        assert segment.jurisdiction == 'UnobservedFutureJurisdiction'


class TestFuelTaxDetailFrame:
    def test_all_records_build_one_typed_frame(self) -> None:
        frame = models_to_dataframe(
            [
                FuelTaxDetail.model_validate(record)
                for record in FUEL_TAX_DETAIL_RECORDS
            ],
            FuelTaxDetail,
        )
        assert frame.height == 3
        assert frame.schema['enter_time'] == pl.Datetime(
            time_unit='us', time_zone='UTC'
        )
        assert frame.schema['hourly_gps_odometer'] == pl.List(pl.Float64)
        assert frame.schema['hourly_is_odometer_interpolated'] == pl.List(pl.Boolean)
        assert frame.schema['versions'] == pl.List(pl.String)
        assert frame.schema['driver__id'] == pl.String
        # The empty arrays land as empty lists, not nulls.
        empty_row = frame.filter(pl.col('id') == 'b18e402')
        assert empty_row['hourly_odometer'].to_list() == [[]]
        # The sentinel flattening: is_driver nulls exactly on the
        # sentinel row.
        for driver_id, is_driver in zip(
            frame['driver__id'].to_list(),
            frame['driver__is_driver'].to_list(),
            strict=True,
        ):
            assert (driver_id == 'UnknownDriverId') == (is_driver is None)

    def test_empty_input_carries_the_full_schema(self) -> None:
        populated = models_to_dataframe(
            [
                FuelTaxDetail.model_validate(record)
                for record in FUEL_TAX_DETAIL_RECORDS
            ],
            FuelTaxDetail,
        )
        empty = models_to_dataframe([], FuelTaxDetail)
        assert empty.height == 0
        assert empty.schema == populated.schema
