"""Tests for fleetpull.models.geotab.fill_up.

Every fixture is the committed synthetic 2026-07-21 fixture set
(``tests/geotab_fill_ups_capture.py``), shaped by the whole-page census
(100/100 records carried every modeled key, on the estimates-only
tenant), so every modeled key is required. The observed arms are all
exercised: both driver shapes, the ``-1.0`` ``derivedVolume`` sentinel,
the mixed-numeric int arms, and ``fuelTransactions`` riding raw and
ignored.
"""

import polars as pl
import pytest
from pydantic import ValidationError

from fleetpull.models.geotab import FillUp, FillUpDriverRef
from fleetpull.records import models_to_dataframe
from fleetpull.vocabulary import JsonValue
from tests.geotab_fill_ups_capture import (
    FILL_UP_FULL_RECORD,
    FILL_UP_RECORDS,
    FILL_UP_SENTINEL_RECORD,
)

# The whole-page census observed every key on all 100 records, so every
# modeled wire key is required.
_REQUIRED_KEYS = frozenset(
    {
        'confidence',
        'cost',
        'currencyCode',
        'dateTime',
        'derivedVolume',
        'device',
        'distance',
        'driver',
        'id',
        'location',
        'odometer',
        'productType',
        'tankCapacity',
        'tankLevelExtrema',
        'totalFuelUsed',
        'version',
        'volume',
    }
)


class TestFixtureProperties:
    """The variant coverage the fixture module promises."""

    def test_every_record_carries_the_required_keys(self) -> None:
        assert len(FILL_UP_RECORDS) == 3
        for record in FILL_UP_RECORDS:
            assert set(record) >= _REQUIRED_KEYS

    def test_both_driver_variants_ride_the_fixtures(self) -> None:
        wire_shapes = {type(record['driver']).__name__ for record in FILL_UP_RECORDS}
        assert wire_shapes == {'str', 'dict'}

    def test_the_estimates_only_tenant_shape(self) -> None:
        # The census truth behind the model caveat: no fuel-transaction
        # integration, so cost is 0.0 and fuelTransactions is empty on
        # every record.
        for record in FILL_UP_RECORDS:
            assert record['cost'] == 0.0
            assert record['fuelTransactions'] == []
            assert record['productType'] == 'Unknown'

    def test_all_three_tank_capacity_source_tokens_appear(self) -> None:
        capacities = [record['tankCapacity'] for record in FILL_UP_RECORDS]
        sources = set()
        for capacity in capacities:
            assert isinstance(capacity, dict)
            sources.add(capacity['source'])
        assert sources == {'EstimateFuelLevel', 'DiagnosticTankCapacity', 'Unknown'}


class TestFillUpValidation:
    @pytest.mark.parametrize('required_key', sorted(_REQUIRED_KEYS))
    def test_each_required_key_rejects_absence(self, required_key: str) -> None:
        record = {
            key: value
            for key, value in FILL_UP_FULL_RECORD.items()
            if key != required_key
        }
        with pytest.raises(ValidationError):
            FillUp.model_validate(record)

    @pytest.mark.parametrize('extrema_key', ['maximaPoint', 'minimaPoint'])
    def test_each_extrema_point_rejects_absence(self, extrema_key: str) -> None:
        extrema = FILL_UP_FULL_RECORD['tankLevelExtrema']
        assert isinstance(extrema, dict)
        stripped = {key: value for key, value in extrema.items() if key != extrema_key}
        with pytest.raises(ValidationError):
            FillUp.model_validate({**FILL_UP_FULL_RECORD, 'tankLevelExtrema': stripped})

    def test_every_record_validates(self) -> None:
        fill_ups = [FillUp.model_validate(record) for record in FILL_UP_RECORDS]
        assert [fill_up.id for fill_up in fill_ups] == [
            'b16c201',
            'b16c202',
            'b16c203',
        ]

    def test_full_record_populates_every_field(self) -> None:
        # The alias-trap closure, mechanically (the Trip pattern).
        full = FillUp.model_validate(FILL_UP_FULL_RECORD)
        for field_name in FillUp.model_fields:
            assert getattr(full, field_name) is not None, field_name
        assert isinstance(full.driver, FillUpDriverRef)
        assert full.driver.id == 'b4B82'
        assert full.driver.is_driver is True
        assert full.tank_level_extrema.maxima_point.data == 0.95
        assert full.tank_level_extrema.minima_point.data == 0.31

    def test_fuel_transactions_ride_raw_and_are_ignored(self) -> None:
        # Excluded as value-unobservable (empty on 100/100; the
        # integrated-tenant note lives on the model): the wire key is
        # not a model field, and validation succeeds with it present.
        assert 'fuel_transactions' not in FillUp.model_fields
        validated = FillUp.model_validate(FILL_UP_FULL_RECORD)
        assert 'fuelTransactions' in FILL_UP_FULL_RECORD
        assert validated.id == 'b16c201'

    def test_sentinel_record_lands_the_sentinel_arms(self) -> None:
        sentinel = FillUp.model_validate(FILL_UP_SENTINEL_RECORD)
        # The UnknownDriver string arm: the shared coercion lifts the
        # bare string, so is_driver nulls exactly here.
        assert sentinel.driver.id == 'UnknownDriverId'
        assert sentinel.driver.is_driver is None
        # The could-not-derive sentinel, mirrored verbatim.
        assert sentinel.derived_volume == -1.0
        # The int arms of the mixed numerics land as floats.
        assert sentinel.distance == 388.0
        assert sentinel.volume == 96.0
        assert sentinel.tank_capacity.volume == 200.0

    def test_unobserved_vocabulary_tokens_validate(self) -> None:
        # The census-open vocabulary posture with teeth: confidence,
        # productType, currencyCode, and tankCapacity.source are plain
        # str mirrors, so tokens the census never showed must validate.
        unobserved_capacity: dict[str, JsonValue] = {
            'source': 'UnobservedFutureSource',
            'volume': 1.0,
        }
        record = {
            **FILL_UP_FULL_RECORD,
            'confidence': 'UnobservedFutureMethod, FuelLevel',
            'productType': 'UnobservedFutureProduct',
            'currencyCode': 'ZZZ',
            'tankCapacity': unobserved_capacity,
        }
        fill_up = FillUp.model_validate(record)
        assert fill_up.confidence == 'UnobservedFutureMethod, FuelLevel'
        assert fill_up.product_type == 'UnobservedFutureProduct'
        assert fill_up.currency_code == 'ZZZ'
        assert fill_up.tank_capacity.source == 'UnobservedFutureSource'


class TestFillUpFrame:
    def test_all_records_build_one_typed_frame(self) -> None:
        frame = models_to_dataframe(
            [FillUp.model_validate(record) for record in FILL_UP_RECORDS], FillUp
        )
        assert frame.height == 3
        assert frame.schema['date_time'] == pl.Datetime(time_unit='us', time_zone='UTC')
        assert frame.schema['derived_volume'] == pl.Float64
        assert frame.schema['driver__id'] == pl.String
        assert frame.schema['driver__is_driver'] == pl.Boolean
        assert frame.schema['location__x'] == pl.Float64
        assert frame.schema['tank_level_extrema__maxima_point__data'] == pl.Float64
        # The sentinel flattening: the string lands verbatim, is_driver
        # nulls exactly on sentinel rows.
        for driver_id, is_driver in zip(
            frame['driver__id'].to_list(),
            frame['driver__is_driver'].to_list(),
            strict=True,
        ):
            assert (driver_id == 'UnknownDriverId') == (is_driver is None)
        assert frame['derived_volume'].to_list() == [143.2, -1.0, 88.4]

    def test_empty_input_carries_the_full_schema(self) -> None:
        populated = models_to_dataframe(
            [FillUp.model_validate(record) for record in FILL_UP_RECORDS], FillUp
        )
        empty = models_to_dataframe([], FillUp)
        assert empty.height == 0
        assert empty.schema == populated.schema
