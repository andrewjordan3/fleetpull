"""Tests for fleetpull.models.geotab.device.

Every fixture is the committed 2026-07-09 capture set
(``tests/geotab_devices_capture.py``): the three observed shapes --
GO7-era, GO9-era (with and without ``deviceFlags``/``devicePlans``),
and the trailer entry -- drive validation and the mixed-shape frame;
the one CONSTRUCTED variant plants the live-observed year-one
``ignoreDownloadsUntil`` to prove the exclusion holds.
"""

from datetime import UTC, datetime

import polars as pl

from fleetpull.models.geotab import Device
from fleetpull.records import models_to_dataframe
from fleetpull.vocabulary import JsonObject
from tests.geotab_devices_capture import DEVICE_RECORDS, TRAILER_DEVICE_RECORD

_ALL_SHAPES: list[JsonObject] = [*DEVICE_RECORDS, TRAILER_DEVICE_RECORD]


class TestDeviceValidation:
    def test_every_captured_shape_validates(self) -> None:
        validated = [Device.model_validate(record) for record in _ALL_SHAPES]
        assert [device.id for device in validated] == [
            'b101',
            'b102',
            'b105',
            'b106',
            'b107',
            'b10A',
            'b179',
        ]

    def test_shape_poverty_lands_as_nulls_not_errors(self) -> None:
        # Two of the six tracked records carry no deviceFlags/devicePlans --
        # a shape, not a curiosity; devicePlans (the modeled one) is None.
        poor = [Device.model_validate(record) for record in DEVICE_RECORDS[-2:]]
        assert [device.device_plans for device in poor] == [None, None]
        rich = Device.model_validate(DEVICE_RECORDS[0])
        assert rich.device_plans == ['Pro']

    def test_trailer_sentinels_are_stored_as_is(self) -> None:
        trailer = Device.model_validate(TRAILER_DEVICE_RECORD)
        assert trailer.device_type == 'None'
        assert trailer.product_id == -1
        assert trailer.tmp_trailer_id == 'SynthTmpTrailerId000001'
        # The VIN sentinels: "" and the literal "?", never interpreted.
        assert trailer.vehicle_identification_number == ''
        assert trailer.engine_vehicle_identification_number == '?'

    def test_active_to_2050_sentinel_is_ns_safe_and_untouched(self) -> None:
        trailer = Device.model_validate(TRAILER_DEVICE_RECORD)
        assert trailer.active_to == datetime(2050, 1, 1, tzinfo=UTC)


class TestDeviceFrame:
    def test_mixed_shape_set_builds_one_typed_frame(self) -> None:
        models = [Device.model_validate(record) for record in _ALL_SHAPES]
        frame = models_to_dataframe(models, Device)
        assert frame.height == len(_ALL_SHAPES)
        assert frame.schema['id'] == pl.String
        assert frame.schema['product_id'] == pl.Int64
        assert frame.schema['active_to'] == pl.Datetime(time_unit='us', time_zone='UTC')
        assert frame.schema['device_plans'] == pl.List(pl.String)
        # Absent-in-shape fields land as nulls, never as missing columns.
        assert frame['tmp_trailer_id'].null_count() == len(DEVICE_RECORDS)

    def test_empty_harvest_yields_zero_rows_with_the_full_schema(self) -> None:
        populated = models_to_dataframe(
            [Device.model_validate(record) for record in _ALL_SHAPES], Device
        )
        empty = models_to_dataframe([], Device)
        assert empty.height == 0
        assert empty.schema == populated.schema

    def test_year_one_ignore_downloads_until_is_never_modeled(self) -> None:
        # CONSTRUCTED variant of a captured record: the live probe observed
        # ignoreDownloadsUntil at 0001-01-01 (which overflows ns-precision
        # timestamp columns); the field is excluded from the model, so the
        # frame builds clean because it is never a column.
        constructed: JsonObject = {
            **DEVICE_RECORDS[0],
            'ignoreDownloadsUntil': '0001-01-01T00:00:00.000Z',
        }
        frame = models_to_dataframe([Device.model_validate(constructed)], Device)
        assert frame.height == 1
        assert 'ignore_downloads_until' not in frame.columns
        assert 'ignoreDownloadsUntil' not in frame.columns
