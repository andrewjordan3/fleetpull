"""Tests for fleetpull.models.geotab.device.

The shared fixtures in ``tests/geotab_devices_capture.py`` model the
provider wire shape with deterministic synthetic values: GO7-era,
GO9-era (with and without ``deviceFlags``/``devicePlans``), and trailer
entry shapes drive validation and the mixed-shape frame. The one
constructed variant uses the provider year-one ``ignoreDownloadsUntil``
edge value to prove the exclusion holds.
"""

from datetime import UTC, datetime

import polars as pl

from fleetpull.models.geotab import CustomFeatures, Device, DeviceFlags
from fleetpull.records import models_to_dataframe
from fleetpull.vocabulary import JsonObject
from tests.geotab_devices_capture import DEVICE_RECORDS, TRAILER_DEVICE_RECORD

_ALL_SHAPES: list[JsonObject] = [*DEVICE_RECORDS, TRAILER_DEVICE_RECORD]


class TestDeviceValidation:
    def test_every_fixture_shape_validates(self) -> None:
        validated = [Device.model_validate(record) for record in _ALL_SHAPES]
        assert [device.id for device in validated] == [
            'bF7C22',
            'bF7C19',
            'bF7C24',
            'bF7C1C',
            'bF7C25',
            'bF7C18',
            'bF7C1F',
        ]

    def test_shape_poverty_lands_as_nulls_not_errors(self) -> None:
        # Two of the six tracked records carry no deviceFlags/devicePlans --
        # a shape, not a curiosity; both modeled fields land as None.
        poor = [Device.model_validate(record) for record in DEVICE_RECORDS[-2:]]
        assert [device.device_plans for device in poor] == [None, None]
        assert [device.device_flags for device in poor] == [None, None]
        rich = Device.model_validate(DEVICE_RECORDS[0])
        assert rich.device_plans == ['Pro']
        assert isinstance(rich.device_flags, DeviceFlags)

    def test_present_device_flags_populates_every_field(self) -> None:
        # The acronym-alias trap, closed mechanically: every fixture
        # deviceFlags block carries every key, so a typo'd alias on ANY
        # field (to_camel's isHosAllowed vs the wire's isHOSAllowed kind)
        # would land that field as None under extra='ignore' and fail
        # here -- "the shape validates" alone cannot catch it.
        flags = Device.model_validate(DEVICE_RECORDS[0]).device_flags
        assert flags is not None
        for field_name in DeviceFlags.model_fields:
            assert getattr(flags, field_name) is not None, field_name
        assert flags.active_features == ['GeotabDriveHos']
        assert flags.is_hos_allowed is True
        assert flags.is_ui_allowed is True
        assert flags.is_vin_allowed is True

    def test_custom_features_populates_from_every_tracked_record(self) -> None:
        for record in DEVICE_RECORDS:
            features = Device.model_validate(record).custom_features
            assert isinstance(features, CustomFeatures)
            assert features.auto_hos is True

    def test_trailer_sentinels_are_stored_as_is(self) -> None:
        trailer = Device.model_validate(TRAILER_DEVICE_RECORD)
        assert trailer.device_type == 'None'
        assert trailer.product_id == -1
        assert trailer.tmp_trailer_id == 'SynthTmpTrailerId000001'
        # The VIN sentinels: "" and the literal "?", never interpreted.
        assert trailer.vehicle_identification_number == ''
        assert trailer.engine_vehicle_identification_number == '?'
        # The trailer fixture carries neither nested block.
        assert trailer.device_flags is None
        assert trailer.custom_features is None

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
        # The nested blocks arrive flattened with typed leaf columns.
        assert frame.schema['device_flags__active_features'] == pl.List(pl.String)
        assert frame.schema['device_flags__is_engine_allowed'] == pl.Boolean
        assert frame.schema['custom_features__auto_hos'] == pl.Boolean
        # Absent-in-shape fields land as nulls, never as missing columns.
        assert frame['tmp_trailer_id'].null_count() == len(DEVICE_RECORDS)

    def test_absent_device_flags_blocks_land_as_nulls_in_the_frame(self) -> None:
        # b107/b10A (no deviceFlags) and the trailer null every
        # device_flags__* column; b101's row carries values. The trailer
        # also nulls custom_features__auto_hos (no customFeatures there).
        frame = models_to_dataframe(
            [Device.model_validate(record) for record in _ALL_SHAPES], Device
        )
        engine_allowed = frame['device_flags__is_engine_allowed'].to_list()
        assert engine_allowed[0] is True  # b101, flags present
        assert engine_allowed[-3:] == [None, None, None]  # b107, b10A, trailer
        auto_hos = frame['custom_features__auto_hos'].to_list()
        assert auto_hos[:6] == [True] * 6  # every tracked record
        assert auto_hos[-1] is None  # the trailer

    def test_empty_harvest_yields_zero_rows_with_the_full_schema(self) -> None:
        populated = models_to_dataframe(
            [Device.model_validate(record) for record in _ALL_SHAPES], Device
        )
        empty = models_to_dataframe([], Device)
        assert empty.height == 0
        assert empty.schema == populated.schema

    def test_year_one_ignore_downloads_until_is_never_modeled(self) -> None:
        # Constructed variant of a fixture record: the provider edge case uses
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
