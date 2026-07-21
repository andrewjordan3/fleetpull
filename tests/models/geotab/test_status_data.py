"""Tests for fleetpull.models.geotab.status_data.

Every fixture is the committed synthetic 2026-07-21 fixture set
(``tests/geotab_status_data_capture.py``), shaped by the whole-page
census (2,000/2,000 records carried all seven keys — ``version``
included, the asymmetry against LogRecord), so every modeled key is
required and the drop-key teeth keep a future optional-demotion loud.
"""

import polars as pl
import pytest
from pydantic import ValidationError

from fleetpull.models.geotab import StatusData
from fleetpull.records import models_to_dataframe
from tests.geotab_status_data_capture import (
    STATUS_DATA_FULL_RECORD,
    STATUS_DATA_RECORDS,
)

# The whole-page census observed every key on all 2,000 records, so
# every modeled wire key is required.
_REQUIRED_KEYS = frozenset(
    {'controller', 'data', 'dateTime', 'device', 'diagnostic', 'id', 'version'}
)


class TestFixtureProperties:
    """The variant coverage the fixture module promises."""

    def test_every_record_carries_the_required_keys(self) -> None:
        assert len(STATUS_DATA_RECORDS) == 3
        for record in STATUS_DATA_RECORDS:
            assert set(record) >= _REQUIRED_KEYS

    def test_both_data_arms_ride_the_fixtures(self) -> None:
        wire_types = {type(record['data']).__name__ for record in STATUS_DATA_RECORDS}
        assert wire_types == {'float', 'int'}

    def test_every_record_carries_a_version(self) -> None:
        # The asymmetry against LogRecord: this active feed versions its
        # records, and the model mirrors it.
        for record in STATUS_DATA_RECORDS:
            assert isinstance(record['version'], str)


class TestStatusDataValidation:
    @pytest.mark.parametrize('required_key', sorted(_REQUIRED_KEYS))
    def test_each_required_key_rejects_absence(self, required_key: str) -> None:
        record = {
            key: value
            for key, value in STATUS_DATA_FULL_RECORD.items()
            if key != required_key
        }
        with pytest.raises(ValidationError):
            StatusData.model_validate(record)

    @pytest.mark.parametrize('reference_key', ['device', 'diagnostic'])
    def test_each_reference_id_rejects_absence(self, reference_key: str) -> None:
        with pytest.raises(ValidationError):
            StatusData.model_validate({**STATUS_DATA_FULL_RECORD, reference_key: {}})

    def test_every_record_validates(self) -> None:
        readings = [StatusData.model_validate(record) for record in STATUS_DATA_RECORDS]
        assert [reading.id for reading in readings] == [
            'b15b201',
            'b15b202',
            'b15b203',
        ]

    def test_full_record_populates_every_field(self) -> None:
        # The alias-trap closure, mechanically (the Trip pattern).
        full = StatusData.model_validate(STATUS_DATA_FULL_RECORD)
        for field_name in StatusData.model_fields:
            assert getattr(full, field_name) is not None, field_name
        assert full.diagnostic.id == 'DiagnosticEngineSpeedId'
        assert full.version == '00000000000015b1'

    def test_the_int_data_arm_lands_as_float(self) -> None:
        int_arm = StatusData.model_validate(STATUS_DATA_RECORDS[1])
        assert int_arm.data == 1200.0
        assert isinstance(int_arm.data, float)

    def test_controller_rides_both_wire_arms(self) -> None:
        # The live-census split (49,745 sentinel strings / 255 objects):
        # the shared coercion lifts the bare sentinel to {'id': ...}, so
        # both arms land as controller__id.
        sentinel = StatusData.model_validate(
            {**STATUS_DATA_FULL_RECORD, 'controller': 'ControllerNoneId'}
        )
        assert sentinel.controller.id == 'ControllerNoneId'
        referenced = StatusData.model_validate(
            {**STATUS_DATA_FULL_RECORD, 'controller': {'id': 'ControllerObdId'}}
        )
        assert referenced.controller.id == 'ControllerObdId'

    def test_an_unobserved_controller_token_validates(self) -> None:
        # The census-open vocabulary posture with teeth: controller is a
        # plain str mirror, so a token the census never showed must
        # validate rather than reject.
        reading = StatusData.model_validate(
            {**STATUS_DATA_FULL_RECORD, 'controller': 'unobserved_future_controller'}
        )
        assert reading.controller.id == 'unobserved_future_controller'


class TestStatusDataFrame:
    def test_all_records_build_one_typed_frame(self) -> None:
        frame = models_to_dataframe(
            [StatusData.model_validate(record) for record in STATUS_DATA_RECORDS],
            StatusData,
        )
        assert frame.height == 3
        assert frame.schema['date_time'] == pl.Datetime(time_unit='us', time_zone='UTC')
        assert frame.schema['data'] == pl.Float64
        assert frame.schema['device__id'] == pl.String
        assert frame.schema['diagnostic__id'] == pl.String
        assert frame['data'].to_list() == [87.5, 1200.0, 0.0]

    def test_empty_input_carries_the_full_schema(self) -> None:
        populated = models_to_dataframe(
            [StatusData.model_validate(record) for record in STATUS_DATA_RECORDS],
            StatusData,
        )
        empty = models_to_dataframe([], StatusData)
        assert empty.height == 0
        assert empty.schema == populated.schema
