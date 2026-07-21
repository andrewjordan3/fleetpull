"""Tests for fleetpull.models.geotab.log_record.

Every fixture is the committed synthetic 2026-07-21 fixture set
(``tests/geotab_log_records_capture.py``), shaped by the whole-page
census (2,000/2,000 records carried all six keys), so every modeled key
is required — and the drop-key teeth below keep a future
optional-demotion from passing every gate.
"""

from datetime import UTC, datetime

import polars as pl
import pytest
from pydantic import ValidationError

from fleetpull.models.geotab import LogRecord
from fleetpull.records import models_to_dataframe
from tests.geotab_log_records_capture import LOG_RECORD_FULL_RECORD, LOG_RECORD_RECORDS

# The whole-page census observed every key on all 2,000 records, so
# every modeled wire key is required.
_REQUIRED_KEYS = frozenset(
    {'dateTime', 'device', 'id', 'latitude', 'longitude', 'speed'}
)


class TestFixtureProperties:
    """The variant coverage the fixture module promises."""

    def test_every_record_carries_the_required_keys(self) -> None:
        assert len(LOG_RECORD_RECORDS) == 3
        for record in LOG_RECORD_RECORDS:
            assert set(record) >= _REQUIRED_KEYS

    def test_the_records_span_two_event_dates(self) -> None:
        event_dates = {
            LogRecord.model_validate(record).date_time.date().isoformat()
            for record in LOG_RECORD_RECORDS
        }
        assert event_dates == {'2026-07-14', '2026-07-15'}

    def test_speed_rides_the_bare_int_arm_on_every_record(self) -> None:
        for record in LOG_RECORD_RECORDS:
            assert type(record['speed']) is int


class TestLogRecordValidation:
    @pytest.mark.parametrize('required_key', sorted(_REQUIRED_KEYS))
    def test_each_required_key_rejects_absence(self, required_key: str) -> None:
        record = {
            key: value
            for key, value in LOG_RECORD_FULL_RECORD.items()
            if key != required_key
        }
        with pytest.raises(ValidationError):
            LogRecord.model_validate(record)

    def test_the_device_id_rejects_absence(self) -> None:
        with pytest.raises(ValidationError):
            LogRecord.model_validate({**LOG_RECORD_FULL_RECORD, 'device': {}})

    def test_every_record_validates(self) -> None:
        readings = [LogRecord.model_validate(record) for record in LOG_RECORD_RECORDS]
        assert [reading.id for reading in readings] == [
            'b14a101',
            'b14a102',
            'b14a103',
        ]

    def test_full_record_populates_every_field(self) -> None:
        # The alias-trap closure, mechanically (the Trip pattern): the
        # full record carries every modeled field, so a typo'd alias on
        # ANY field would land it as None under extra='ignore' and fail
        # here.
        full = LogRecord.model_validate(LOG_RECORD_FULL_RECORD)
        for field_name in LogRecord.model_fields:
            assert getattr(full, field_name) is not None, field_name
        assert full.device.id == 'b8E2'
        assert full.speed == 63

    def test_date_time_is_recovered_tz_aware(self) -> None:
        # Equality against an aware constant proves both the value and
        # tz-awareness (naive-vs-aware comparison would raise).
        reading = LogRecord.model_validate(LOG_RECORD_FULL_RECORD)
        assert reading.date_time == datetime(2026, 7, 14, 8, 0, 1, tzinfo=UTC)


class TestLogRecordFrame:
    def test_all_records_build_one_typed_frame(self) -> None:
        frame = models_to_dataframe(
            [LogRecord.model_validate(record) for record in LOG_RECORD_RECORDS],
            LogRecord,
        )
        assert frame.height == 3
        assert frame.schema['date_time'] == pl.Datetime(time_unit='us', time_zone='UTC')
        assert frame.schema['device__id'] == pl.String
        assert frame.schema['latitude'] == pl.Float64
        assert frame.schema['speed'] == pl.Int64
        assert frame['speed'].to_list() == [63, 0, 97]

    def test_empty_input_carries_the_full_schema(self) -> None:
        populated = models_to_dataframe(
            [LogRecord.model_validate(record) for record in LOG_RECORD_RECORDS],
            LogRecord,
        )
        empty = models_to_dataframe([], LogRecord)
        assert empty.height == 0
        assert empty.schema == populated.schema
