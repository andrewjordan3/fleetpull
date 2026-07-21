"""Tests for fleetpull.models.geotab.media_file.

Every fixture is the committed synthetic 2026-07-21 fixture set
(``tests/geotab_media_files_capture.py``), shaped by the wave three
SCALE census (55 records — thin data; ``device`` PROVEN mixed, NO
``dateTime`` — the event time is ``fromDate``). Requiredness is the
wave-two conservative posture: only the structural identity (``id`` /
``fromDate`` / ``version``) rejects absence; both refs are optional (the
ambiguous-primary-entity choice). The three empty-container exclusions
(``metaData`` / ``tags`` / ``thumbnails``) are pinned here: a POPULATED
container must be absorbed, never crash.
"""

import polars as pl
import pytest
from pydantic import ValidationError

from fleetpull.models.geotab import MediaFile
from fleetpull.records import models_to_dataframe
from tests.geotab_media_files_capture import (
    MEDIA_FILE_FULL_RECORD,
    MEDIA_FILE_RECORDS,
    MEDIA_FILE_STRING_DEVICE_RECORD,
)

# The wave-two structural identity: id, the event time (fromDate, in
# place of the absent dateTime), and the version. Both refs are optional
# (a media file's primary entity is ambiguous).
_REQUIRED_KEYS = frozenset({'fromDate', 'id', 'version'})

# The three documented exclusions (the defectList.children doctrine).
_EXCLUDED_KEYS = ('metaData', 'tags', 'thumbnails')


class TestFixtureProperties:
    """The variant coverage the fixture module promises."""

    def test_every_record_carries_the_required_keys(self) -> None:
        assert len(MEDIA_FILE_RECORDS) == 3
        for record in MEDIA_FILE_RECORDS:
            assert set(record) >= _REQUIRED_KEYS

    def test_no_record_carries_a_date_time(self) -> None:
        # The event time is fromDate; there is no dateTime key.
        for record in MEDIA_FILE_RECORDS:
            assert 'dateTime' not in record
        assert 'date_time' not in MediaFile.model_fields

    def test_both_device_arms_ride_the_fixtures(self) -> None:
        wire_shapes = {type(record['device']).__name__ for record in MEDIA_FILE_RECORDS}
        assert wire_shapes == {'str', 'dict'}

    def test_driver_is_a_bare_string_on_every_record(self) -> None:
        for record in MEDIA_FILE_RECORDS:
            assert isinstance(record['driver'], str)

    def test_the_excluded_containers_are_empty_on_every_record(self) -> None:
        for record in MEDIA_FILE_RECORDS:
            assert record['metaData'] == {}
            assert record['tags'] == []
            assert record['thumbnails'] == []


class TestMediaFileValidation:
    @pytest.mark.parametrize('required_key', sorted(_REQUIRED_KEYS))
    def test_each_required_key_rejects_absence(self, required_key: str) -> None:
        record = {
            key: value
            for key, value in MEDIA_FILE_FULL_RECORD.items()
            if key != required_key
        }
        with pytest.raises(ValidationError):
            MediaFile.model_validate(record)

    def test_every_record_validates(self) -> None:
        media = [MediaFile.model_validate(record) for record in MEDIA_FILE_RECORDS]
        assert [item.id for item in media] == ['bMF201', 'bMF202', 'bMF203']

    def test_full_record_populates_every_field(self) -> None:
        # The alias-trap closure, mechanically (the Trip pattern).
        full = MediaFile.model_validate(MEDIA_FILE_FULL_RECORD)
        for field_name in MediaFile.model_fields:
            assert getattr(full, field_name) is not None, field_name
        assert full.device is not None
        assert full.device.id == 'bMV901'
        assert full.driver is not None
        assert full.driver.id == 'bMD301'
        assert full.version == '0000000000002e01'

    def test_device_rides_both_wire_arms(self) -> None:
        # The proven mixed ref: the object arm and the bare string arm
        # both land as device__id.
        object_arm = MediaFile.model_validate(MEDIA_FILE_FULL_RECORD)
        assert object_arm.device is not None
        assert object_arm.device.id == 'bMV901'
        string_arm = MediaFile.model_validate(MEDIA_FILE_STRING_DEVICE_RECORD)
        assert string_arm.device is not None
        assert string_arm.device.id == 'bMV902'

    def test_string_driver_arm_lands_as_the_ref_id(self) -> None:
        # The string-only driver rides the defensive lift so it lands as
        # driver__id (the census-scope lesson).
        full = MediaFile.model_validate(MEDIA_FILE_FULL_RECORD)
        assert full.driver is not None
        assert full.driver.id == 'bMD301'

    @pytest.mark.parametrize('excluded_key', _EXCLUDED_KEYS)
    def test_populated_excluded_containers_are_absorbed(
        self, excluded_key: str
    ) -> None:
        # The documented-exclusion pins: none of the three is a model
        # field (empty on all 55 — element/content shape unobservable),
        # and extra='ignore' absorbs a tenant that DOES populate them.
        assert excluded_key not in MediaFile.model_fields
        assert 'meta_data' not in MediaFile.model_fields
        populated_values = {
            'metaData': {'width': 1920, 'height': 1080},
            'tags': ['synthetic-tag'],
            'thumbnails': [{'id': 'bTH99', 'size': 64}],
        }
        item = MediaFile.model_validate(
            {**MEDIA_FILE_FULL_RECORD, excluded_key: populated_values[excluded_key]}
        )
        assert item.id == 'bMF201'

    def test_unobserved_vocabulary_strings_validate(self) -> None:
        # mediaType/status are census-open str mirrors, so unobserved
        # tokens must validate.
        item = MediaFile.model_validate(
            {
                **MEDIA_FILE_FULL_RECORD,
                'mediaType': 'UnobservedFutureType',
                'status': 'UnobservedFutureStatus',
            }
        )
        assert item.media_type == 'UnobservedFutureType'
        assert item.status == 'UnobservedFutureStatus'


class TestMediaFileFrame:
    def test_all_records_build_one_typed_frame(self) -> None:
        frame = models_to_dataframe(
            [MediaFile.model_validate(record) for record in MEDIA_FILE_RECORDS],
            MediaFile,
        )
        assert frame.height == 3
        assert frame.schema['from_date'] == pl.Datetime(time_unit='us', time_zone='UTC')
        assert frame.schema['device__id'] == pl.String
        assert frame.schema['driver__id'] == pl.String
        # The exclusions hold in the derived schema: no columns for them.
        for excluded in ('meta_data', 'tags', 'thumbnails'):
            assert excluded not in frame.columns
        assert frame['device__id'].to_list() == ['bMV901', 'bMV902', 'bMV903']
        assert frame['driver__id'].to_list() == ['bMD301', 'bMD302', 'bMD325']

    def test_empty_input_carries_the_full_schema(self) -> None:
        populated = models_to_dataframe(
            [MediaFile.model_validate(record) for record in MEDIA_FILE_RECORDS],
            MediaFile,
        )
        empty = models_to_dataframe([], MediaFile)
        assert empty.height == 0
        assert empty.schema == populated.schema
