"""Tests for fleetpull.models.geotab.text_message.

Every fixture is the committed synthetic 2026-07-21 fixture set
(``tests/geotab_text_messages_capture.py``), shaped by the wave three
SCALE census (25,000 records; NO per-record ``version``, NO ``dateTime``
— the append-only asymmetry, the event time is ``sent``). Requiredness
is the wave-two conservative posture: only the structural identity
(``id`` / ``sent``) rejects absence. The ``messageContent`` nested block
and its ``ids`` list[str] are pinned here.
"""

import polars as pl
import pytest
from pydantic import ValidationError

from fleetpull.models.geotab import TextMessage, TextMessageContent
from fleetpull.records import models_to_dataframe
from tests.geotab_text_messages_capture import (
    TEXT_MESSAGE_FULL_RECORD,
    TEXT_MESSAGE_RECORDS,
    TEXT_MESSAGE_SPARSE_RECORD,
)

# The wave-two structural identity for the versionless vertical: id and
# the event time (sent, in place of the absent dateTime). Everything
# else is optional.
_REQUIRED_KEYS = frozenset({'id', 'sent'})


class TestFixtureProperties:
    """The variant coverage the fixture module promises."""

    def test_every_record_carries_the_required_keys(self) -> None:
        assert len(TEXT_MESSAGE_RECORDS) == 3
        for record in TEXT_MESSAGE_RECORDS:
            assert set(record) >= _REQUIRED_KEYS

    def test_no_record_carries_version_or_date_time(self) -> None:
        # The append-only asymmetry: no per-record version key AND no
        # dateTime key — the event time is sent.
        for record in TEXT_MESSAGE_RECORDS:
            assert 'version' not in record
            assert 'dateTime' not in record
        assert 'version' not in TextMessage.model_fields
        assert 'date_time' not in TextMessage.model_fields

    def test_the_receipts_are_absent_on_the_sparse_record(self) -> None:
        for key in ('delivered', 'read'):
            assert key in TEXT_MESSAGE_FULL_RECORD
            assert key not in TEXT_MESSAGE_SPARSE_RECORD

    def test_every_record_carries_a_message_content_block(self) -> None:
        for record in TEXT_MESSAGE_RECORDS:
            content = record['messageContent']
            assert isinstance(content, dict)
            assert set(content) == {'contentType', 'ids'}
            assert isinstance(content['ids'], list)


class TestTextMessageValidation:
    @pytest.mark.parametrize('required_key', sorted(_REQUIRED_KEYS))
    def test_each_required_key_rejects_absence(self, required_key: str) -> None:
        record = {
            key: value
            for key, value in TEXT_MESSAGE_FULL_RECORD.items()
            if key != required_key
        }
        with pytest.raises(ValidationError):
            TextMessage.model_validate(record)

    def test_every_record_validates(self) -> None:
        messages = [
            TextMessage.model_validate(record) for record in TEXT_MESSAGE_RECORDS
        ]
        assert [message.id for message in messages] == ['bTM201', 'bTM202', 'bTM203']

    def test_full_record_populates_every_field(self) -> None:
        # The alias-trap closure, mechanically (the Trip pattern).
        full = TextMessage.model_validate(TEXT_MESSAGE_FULL_RECORD)
        for field_name in TextMessage.model_fields:
            assert getattr(full, field_name) is not None, field_name
        assert full.is_direction_to_vehicle is True
        assert full.message_size == 128
        assert full.device is not None
        assert full.device.id == 'bTV801'

    def test_sparse_record_nulls_the_absent_receipts(self) -> None:
        sparse = TextMessage.model_validate(TEXT_MESSAGE_SPARSE_RECORD)
        assert sparse.delivered is None
        assert sparse.read is None
        assert sparse.sent is not None

    def test_message_content_block_carries_the_id_list(self) -> None:
        # The nested block with a direct list[str] ids field (NOT the
        # annotations id-object reduction — the elements ARE strings).
        full = TextMessage.model_validate(TEXT_MESSAGE_FULL_RECORD)
        assert isinstance(full.message_content, TextMessageContent)
        assert full.message_content.content_type == 'Text'
        assert full.message_content.ids == ['bMC501', 'bMC502']

    @pytest.mark.parametrize('nested_key', ['contentType', 'ids'])
    def test_message_content_block_requires_both_keys(self, nested_key: str) -> None:
        # The nested-block-required convention: a present messageContent
        # block missing either key is a shape change and must fail.
        content = TEXT_MESSAGE_FULL_RECORD['messageContent']
        assert isinstance(content, dict)
        partial = {key: value for key, value in content.items() if key != nested_key}
        with pytest.raises(ValidationError):
            TextMessage.model_validate(
                {**TEXT_MESSAGE_FULL_RECORD, 'messageContent': partial}
            )

    def test_object_only_device_ref_still_lifts_a_bare_string(self) -> None:
        # The defensive lift on the census-object-only device ref (the
        # StatusData census-scope lesson).
        lifted = TextMessage.model_validate(
            {**TEXT_MESSAGE_FULL_RECORD, 'device': 'UnobservedSentinelId'}
        )
        assert lifted.device is not None
        assert lifted.device.id == 'UnobservedSentinelId'

    def test_unobserved_content_type_validates(self) -> None:
        # contentType is a census-open str mirror.
        message = TextMessage.model_validate(
            {
                **TEXT_MESSAGE_FULL_RECORD,
                'messageContent': {
                    'contentType': 'UnobservedFutureType',
                    'ids': ['bMC599'],
                },
            }
        )
        assert message.message_content is not None
        assert message.message_content.content_type == 'UnobservedFutureType'


class TestTextMessageFrame:
    def test_all_records_build_one_typed_frame(self) -> None:
        frame = models_to_dataframe(
            [TextMessage.model_validate(record) for record in TEXT_MESSAGE_RECORDS],
            TextMessage,
        )
        assert frame.height == 3
        assert frame.schema['sent'] == pl.Datetime(time_unit='us', time_zone='UTC')
        assert frame.schema['delivered'] == pl.Datetime(time_unit='us', time_zone='UTC')
        assert frame.schema['device__id'] == pl.String
        assert frame.schema['message_content__content_type'] == pl.String
        assert frame.schema['message_content__ids'] == pl.List(pl.String)
        # The append-only asymmetry holds in the derived schema.
        assert 'version' not in frame.columns
        assert 'date_time' not in frame.columns
        assert frame['message_content__ids'].to_list() == [
            ['bMC501', 'bMC502'],
            ['bMC503'],
            ['bMC504'],
        ]
        assert frame['read'].null_count() == 1

    def test_empty_input_carries_the_full_schema(self) -> None:
        populated = models_to_dataframe(
            [TextMessage.model_validate(record) for record in TEXT_MESSAGE_RECORDS],
            TextMessage,
        )
        empty = models_to_dataframe([], TextMessage)
        assert empty.height == 0
        assert empty.schema == populated.schema
