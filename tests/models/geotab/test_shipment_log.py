"""Tests for fleetpull.models.geotab.shipment_log.

Every fixture is the committed synthetic 2026-07-21 fixture set
(``tests/geotab_shipment_logs_capture.py``), shaped by the wave three
SCALE census (ten keys, census-total on 2,771 records). Requiredness is
the wave-two conservative posture: only the structural identity (``id``
/ ``dateTime`` / ``version`` / ``driver``) rejects absence.
"""

import polars as pl
import pytest
from pydantic import ValidationError

from fleetpull.models.geotab import ShipmentLog, ShipmentLogDriverRef
from fleetpull.records import models_to_dataframe
from tests.geotab_shipment_logs_capture import (
    SHIPMENT_LOG_FULL_RECORD,
    SHIPMENT_LOG_RECORDS,
    SHIPMENT_LOG_SPARSE_RECORD,
)

# The wave-two structural identity: id, the event time, the version, and
# the primary entity ref (driver, consistent with the log family).
# Everything else is optional.
_REQUIRED_KEYS = frozenset({'dateTime', 'driver', 'id', 'version'})


class TestFixtureProperties:
    """The variant coverage the fixture module promises."""

    def test_every_record_carries_the_required_keys(self) -> None:
        assert len(SHIPMENT_LOG_RECORDS) == 3
        for record in SHIPMENT_LOG_RECORDS:
            assert set(record) >= _REQUIRED_KEYS

    def test_the_optional_device_is_absent_on_the_sparse_record(self) -> None:
        assert 'device' in SHIPMENT_LOG_FULL_RECORD
        assert 'device' not in SHIPMENT_LOG_SPARSE_RECORD

    def test_every_record_carries_a_version(self) -> None:
        for record in SHIPMENT_LOG_RECORDS:
            assert isinstance(record['version'], str)


class TestShipmentLogValidation:
    @pytest.mark.parametrize('required_key', sorted(_REQUIRED_KEYS))
    def test_each_required_key_rejects_absence(self, required_key: str) -> None:
        record = {
            key: value
            for key, value in SHIPMENT_LOG_FULL_RECORD.items()
            if key != required_key
        }
        with pytest.raises(ValidationError):
            ShipmentLog.model_validate(record)

    def test_every_record_validates(self) -> None:
        shipments = [
            ShipmentLog.model_validate(record) for record in SHIPMENT_LOG_RECORDS
        ]
        assert [shipment.id for shipment in shipments] == [
            'bSL201',
            'bSL202',
            'bSL203',
        ]

    def test_full_record_populates_every_field(self) -> None:
        # The alias-trap closure, mechanically (the Trip pattern).
        full = ShipmentLog.model_validate(SHIPMENT_LOG_FULL_RECORD)
        for field_name in ShipmentLog.model_fields:
            assert getattr(full, field_name) is not None, field_name
        assert isinstance(full.driver, ShipmentLogDriverRef)
        assert full.driver.id == 'bSR601'
        assert full.device is not None
        assert full.device.id == 'bSV801'
        assert full.commodity == 'Synthetic Commodity Alpha'
        assert full.version == '0000000000002b01'

    def test_sparse_record_nulls_the_optional_device(self) -> None:
        sparse = ShipmentLog.model_validate(SHIPMENT_LOG_SPARSE_RECORD)
        assert sparse.device is None
        assert sparse.driver.id == 'bSR602'

    @pytest.mark.parametrize('reference_key', ['device', 'driver'])
    def test_object_only_refs_still_lift_a_bare_string(
        self, reference_key: str
    ) -> None:
        # The defensive lift on the census-object-only refs (the
        # StatusData census-scope lesson).
        lifted = ShipmentLog.model_validate(
            {**SHIPMENT_LOG_FULL_RECORD, reference_key: 'UnobservedSentinelId'}
        )
        reference = getattr(lifted, reference_key)
        assert reference is not None
        assert reference.id == 'UnobservedSentinelId'

    def test_unobserved_vocabulary_strings_validate(self) -> None:
        # commodity/documentNumber/shipperName are census-open str
        # mirrors, so unobserved values must validate.
        shipment = ShipmentLog.model_validate(
            {
                **SHIPMENT_LOG_FULL_RECORD,
                'commodity': 'An Unobserved Commodity',
                'shipperName': 'An Unobserved Shipper',
            }
        )
        assert shipment.commodity == 'An Unobserved Commodity'
        assert shipment.shipper_name == 'An Unobserved Shipper'


class TestShipmentLogFrame:
    def test_all_records_build_one_typed_frame(self) -> None:
        frame = models_to_dataframe(
            [ShipmentLog.model_validate(record) for record in SHIPMENT_LOG_RECORDS],
            ShipmentLog,
        )
        assert frame.height == 3
        assert frame.schema['date_time'] == pl.Datetime(time_unit='us', time_zone='UTC')
        assert frame.schema['active_from'] == pl.Datetime(
            time_unit='us', time_zone='UTC'
        )
        assert frame.schema['device__id'] == pl.String
        assert frame.schema['driver__id'] == pl.String
        assert frame['device__id'].to_list() == ['bSV801', None, 'bSV803']
        assert frame['driver__id'].to_list() == ['bSR601', 'bSR602', 'bSR625']

    def test_empty_input_carries_the_full_schema(self) -> None:
        populated = models_to_dataframe(
            [ShipmentLog.model_validate(record) for record in SHIPMENT_LOG_RECORDS],
            ShipmentLog,
        )
        empty = models_to_dataframe([], ShipmentLog)
        assert empty.height == 0
        assert empty.schema == populated.schema
