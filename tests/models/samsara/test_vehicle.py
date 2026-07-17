"""Tests for fleetpull.models.samsara.vehicle.

Every fixture is the committed 2026-07-17 capture set
(``tests/samsara_vehicles_capture.py``): six of the 608 swept records --
the three minimal-shape variants and the three rich variants. The
scrub-preserved fixture properties (the minimal 7-key shape, the
serial/externalIds/gateway equality classes, the single
``staticAssignedDriver`` and ``auxInputType1`` carriers, both ESN
shapes) are asserted here beside the model they serve.
"""

from datetime import UTC, datetime

from fleetpull.models.samsara import (
    Vehicle,
    VehicleExternalIds,
    VehicleGatewayRef,
    VehicleStaticAssignedDriverRef,
)
from tests.samsara_vehicles_capture import VEHICLE_RECORDS

_MINIMAL_KEYS = frozenset(
    {
        'harshAccelerationSettingType',
        'id',
        'name',
        'notes',
        'vehicleRegulationMode',
        'createdAtTime',
        'updatedAtTime',
    }
)


class TestFixtureProperties:
    """The scrub-preserved properties the capture module promises."""

    def test_the_variant_split(self) -> None:
        assert len(VEHICLE_RECORDS) == 6
        minimal = [r for r in VEHICLE_RECORDS if 'gateway' not in r]
        assert len(minimal) == 3
        for record in minimal:
            assert set(record) == _MINIMAL_KEYS

    def test_the_serial_equality_classes(self) -> None:
        # The undashed serial rides three keys per rich record: serial,
        # externalIds['samsara.serial'], and (dashed 4-3-3) gateway.serial.
        for record in VEHICLE_RECORDS:
            if 'gateway' not in record:
                continue
            serial = record['serial']
            assert isinstance(serial, str)
            external = record['externalIds']
            assert isinstance(external, dict)
            assert external['samsara.serial'] == serial
            assert external['samsara.vin'] == record['vin']
            gateway = record['gateway']
            assert isinstance(gateway, dict)
            assert gateway['serial'] == f'{serial[:4]}-{serial[4:7]}-{serial[7:]}'

    def test_single_carriers_and_esn_shapes(self) -> None:
        assert sum('staticAssignedDriver' in r for r in VEHICLE_RECORDS) == 1
        assert sum('auxInputType1' in r for r in VEHICLE_RECORDS) == 1
        esns = [r['esn'] for r in VEHICLE_RECORDS if 'esn' in r]
        assert len(esns) == 2
        assert {str(e)[0].isalpha() for e in esns} == {True, False}

    def test_ids_ascend_in_capture_order(self) -> None:
        identifiers = [
            identifier
            for record in VEHICLE_RECORDS
            if isinstance(identifier := record['id'], str)
        ]
        assert len(identifiers) == 6
        assert identifiers == sorted(identifiers)


class TestVehicleValidation:
    def test_every_record_validates_with_aware_datetimes(self) -> None:
        validated = [Vehicle.model_validate(record) for record in VEHICLE_RECORDS]
        assert len(validated) == 6
        for vehicle in validated:
            assert vehicle.created_at_time.tzinfo is not None
            assert vehicle.updated_at_time.tzinfo is not None

    def test_the_minimal_shape_lands_every_optional_null(self) -> None:
        vehicle = Vehicle.model_validate(VEHICLE_RECORDS[0])
        assert vehicle.id == '212000000000001'
        assert vehicle.gateway is None
        assert vehicle.external_ids is None
        assert vehicle.serial is None
        assert vehicle.vin is None
        assert vehicle.year is None
        assert vehicle.static_assigned_driver is None
        assert vehicle.aux_input_type1 is None

    def test_the_rich_record_pins_the_wire_values(self) -> None:
        vehicle = Vehicle.model_validate(VEHICLE_RECORDS[3])
        assert vehicle.id == '278000000000001'
        assert vehicle.make == 'FORD'
        assert vehicle.model == 'F-550'
        assert vehicle.vin == '4SYNTHV1N00000023'
        assert vehicle.vehicle_regulation_mode == 'unregulated'
        assert vehicle.created_at_time == datetime(2019, 9, 13, 22, 32, 39, tzinfo=UTC)
        gateway = vehicle.gateway
        assert isinstance(gateway, VehicleGatewayRef)
        assert gateway.serial == 'GSYN-AAA-001'
        assert gateway.model == 'VG34'

    def test_the_quoted_year_coerces_to_int(self) -> None:
        # "2013" on the wire; lax coercion types it (the Motive year
        # precedent -- and like Motive, an empty string would fail
        # loudly, which 608 swept records never showed).
        years = {Vehicle.model_validate(r).year for r in VEHICLE_RECORDS if 'year' in r}
        assert years == {2013, 2015, 2019}

    def test_the_dotted_external_id_aliases_land(self) -> None:
        # The aliases sit above the to_camel generator's reach; a broken
        # alias would land these as None under extra='ignore'.
        vehicle = Vehicle.model_validate(VEHICLE_RECORDS[3])
        external = vehicle.external_ids
        assert isinstance(external, VehicleExternalIds)
        assert external.samsara_serial == 'GSYNAAA001'
        assert external.samsara_vin == '4SYNTHV1N00000023'

    def test_the_assigned_driver_reference(self) -> None:
        validated = [Vehicle.model_validate(record) for record in VEHICLE_RECORDS]
        carriers = [v for v in validated if v.static_assigned_driver is not None]
        assert len(carriers) == 1
        driver = carriers[0].static_assigned_driver
        assert isinstance(driver, VehicleStaticAssignedDriverRef)
        assert driver.id == '7000001'
        assert driver.name == 'Synthetic Driver001'

    def test_the_aux_input_carrier(self) -> None:
        validated = [Vehicle.model_validate(record) for record in VEHICLE_RECORDS]
        aux_values = {v.aux_input_type1 for v in validated}
        assert aux_values == {None, 'powerTakeOff'}

    def test_empty_notes_mirror_verbatim(self) -> None:
        # "" on every captured record; models preserve it faithfully --
        # the DataFrame boundary is where empty strings become null.
        for record in VEHICLE_RECORDS:
            assert Vehicle.model_validate(record).notes == ''


class TestExcludedFields:
    def test_tags_are_not_modeled(self) -> None:
        # The list-of-structs exclusion (Device/User precedent): tags
        # ride every rich capture and must land nowhere.
        rich_record = VEHICLE_RECORDS[3]
        assert 'tags' in rich_record
        vehicle = Vehicle.model_validate(rich_record)
        assert not hasattr(vehicle, 'tags')
        assert 'tags' not in Vehicle.model_fields
