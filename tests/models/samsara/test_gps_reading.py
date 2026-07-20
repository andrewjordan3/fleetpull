"""Tests for fleetpull.models.samsara.gps_reading.

Every fixture is the committed 2026-07-20 capture set
(``tests/samsara_gps_readings_capture.py``), unnested through the
PRODUCTION ``SamsaraVehicleSeriesPageDecoder`` -- the model mirrors the
flat post-decoder record, so validating decoder output is exactly the
model's input contract (and doubles as the decoder-to-model
integration seam). The census-preserved shapes (the seven
always-present series keys, the optional ``address`` book reference at
401/2,512, the int|float ``speedMilesPerHour`` mixing modeled float,
the synthesized-identity keys with serial/vin omitted when
``externalIds`` is absent) are asserted here beside the model that
mirrors them; requiredness carries drop-key rejection teeth (the
addresses precedent).
"""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from fleetpull.models.samsara import (
    GpsReading,
    GpsReadingAddressRef,
    GpsReadingReverseGeo,
)
from fleetpull.network.contract import HttpMethod, RequestSpec
from fleetpull.network.decoders import SamsaraVehicleSeriesPageDecoder
from fleetpull.vocabulary import JsonObject
from tests.samsara_gps_readings_capture import (
    GPS_READINGS_PAGE_RESPONSE,
    GPS_READINGS_TERMINAL_RESPONSE,
    GPS_READINGS_VEHICLE_RECORDS,
)

# The required wire keys of the FLAT record: vehicleId/vehicleName are
# decoder-synthesized (74/74 on the censused page); the seven series
# keys rode every one of the 2,512 sampled readings. vehicleSerial/
# vehicleVin are deliberately NOT here -- optional by the conservative
# posture (one page is not a whole-population oath) -- and neither is
# `address` (401/2,512).
_REQUIRED_KEYS = frozenset(
    {
        'vehicleId',
        'vehicleName',
        'time',
        'latitude',
        'longitude',
        'headingDegrees',
        'speedMilesPerHour',
        'isEcuSpeed',
        'reverseGeo',
    }
)


def _decoded_reading_records() -> list[JsonObject]:
    """The capture set unnested through the production decoder."""
    decoder = SamsaraVehicleSeriesPageDecoder(
        records_key='data', results_limit=512, series_key='gps'
    )
    spec = RequestSpec(
        method=HttpMethod.GET,
        url='https://api.example.test/fleet/vehicles/stats/history',
    )
    page = decoder.decode_page(spec, GPS_READINGS_PAGE_RESPONSE)
    terminal = decoder.decode_page(spec, GPS_READINGS_TERMINAL_RESPONSE)
    return page.records + terminal.records


_READING_RECORDS: list[JsonObject] = _decoded_reading_records()


class TestFixtureProperties:
    """The variant coverage the capture module promises."""

    def test_the_vehicle_axis_pages_are_disjoint(self) -> None:
        # The cursor walks the vehicle axis: zero vehicle-id overlap
        # across the committed page pair (the probe-proven shape).
        page_data = GPS_READINGS_PAGE_RESPONSE['data']
        terminal_data = GPS_READINGS_TERMINAL_RESPONSE['data']
        assert isinstance(page_data, list)
        assert isinstance(terminal_data, list)
        page_ids = {vehicle['id'] for vehicle in page_data if isinstance(vehicle, dict)}
        terminal_ids = {
            vehicle['id'] for vehicle in terminal_data if isinstance(vehicle, dict)
        }
        assert page_ids.isdisjoint(terminal_ids)

    def test_the_variant_split(self) -> None:
        # Multi-reading carrier, externalIds-absent single-reading,
        # terminal-page single-reading carrier -- 2 + 1 + 1 readings,
        # two with the address ref and two without.
        assert len(GPS_READINGS_VEHICLE_RECORDS) == 3
        assert sum('externalIds' not in v for v in GPS_READINGS_VEHICLE_RECORDS) == 1
        assert len(_READING_RECORDS) == 4
        assert sum('address' in record for record in _READING_RECORDS) == 2

    def test_the_speed_field_mixes_int_and_float(self) -> None:
        # The wire carried both shapes across the sample; the second
        # reading pins the int shape the float field must absorb.
        assert isinstance(_READING_RECORDS[0]['speedMilesPerHour'], float)
        int_shaped = _READING_RECORDS[1]['speedMilesPerHour']
        assert isinstance(int_shaped, int)
        assert not isinstance(int_shaped, bool)

    def test_every_flat_record_carries_the_required_keys(self) -> None:
        for record in _READING_RECORDS:
            assert set(record) >= _REQUIRED_KEYS


class TestGpsReadingValidation:
    @pytest.mark.parametrize('required_key', sorted(_REQUIRED_KEYS))
    def test_each_required_key_rejects_absence(self, required_key: str) -> None:
        # The requiredness posture with teeth: only a loud rejection
        # here keeps a future optional-demotion from passing every gate.
        record = {
            key: value
            for key, value in _READING_RECORDS[0].items()
            if key != required_key
        }
        with pytest.raises(ValidationError):
            GpsReading.model_validate(record)

    def test_every_decoded_record_validates_with_aware_datetimes(self) -> None:
        validated = [GpsReading.model_validate(record) for record in _READING_RECORDS]
        assert len(validated) == 4
        for reading in validated:
            assert reading.time.tzinfo is not None
            assert isinstance(reading.reverse_geo, GpsReadingReverseGeo)

    def test_the_maximal_record_pins_the_wire_values(self) -> None:
        reading = GpsReading.model_validate(_READING_RECORDS[0])
        assert reading.vehicle_id == '281474980000011'
        assert reading.vehicle_name == 'Truck 201'
        assert reading.vehicle_serial == 'GSYNTH00001A'
        assert reading.vehicle_vin == 'SYNTH000000000011'
        assert reading.time == datetime(2026, 1, 1, 12, 0, 5, 100000, tzinfo=UTC)
        assert reading.latitude == pytest.approx(33.1001)
        assert reading.longitude == pytest.approx(-96.1001)
        assert reading.heading_degrees == 270
        assert isinstance(reading.heading_degrees, int)
        assert reading.speed_miles_per_hour == pytest.approx(58.4)
        assert reading.is_ecu_speed is True
        assert reading.reverse_geo.formatted_location == (
            '100 Example St, Example City, TX'
        )

    def test_the_address_book_reference_lands(self) -> None:
        reading = GpsReading.model_validate(_READING_RECORDS[0])
        address = reading.address
        assert isinstance(address, GpsReadingAddressRef)
        assert address.id == '88000011'
        assert address.name == 'Depot North'

    def test_a_reading_without_the_address_ref_lands_none(self) -> None:
        reading = GpsReading.model_validate(_READING_RECORDS[1])
        assert reading.address is None

    def test_int_shaped_speed_lifts_to_float(self) -> None:
        # The wire mixes int and float; the float field type absorbs
        # both shapes.
        reading = GpsReading.model_validate(_READING_RECORDS[1])
        assert reading.speed_miles_per_hour == pytest.approx(0.0)
        assert isinstance(reading.speed_miles_per_hour, float)
        assert reading.is_ecu_speed is False

    def test_absent_serial_and_vin_land_none(self) -> None:
        # The externalIds-absent vehicle's reading: the decoder omitted
        # the keys, and the optional fields land None -- never a crash.
        reading = GpsReading.model_validate(_READING_RECORDS[2])
        assert reading.vehicle_id == '281474980000012'
        assert reading.vehicle_serial is None
        assert reading.vehicle_vin is None
