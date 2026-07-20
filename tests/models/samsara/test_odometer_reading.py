"""Tests for fleetpull.models.samsara.odometer_reading.

Every fixture is the committed 2026-07-20 capture set
(``tests/samsara_odometer_readings_capture.py``), unnested through the
PRODUCTION ``SamsaraVehicleSeriesPageDecoder`` -- the model mirrors the
flat post-decoder record, so validating decoder output is exactly the
model's input contract (and doubles as the decoder-to-model
integration seam). The census-preserved shapes (the exact
``{time, value}`` series keys with bare-int meter values, the
synthesized-identity keys with serial/vin omitted when ``externalIds``
is absent) are asserted here beside the model that mirrors them;
requiredness carries drop-key rejection teeth (the addresses
precedent).
"""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from fleetpull.models.samsara import OdometerReading
from fleetpull.network.contract import HttpMethod, RequestSpec
from fleetpull.network.decoders import SamsaraVehicleSeriesPageDecoder
from fleetpull.vocabulary import JsonObject
from tests.samsara_odometer_readings_capture import (
    ODOMETER_READINGS_PAGE_RESPONSE,
    ODOMETER_READINGS_TERMINAL_RESPONSE,
    ODOMETER_READINGS_VEHICLE_RECORDS,
)

# The required wire keys of the FLAT record: vehicleId/vehicleName are
# decoder-synthesized (74/74 on the censused page), time/value are the
# wire-verbatim series keys (present on all 9,480 censused readings).
# vehicleSerial/vehicleVin are deliberately NOT here -- optional by the
# conservative posture (one page is not a whole-population oath).
_REQUIRED_KEYS = frozenset({'vehicleId', 'vehicleName', 'time', 'value'})


def _decoded_reading_records() -> list[JsonObject]:
    """The capture set unnested through the production decoder."""
    decoder = SamsaraVehicleSeriesPageDecoder(
        records_key='data', results_limit=512, series_key='obdOdometerMeters'
    )
    spec = RequestSpec(
        method=HttpMethod.GET,
        url='https://api.example.test/fleet/vehicles/stats/history',
    )
    page = decoder.decode_page(spec, ODOMETER_READINGS_PAGE_RESPONSE)
    terminal = decoder.decode_page(spec, ODOMETER_READINGS_TERMINAL_RESPONSE)
    return page.records + terminal.records


_READING_RECORDS: list[JsonObject] = _decoded_reading_records()


class TestFixtureProperties:
    """The variant coverage the capture module promises."""

    def test_the_vehicle_axis_pages_are_disjoint(self) -> None:
        # The cursor walks the vehicle axis: zero vehicle-id overlap
        # across the committed page pair (the probe-proven shape).
        page_data = ODOMETER_READINGS_PAGE_RESPONSE['data']
        terminal_data = ODOMETER_READINGS_TERMINAL_RESPONSE['data']
        assert isinstance(page_data, list)
        assert isinstance(terminal_data, list)
        page_ids = {vehicle['id'] for vehicle in page_data if isinstance(vehicle, dict)}
        terminal_ids = {
            vehicle['id'] for vehicle in terminal_data if isinstance(vehicle, dict)
        }
        assert page_ids.isdisjoint(terminal_ids)

    def test_the_variant_split(self) -> None:
        # Multi-reading carrier, externalIds-absent single-reading,
        # terminal-page single-reading carrier -- 3 + 1 + 1 readings.
        assert len(ODOMETER_READINGS_VEHICLE_RECORDS) == 3
        absent = sum('externalIds' not in v for v in ODOMETER_READINGS_VEHICLE_RECORDS)
        assert absent == 1
        assert len(_READING_RECORDS) == 5

    def test_every_value_is_a_bare_int(self) -> None:
        # The census observed every value int (no float drift); the
        # fixtures span the observed 3,552,000..1,012,456,215 range.
        values: list[int] = []
        for record in _READING_RECORDS:
            value = record['value']
            assert isinstance(value, int)
            assert not isinstance(value, bool)
            values.append(value)
        assert min(values) == 3552000
        assert max(values) == 1012456215

    def test_every_flat_record_carries_the_required_keys(self) -> None:
        for record in _READING_RECORDS:
            assert set(record) >= _REQUIRED_KEYS


class TestOdometerReadingValidation:
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
            OdometerReading.model_validate(record)

    def test_every_decoded_record_validates_with_aware_datetimes(self) -> None:
        validated = [
            OdometerReading.model_validate(record) for record in _READING_RECORDS
        ]
        assert len(validated) == 5
        for reading in validated:
            assert reading.time.tzinfo is not None

    def test_the_maximal_record_pins_the_wire_values(self) -> None:
        reading = OdometerReading.model_validate(_READING_RECORDS[0])
        assert reading.vehicle_id == '281474980000021'
        assert reading.vehicle_name == 'Truck 301'
        assert reading.vehicle_serial == 'GSYNTH00002A'
        assert reading.vehicle_vin == 'SYNTH000000000021'
        assert reading.time == datetime(2026, 1, 1, 12, 0, 7, 400000, tzinfo=UTC)
        assert reading.value == 152000345
        assert isinstance(reading.value, int)

    def test_absent_serial_and_vin_land_none(self) -> None:
        # The externalIds-absent vehicle's reading: the decoder omitted
        # the keys, and the optional fields land None -- never a crash.
        reading = OdometerReading.model_validate(_READING_RECORDS[3])
        assert reading.vehicle_id == '281474980000022'
        assert reading.vehicle_serial is None
        assert reading.vehicle_vin is None
