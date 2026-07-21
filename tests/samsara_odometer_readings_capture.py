"""The committed Samsara odometer_readings capture set (2026-07-20 probe session).

Three FULLY SYNTHETIC vehicle records shaped by the live census of
``GET /fleet/vehicles/stats/history`` with ``types=obdOdometerMeters``
(a 9,480-reading, 135-vehicle 24-hour walk; per-vehicle keys exactly
``id``/``name``/``externalIds``/``obdOdometerMeters``, series keys
exactly ``{time, value}``, every value a bare int -- observed range
3,552,000..1,012,456,215 meters), arranged as a two-page cursor walk
whose pages carry DISJOINT vehicle ids -- the probe-proven
vehicle-axis cursor (three consecutive live pages showed zero
vehicle-id overlap). The variants exercise every decoder and model
arm: a multi-reading vehicle with monotonically increasing odometer
values, a single-reading vehicle with ``externalIds`` ABSENT (a
synthetic variant -- the censused page carried the block 74/74, but
one page is not a whole-population oath and the vehicles surface shows
exactly this variance; downstream it proves the serial/vin omit-absent
posture), and a terminal-page single-reading carrier.

No record values here are scrubbed live values -- every id, name,
serial, VIN-shaped string, timestamp, and odometer value is synthetic
outright. What IS verbatim wire truth: the ``data`` + ``pagination
{endCursor, hasNextPage}`` envelope, the per-vehicle key set with the
literal DOTTED ``externalIds`` keys (``samsara.serial`` /
``samsara.vin``), the millisecond RFC3339 ``time`` shape, the exact
``{time, value}`` series keys with the bare-int meter values, and the
terminal ``hasNextPage: false`` beside an empty-string ``endCursor``.

Consumed by the OdometerReading model tests (which unnest through the
production series decoder -- the model mirrors the flat post-decoder
record) and the odometer_readings endpoint tests -- kept as a helper
module under ``tests/`` so consumers share one capture set (the
``samsara_vehicles_capture`` precedent). The raw JSON literals are the
captures; the parsed objects beside them are what tests consume.
"""

import json

from fleetpull.vocabulary import JsonObject, JsonValue

# A continuation page: the multi-reading carrier (bare-int meter
# values, monotonically increasing) beside the externalIds-ABSENT
# single-reading vehicle. Reading times sit strictly inside a
# 12:00-13:00Z window -- the probe's [startTime, endTime) anchoring
# evidence.
ODOMETER_READINGS_PAGE_RESPONSE_JSON: str = r"""
{
    "data": [
        {
            "id": "281474980000021",
            "name": "Truck 301",
            "externalIds": {
                "samsara.serial": "GSYNTH00002A",
                "samsara.vin": "SYNTH000000000021"
            },
            "obdOdometerMeters": [
                {
                    "time": "2026-01-01T12:00:07.400Z",
                    "value": 152000345
                },
                {
                    "time": "2026-01-01T12:20:09.150Z",
                    "value": 152018892
                },
                {
                    "time": "2026-01-01T12:59:41.600Z",
                    "value": 152047204
                }
            ]
        },
        {
            "id": "281474980000022",
            "name": "Truck 302",
            "obdOdometerMeters": [
                {
                    "time": "2026-01-01T12:15:30.800Z",
                    "value": 3552000
                }
            ]
        }
    ],
    "pagination": {
        "endCursor": "00000000-0000-0000-0000-000000000061",
        "hasNextPage": true
    }
}"""

ODOMETER_READINGS_PAGE_RESPONSE: dict[str, JsonValue] = json.loads(
    ODOMETER_READINGS_PAGE_RESPONSE_JSON
)

# The terminal page shape -- hasNextPage false beside an empty-string
# endCursor (the provider-wide cursor contract) -- carrying a vehicle
# id DISJOINT from page one's (the vehicle-axis cursor, proven live).
ODOMETER_READINGS_TERMINAL_RESPONSE_JSON: str = r"""
{
    "data": [
        {
            "id": "281474980000023",
            "name": "Truck 303",
            "externalIds": {
                "samsara.serial": "GSYNTH00002C",
                "samsara.vin": "SYNTH000000000023"
            },
            "obdOdometerMeters": [
                {
                    "time": "2026-01-01T12:40:12.050Z",
                    "value": 1012456215
                }
            ]
        }
    ],
    "pagination": {
        "endCursor": "",
        "hasNextPage": false
    }
}"""

ODOMETER_READINGS_TERMINAL_RESPONSE: dict[str, JsonValue] = json.loads(
    ODOMETER_READINGS_TERMINAL_RESPONSE_JSON
)


def _envelope_records(envelope: dict[str, JsonValue]) -> list[JsonObject]:
    result = envelope['data']
    assert isinstance(result, list)
    records: list[JsonObject] = []
    for record in result:
        assert isinstance(record, dict)
        records.append(record)
    return records


# All three committed vehicle records, in capture order: multi-reading
# carrier, externalIds-absent single-reading, terminal-page carrier.
ODOMETER_READINGS_VEHICLE_RECORDS: list[JsonObject] = _envelope_records(
    ODOMETER_READINGS_PAGE_RESPONSE
) + _envelope_records(ODOMETER_READINGS_TERMINAL_RESPONSE)
