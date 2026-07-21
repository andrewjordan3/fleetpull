"""The committed Samsara gps_readings capture set (2026-07-20 probe session).

Three FULLY SYNTHETIC vehicle records shaped by the live census of
``GET /fleet/vehicles/stats/history`` with ``types=gps`` (a
2,512-reading sample over 8 cursor pages of a 569-vehicle 24-hour
walk; per-vehicle keys exactly ``id``/``name``/``externalIds``/``gps``;
series keys ``time``/``latitude``/``longitude``/``headingDegrees``/
``speedMilesPerHour``/``isEcuSpeed``/``reverseGeo`` on every sampled
reading, ``address`` on 401/2,512), arranged as a two-page cursor walk
whose pages carry DISJOINT vehicle ids -- the probe-proven
vehicle-axis cursor (three consecutive live pages showed zero
vehicle-id overlap). The variants exercise every decoder and model
arm: a multi-reading vehicle whose readings carry the address-book
reference on one reading and not the other (plus the int-shaped
``speedMilesPerHour`` -- the wire mixes int and float, modeled float),
a single-reading vehicle with ``externalIds`` ABSENT (a synthetic
variant -- the censused page carried the block 74/74, but one page is
not a whole-population oath and the vehicles surface shows exactly
this variance; downstream it proves the serial/vin omit-absent
posture), and a terminal-page single-reading address carrier.

No record values here are scrubbed live values -- every id, name,
serial, VIN-shaped string, coordinate, heading, speed, location
string, address id, and timestamp is synthetic outright (location
strings and coordinates are PII-adjacent; the trips precedent). What
IS verbatim wire truth: the ``data`` + ``pagination {endCursor,
hasNextPage}`` envelope, the per-vehicle key set with the literal
DOTTED ``externalIds`` keys (``samsara.serial``/``samsara.vin``), the
millisecond RFC3339 ``time`` shape, the series key set with its
int-heading / mixed-int-float-speed / bool-``isEcuSpeed`` types, the
``reverseGeo {formattedLocation}`` and ``address {id, name}`` block
shapes, and the terminal ``hasNextPage: false`` beside an empty-string
``endCursor``.

Consumed by the GpsReading model tests (which unnest through the
production series decoder -- the model mirrors the flat post-decoder
record) and the gps_readings endpoint tests -- kept as a helper module
under ``tests/`` so consumers share one capture set (the
``samsara_vehicles_capture`` precedent). The raw JSON literals are the
captures; the parsed objects beside them are what tests consume.
"""

import json

from fleetpull.vocabulary import JsonObject, JsonValue

# A continuation page: the multi-reading carrier (address ref on the
# first reading, absent on the second; float- then int-shaped speed)
# beside the externalIds-ABSENT single-reading vehicle. Reading times
# sit strictly inside a 12:00-13:00Z window -- the probe's
# [startTime, endTime) anchoring evidence.
GPS_READINGS_PAGE_RESPONSE_JSON: str = r"""
{
    "data": [
        {
            "id": "281474980000011",
            "name": "Truck 201",
            "externalIds": {
                "samsara.serial": "GSYNTH00001A",
                "samsara.vin": "SYNTH000000000011"
            },
            "gps": [
                {
                    "time": "2026-01-01T12:00:05.100Z",
                    "latitude": 33.1001,
                    "longitude": -96.1001,
                    "headingDegrees": 270,
                    "speedMilesPerHour": 58.4,
                    "isEcuSpeed": true,
                    "reverseGeo": {
                        "formattedLocation": "100 Example St, Example City, TX"
                    },
                    "address": {
                        "id": "88000011",
                        "name": "Depot North"
                    }
                },
                {
                    "time": "2026-01-01T12:30:10.750Z",
                    "latitude": 33.1501,
                    "longitude": -96.1501,
                    "headingDegrees": 0,
                    "speedMilesPerHour": 0,
                    "isEcuSpeed": false,
                    "reverseGeo": {
                        "formattedLocation": "200 Example Ave, Example City, TX"
                    }
                }
            ]
        },
        {
            "id": "281474980000012",
            "name": "Truck 202",
            "gps": [
                {
                    "time": "2026-01-01T12:10:20.300Z",
                    "latitude": 33.2001,
                    "longitude": -96.2001,
                    "headingDegrees": 95,
                    "speedMilesPerHour": 41.7,
                    "isEcuSpeed": true,
                    "reverseGeo": {
                        "formattedLocation": "300 Example Blvd, Example City, TX"
                    }
                }
            ]
        }
    ],
    "pagination": {
        "endCursor": "00000000-0000-0000-0000-000000000051",
        "hasNextPage": true
    }
}"""

GPS_READINGS_PAGE_RESPONSE: dict[str, JsonValue] = json.loads(
    GPS_READINGS_PAGE_RESPONSE_JSON
)

# The terminal page shape -- hasNextPage false beside an empty-string
# endCursor (the provider-wide cursor contract) -- carrying a vehicle
# id DISJOINT from page one's (the vehicle-axis cursor, proven live)
# and one more address-carrying reading.
GPS_READINGS_TERMINAL_RESPONSE_JSON: str = r"""
{
    "data": [
        {
            "id": "281474980000013",
            "name": "Truck 203",
            "externalIds": {
                "samsara.serial": "GSYNTH00001C",
                "samsara.vin": "SYNTH000000000013"
            },
            "gps": [
                {
                    "time": "2026-01-01T12:45:33.900Z",
                    "latitude": 33.3001,
                    "longitude": -96.3001,
                    "headingDegrees": 182,
                    "speedMilesPerHour": 12.3,
                    "isEcuSpeed": true,
                    "reverseGeo": {
                        "formattedLocation": "400 Example Pkwy, Example City, TX"
                    },
                    "address": {
                        "id": "88000013",
                        "name": "Yard South"
                    }
                }
            ]
        }
    ],
    "pagination": {
        "endCursor": "",
        "hasNextPage": false
    }
}"""

GPS_READINGS_TERMINAL_RESPONSE: dict[str, JsonValue] = json.loads(
    GPS_READINGS_TERMINAL_RESPONSE_JSON
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
GPS_READINGS_VEHICLE_RECORDS: list[JsonObject] = _envelope_records(
    GPS_READINGS_PAGE_RESPONSE
) + _envelope_records(GPS_READINGS_TERMINAL_RESPONSE)
