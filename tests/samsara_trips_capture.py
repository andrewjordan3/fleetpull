"""The committed Samsara trips capture set (2026-07-20 probe session).

Two FULLY SYNTHETIC trip records shaped by the live census (725 trips
across 60 vehicles, zero nulls anywhere) inside the captured
``{"trips": [...]}`` envelope of ``GET /v1/fleet/trips`` -- the legacy
v1-only surface (the modern candidates 404); one unpaginated response
per (vehicle, window). The maximal variant carries both matched
address/geofence blocks (``startAddress`` 177/725, ``endAddress``
185/725 in census); the minimal variant carries neither, plus
``driverId: 0`` (the UNASSIGNED sentinel, 110/725) and the empty
``assetIds``/``codriverIds`` lists -- the ONLY list shape observed in
all 725 records. Beside the envelope sit the two captured HTTP 400
bodies: the missing-``vehicleId`` rejection (the parameter is REQUIRED)
and the 90-day range-cap rejection (a 90-day window succeeded; 91 was
rejected).

Unlike the sibling capture sets, no record values here are scrubbed
live values -- every identifier, address, coordinate, odometer,
distance, and timestamp is synthetic outright (the address strings and
coordinates are PII-adjacent). What IS verbatim wire truth: the
envelope key, the camelCase key set, the epoch-MILLISECOND int shape of
``startMs``/``endMs``, the bare-int unit fields and int-id family, the
``{address, id, name}`` address-block shape, the empties-only list
shape, the 0 driver sentinel -- and the two 400 bodies, which are
TEXT/PLAIN rpc-error strings exactly as captured (the v1 posture: the
known Samsara plain-string-body rule extends beyond 5xx to these 400s).

Consumed by the Trip model tests, the trips endpoint tests, and the
Samsara classifier test -- kept as a helper module under ``tests/`` so
consumers share one capture set (the ``samsara_vehicles_capture``
precedent). The raw JSON literal is the envelope; the parsed objects
beside it are what tests consume.
"""

import json

from fleetpull.vocabulary import JsonObject, JsonValue

# The captured envelope shape (2026-07-20): a top-level "trips" list and
# NOTHING else -- no pagination object of any kind. Two synthetic
# records: maximal (both address blocks) then minimal (no address
# blocks, the 0 driver sentinel, the empties-only lists).
TRIPS_RESPONSE_JSON: str = r"""
{
    "trips": [
        {
            "startMs": 1767225600123,
            "endMs": 1767229245456,
            "driverId": 7100001,
            "distanceMeters": 52000,
            "fuelConsumedMl": 21000,
            "tollMeters": 1200,
            "startOdometer": 240001000,
            "endOdometer": 240053000,
            "startLocation": "100 Example St, Example City, CA",
            "endLocation": "200 Example Ave, Example City, CA",
            "startCoordinates": {
                "latitude": 34.0001,
                "longitude": -118.0001
            },
            "endCoordinates": {
                "latitude": 34.0501,
                "longitude": -118.0501
            },
            "startAddress": {
                "address": "100 Example St, Example City, CA",
                "id": 8800001,
                "name": "Example Yard"
            },
            "endAddress": {
                "address": "200 Example Ave, Example City, CA",
                "id": 8800002,
                "name": "Example Terminal"
            },
            "assetIds": [],
            "codriverIds": []
        },
        {
            "startMs": 1767312000000,
            "endMs": 1767315600000,
            "driverId": 0,
            "distanceMeters": 18000,
            "fuelConsumedMl": 6000,
            "tollMeters": 0,
            "startOdometer": 118000000,
            "endOdometer": 118018000,
            "startLocation": "300 Example Blvd, Example City, CA",
            "endLocation": "400 Example Way, Example City, CA",
            "startCoordinates": {
                "latitude": 34.1001,
                "longitude": -118.1001
            },
            "endCoordinates": {
                "latitude": 34.1501,
                "longitude": -118.1501
            },
            "assetIds": [],
            "codriverIds": []
        }
    ]
}"""

TRIPS_RESPONSE: dict[str, JsonValue] = json.loads(TRIPS_RESPONSE_JSON)

# Captured: the HTTP 400 body when vehicleId is omitted (2026-07-20) --
# a TEXT/PLAIN rpc-error string, not JSON. vehicleId is REQUIRED: there
# is no fleet-wide request shape on this surface, which is what makes
# the binding a per-vehicle roster fan-out.
TRIPS_MISSING_VEHICLE_ID_400_BODY: str = (
    'rpc error: code = InvalidArgument desc = Missing parameter: vehicleId'
)

# Captured: the HTTP 400 body for a window wider than 90 days
# (2026-07-20; a 90-day window succeeded -- 702 trips, one page; 91+
# days returns this). The same text/plain rpc-error posture: loud,
# never a silent truncation.
TRIPS_RANGE_CAP_400_BODY: str = (
    'rpc error: code = InvalidArgument desc = '
    'requested time range cannot exceed 90 days'
)


def _envelope_records(envelope: dict[str, JsonValue]) -> list[JsonObject]:
    result = envelope['trips']
    assert isinstance(result, list)
    records: list[JsonObject] = []
    for record in result:
        assert isinstance(record, dict)
        records.append(record)
    return records


# Both committed records, in capture order: maximal, then minimal.
TRIP_RECORDS: list[JsonObject] = _envelope_records(TRIPS_RESPONSE)
