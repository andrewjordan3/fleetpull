"""The synthetic GeoTab duty_status_logs feed fixture set (2026-07-21 shapes).

FULLY SYNTHETIC — no capture, no credentials: every value is invented in
the probe-settled shapes (the 2026-07-21 feed wave two census, DESIGN
§8: the census-total identity plus the partial-presence block —
``annotations`` 126/2,000, ``location`` 1,859/2,000, ``verifyDateTime``
765/2,000, and kin). The envelopes are the verified GETFEED shape —
``result: {data, toVersion}`` — as an ADVANCE page (full at the
fixtures' page size of 2) and a TERMINAL page (short), with
16-hex-lowercase version tokens per the machinery's synthetic-token
convention; the page size is 2 where production declares 50,000 (the
trips-capture ``resultsLimit: 3`` precedent). Coordinates are round
open-ocean values and the one address is an invention, both
corresponding to no real place.

Variant coverage promised to consumers: BOTH arms of the proven-mixed
``device`` and ``driver`` refs (objects on records 1 and 3, bare
strings on record 2), the wire ``{id}``-object ``annotations`` elements
(record 1) beside their absence (records 2 and 3), all THREE
``location`` states — the coordinate arm (record 1), absent (record 2),
and the ``formattedAddress`` address arm (record 3, the arm the
live-proof walk found beyond the 200-sample census) — the float
(record 1) and bare-int (record 3) arms of the mixed numerics
(``engineHours`` / ``odometer`` / ``distanceSinceValidCoordinates``),
``verifyDateTime`` present only on record 1, and two event dates
across the pages.

Shared by the DutyStatusLog model tests and the duty_status_logs
endpoint drive-through (the ``geotab_trips_capture`` precedent). The
JSON literals are the fixtures; the parsed objects beside them are
what tests consume.
"""

import json

from fleetpull.vocabulary import JsonObject, JsonValue
from tests.geotab_feed_pages import feed_records

# Synthetic: the advance page — 2 records (full at the fixtures' page
# size). Record 1 is the full record (object refs, annotations,
# location, float numeric arms, verifyDateTime); record 2 is the sparse
# record (bare-string refs, every partial-presence key absent).
DUTY_STATUS_LOGS_FEED_PAGE_1_RESPONSE_JSON: str = r"""
{
    "result": {
        "data": [
            {
                "annotations": [
                    {"id": "bAA31"},
                    {"id": "bAA32"}
                ],
                "dateTime": "2026-07-14T06:00:00.000Z",
                "deferralMinutes": 0,
                "deferralStatus": "None",
                "device": {
                    "id": "b8A1"
                },
                "distanceSinceValidCoordinates": 1.5,
                "driver": {
                    "id": "b4C11"
                },
                "editDateTime": "2026-07-14T06:05:00.000Z",
                "engineHours": 5321.5,
                "eventCode": 1,
                "eventRecordStatus": 1,
                "eventType": 1,
                "id": "b22a201",
                "isIgnored": false,
                "isTransitioning": false,
                "location": {
                    "location": {
                        "x": -140.25,
                        "y": 35.5
                    }
                },
                "malfunction": "None",
                "odometer": 482100.4,
                "origin": "Driver",
                "sequence": "1f",
                "state": "Active",
                "status": "ON",
                "verifyDateTime": "2026-07-14T07:00:00.000Z",
                "version": "00000000000022a1"
            },
            {
                "dateTime": "2026-07-14T14:30:00.000Z",
                "deferralMinutes": 0,
                "deferralStatus": "None",
                "device": "NoDeviceId",
                "driver": "UnknownDriverId",
                "editDateTime": "2026-07-14T14:30:05.000Z",
                "eventRecordStatus": 1,
                "id": "b22a202",
                "isIgnored": true,
                "isTransitioning": false,
                "malfunction": "None",
                "origin": "System",
                "state": "Active",
                "status": "OFF",
                "version": "00000000000022a2"
            }
        ],
        "toVersion": "00000000000022a2"
    },
    "jsonrpc": "2.0"
}"""

DUTY_STATUS_LOGS_FEED_PAGE_1_RESPONSE: dict[str, JsonValue] = json.loads(
    DUTY_STATUS_LOGS_FEED_PAGE_1_RESPONSE_JSON
)

# Synthetic: the terminal page — 1 record (short at the fixtures' page
# size), the second event date and the bare-int numeric arms.
DUTY_STATUS_LOGS_FEED_PAGE_2_RESPONSE_JSON: str = r"""
{
    "result": {
        "data": [
            {
                "dateTime": "2026-07-15T06:00:00.000Z",
                "deferralMinutes": 30,
                "deferralStatus": "Requested",
                "device": {
                    "id": "b8A3"
                },
                "distanceSinceValidCoordinates": 2,
                "driver": {
                    "id": "b4C25"
                },
                "editDateTime": "2026-07-15T06:00:10.000Z",
                "engineHours": 5340,
                "eventCode": 2,
                "eventRecordStatus": 1,
                "eventType": 3,
                "id": "b22a203",
                "isIgnored": false,
                "isTransitioning": true,
                "location": {
                    "address": {
                        "formattedAddress": "100 Example Rd, Testton, TS, USA"
                    }
                },
                "malfunction": "None",
                "odometer": 482400,
                "origin": "Driver",
                "sequence": "20",
                "state": "Active",
                "status": "D",
                "version": "00000000000022a3"
            }
        ],
        "toVersion": "00000000000022a3"
    },
    "jsonrpc": "2.0"
}"""

DUTY_STATUS_LOGS_FEED_PAGE_2_RESPONSE: dict[str, JsonValue] = json.loads(
    DUTY_STATUS_LOGS_FEED_PAGE_2_RESPONSE_JSON
)


# The three feed records, in stream order.
DUTY_STATUS_LOG_RECORDS: list[JsonObject] = [
    *feed_records(DUTY_STATUS_LOGS_FEED_PAGE_1_RESPONSE),
    *feed_records(DUTY_STATUS_LOGS_FEED_PAGE_2_RESPONSE),
]

# The designated full-shape record (page 1, first record): every modeled
# field present with the object-form refs -- the mechanical alias-trap
# test iterates the model's fields against it.
DUTY_STATUS_LOG_FULL_RECORD: JsonObject = DUTY_STATUS_LOG_RECORDS[0]

# The designated sparse record (page 1, second record): the bare-string
# device/driver arms and every partial-presence key absent.
DUTY_STATUS_LOG_SPARSE_RECORD: JsonObject = DUTY_STATUS_LOG_RECORDS[1]
