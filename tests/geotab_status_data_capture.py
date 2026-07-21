"""The synthetic GeoTab status_data feed fixture set (2026-07-21 shapes).

FULLY SYNTHETIC — no capture, no credentials: every value is invented in
the probe-settled shapes (the 2026-07-21 feed wave census, DESIGN §8:
2,000/2,000 records carried all seven keys, ``version`` included — the
asymmetry against LogRecord). The envelopes are the verified GETFEED
shape — ``result: {data, toVersion}`` — as an ADVANCE page (full at the
fixtures' page size of 2) and a TERMINAL page (short), with
16-hex-lowercase version tokens per the machinery's synthetic-token
convention; the page size is 2 where production declares 50,000 (the
trips-capture ``resultsLimit: 3`` precedent).

Variant coverage promised to consumers: ``data`` rides BOTH observed
numeric arms (a float on page 1's first record, a bare int on its
second), the records span two event dates, and every record carries its
per-record ``version``.

Shared by the StatusData model tests and the status_data endpoint
drive-through (the ``geotab_trips_capture`` precedent). The JSON
literals are the fixtures; the parsed objects beside them are what
tests consume.
"""

import json

from fleetpull.vocabulary import JsonObject, JsonValue
from tests.geotab_feed_pages import feed_records

# Synthetic: the advance page — 2 records (full at the fixtures' page
# size), the float and int `data` arms, two event dates.
STATUS_DATA_FEED_PAGE_1_RESPONSE_JSON: str = r"""
{
    "result": {
        "data": [
            {
                "controller": "ControllerNoneId",
                "data": 87.5,
                "dateTime": "2026-07-14T10:15:00.000Z",
                "device": {
                    "id": "b8E2"
                },
                "diagnostic": {
                    "id": "DiagnosticEngineSpeedId"
                },
                "id": "b15b201",
                "version": "00000000000015b1"
            },
            {
                "controller": "ControllerNoneId",
                "data": 1200,
                "dateTime": "2026-07-15T11:45:30.000Z",
                "device": {
                    "id": "b8E7"
                },
                "diagnostic": {
                    "id": "DiagnosticDeviceTotalFuelId"
                },
                "id": "b15b202",
                "version": "00000000000015b2"
            }
        ],
        "toVersion": "00000000000015b2"
    },
    "jsonrpc": "2.0"
}"""

STATUS_DATA_FEED_PAGE_1_RESPONSE: dict[str, JsonValue] = json.loads(
    STATUS_DATA_FEED_PAGE_1_RESPONSE_JSON
)

# Synthetic: the terminal page — 1 record (short at the fixtures' page
# size).
STATUS_DATA_FEED_PAGE_2_RESPONSE_JSON: str = r"""
{
    "result": {
        "data": [
            {
                "controller": {"id": "ControllerObdPowertrainId"},
                "data": 0,
                "dateTime": "2026-07-15T11:45:31.000Z",
                "device": {
                    "id": "b8E2"
                },
                "diagnostic": {
                    "id": "DiagnosticIgnitionId"
                },
                "id": "b15b203",
                "version": "00000000000015b3"
            }
        ],
        "toVersion": "00000000000015b3"
    },
    "jsonrpc": "2.0"
}"""

STATUS_DATA_FEED_PAGE_2_RESPONSE: dict[str, JsonValue] = json.loads(
    STATUS_DATA_FEED_PAGE_2_RESPONSE_JSON
)


# The three feed records, in stream order.
STATUS_DATA_RECORDS: list[JsonObject] = [
    *feed_records(STATUS_DATA_FEED_PAGE_1_RESPONSE),
    *feed_records(STATUS_DATA_FEED_PAGE_2_RESPONSE),
]

# The designated full-shape record (page 1, first record): every modeled
# field present -- the mechanical alias-trap test iterates the model's
# fields against it.
STATUS_DATA_FULL_RECORD: JsonObject = STATUS_DATA_RECORDS[0]
