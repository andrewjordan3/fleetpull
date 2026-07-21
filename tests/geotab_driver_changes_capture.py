"""The synthetic GeoTab driver_changes feed fixture set (2026-07-21 shapes).

FULLY SYNTHETIC — no capture, no credentials: every value is invented in
the probe-settled shapes (the 2026-07-21 feed wave two census, DESIGN
§8: six keys, all census-total on 1,114/1,114, ``version`` included).
The envelopes are the verified GETFEED shape — ``result: {data,
toVersion}`` — as an ADVANCE page (full at the fixtures' page size of
2) and a TERMINAL page (short), with 16-hex-lowercase version tokens
per the machinery's synthetic-token convention; the page size is 2
where production declares 50,000 (the trips-capture ``resultsLimit: 3``
precedent).

Variant coverage promised to consumers: BOTH arms of the proven-mixed
``driver`` ref — the ``{id, isDriver}`` object on records 1 and 3, the
bare ``"UnknownDriverId"`` sentinel on record 2 — and two event dates
across the pages.

Shared by the DriverChange model tests and the driver_changes endpoint
drive-through (the ``geotab_trips_capture`` precedent). The JSON
literals are the fixtures; the parsed objects beside them are what
tests consume.
"""

import json

from fleetpull.vocabulary import JsonObject, JsonValue
from tests.geotab_feed_pages import feed_records

# Synthetic: the advance page — 2 records (full at the fixtures' page
# size). Record 1 carries the object driver arm; record 2 the bare
# sentinel arm.
DRIVER_CHANGES_FEED_PAGE_1_RESPONSE_JSON: str = r"""
{
    "result": {
        "data": [
            {
                "dateTime": "2026-07-14T05:55:00.000Z",
                "device": {
                    "id": "b8A1"
                },
                "driver": {
                    "id": "b4C11",
                    "isDriver": true
                },
                "id": "b23b201",
                "type": "Driver",
                "version": "00000000000023b1"
            },
            {
                "dateTime": "2026-07-14T18:40:00.000Z",
                "device": {
                    "id": "b8A1"
                },
                "driver": "UnknownDriverId",
                "id": "b23b202",
                "type": "None",
                "version": "00000000000023b2"
            }
        ],
        "toVersion": "00000000000023b2"
    },
    "jsonrpc": "2.0"
}"""

DRIVER_CHANGES_FEED_PAGE_1_RESPONSE: dict[str, JsonValue] = json.loads(
    DRIVER_CHANGES_FEED_PAGE_1_RESPONSE_JSON
)

# Synthetic: the terminal page — 1 record (short at the fixtures' page
# size), the second event date.
DRIVER_CHANGES_FEED_PAGE_2_RESPONSE_JSON: str = r"""
{
    "result": {
        "data": [
            {
                "dateTime": "2026-07-15T06:10:00.000Z",
                "device": {
                    "id": "b8A3"
                },
                "driver": {
                    "id": "b4C25",
                    "isDriver": true
                },
                "id": "b23b203",
                "type": "Driver",
                "version": "00000000000023b3"
            }
        ],
        "toVersion": "00000000000023b3"
    },
    "jsonrpc": "2.0"
}"""

DRIVER_CHANGES_FEED_PAGE_2_RESPONSE: dict[str, JsonValue] = json.loads(
    DRIVER_CHANGES_FEED_PAGE_2_RESPONSE_JSON
)


# The three feed records, in stream order.
DRIVER_CHANGE_RECORDS: list[JsonObject] = [
    *feed_records(DRIVER_CHANGES_FEED_PAGE_1_RESPONSE),
    *feed_records(DRIVER_CHANGES_FEED_PAGE_2_RESPONSE),
]

# The designated full-shape record (page 1, first record): every modeled
# field present with the object-form driver -- the mechanical alias-trap
# test iterates the model's fields against it.
DRIVER_CHANGE_FULL_RECORD: JsonObject = DRIVER_CHANGE_RECORDS[0]

# The designated sentinel record (page 1, second record): the bare
# UnknownDriverId driver arm.
DRIVER_CHANGE_SENTINEL_RECORD: JsonObject = DRIVER_CHANGE_RECORDS[1]
