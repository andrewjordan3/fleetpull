"""The synthetic GeoTab log_records feed fixture set (2026-07-21 shapes).

FULLY SYNTHETIC — no capture, no credentials: every value is invented in
the probe-settled shapes (the 2026-07-21 feed wave census, DESIGN §8:
2,000/2,000 records carried all six keys). The envelopes are the
verified GETFEED shape — ``result: {data, toVersion}`` — as an ADVANCE
page (full at the fixtures' page size of 2, so the decoder continues)
and a TERMINAL page (short, so the decoder stops), with 16-hex-lowercase
version tokens per the machinery's synthetic-token convention. The
fixtures' page size is 2 where production declares 50,000 — the walk's
parameter, not the mechanism (the trips-capture ``resultsLimit: 3``
precedent).

Variant coverage promised to consumers: page 1's records span TWO event
dates (a one-page multi-partition append), page 2's record shares
page 1's second date (a partition accruing parts across pages), and
``speed`` rides the observed bare-int arm on every record.

Shared by the LogRecord model tests and the log_records endpoint
drive-through — a multi-consumer fixture set, so it lives in one helper
module under ``tests/`` (the ``geotab_trips_capture`` precedent). The
JSON literals are the fixtures; the parsed objects beside them are what
tests consume.
"""

import json

from fleetpull.vocabulary import JsonObject, JsonValue
from tests.geotab_feed_pages import feed_records

# Synthetic: the advance page — 2 records (full at the fixtures' page
# size), two distinct event dates, toVersion continuing the walk.
LOG_RECORDS_FEED_PAGE_1_RESPONSE_JSON: str = r"""
{
    "result": {
        "data": [
            {
                "dateTime": "2026-07-14T08:00:01.000Z",
                "device": {
                    "id": "b8E2"
                },
                "id": "b14a101",
                "latitude": 40.1000001,
                "longitude": -100.1000001,
                "speed": 63
            },
            {
                "dateTime": "2026-07-15T09:30:02.000Z",
                "device": {
                    "id": "b8E7"
                },
                "id": "b14a102",
                "latitude": 40.2000002,
                "longitude": -100.2000002,
                "speed": 0
            }
        ],
        "toVersion": "00000000000014a1"
    },
    "jsonrpc": "2.0"
}"""

LOG_RECORDS_FEED_PAGE_1_RESPONSE: dict[str, JsonValue] = json.loads(
    LOG_RECORDS_FEED_PAGE_1_RESPONSE_JSON
)

# Synthetic: the terminal page — 1 record (short at the fixtures' page
# size), on page 1's second date.
LOG_RECORDS_FEED_PAGE_2_RESPONSE_JSON: str = r"""
{
    "result": {
        "data": [
            {
                "dateTime": "2026-07-15T09:30:03.000Z",
                "device": {
                    "id": "b8E2"
                },
                "id": "b14a103",
                "latitude": 40.3000003,
                "longitude": -100.3000003,
                "speed": 97
            }
        ],
        "toVersion": "00000000000014a2"
    },
    "jsonrpc": "2.0"
}"""

LOG_RECORDS_FEED_PAGE_2_RESPONSE: dict[str, JsonValue] = json.loads(
    LOG_RECORDS_FEED_PAGE_2_RESPONSE_JSON
)


# The three feed records, in stream order.
LOG_RECORD_RECORDS: list[JsonObject] = [
    *feed_records(LOG_RECORDS_FEED_PAGE_1_RESPONSE),
    *feed_records(LOG_RECORDS_FEED_PAGE_2_RESPONSE),
]

# The designated full-shape record (page 1, first record): every modeled
# field present -- the mechanical alias-trap test iterates the model's
# fields against it.
LOG_RECORD_FULL_RECORD: JsonObject = LOG_RECORD_RECORDS[0]
