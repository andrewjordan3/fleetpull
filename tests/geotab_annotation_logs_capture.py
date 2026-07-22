"""The synthetic GeoTab annotation_logs feed fixture set (2026-07-21 shapes).

FULLY SYNTHETIC — no capture, no credentials: every value is invented in
the probe-settled shapes (the 2026-07-21 feed wave three SCALE census,
DESIGN §8: six keys, all census-total on 8,857 records, ``version``
included). The envelopes are the verified GETFEED shape — ``result:
{data, toVersion}`` — as an ADVANCE page (full at the fixtures' page
size of 2) and a TERMINAL page (short), with 16-hex-lowercase version
tokens per the machinery's synthetic-token convention; the page size is
2 where production declares 50,000 (the trips-capture ``resultsLimit:
3`` precedent).

Variant coverage promised to consumers: the object-only ``driver`` and
``dutyStatusLog`` refs each defensively lifting a bare string (the model
tests exercise the lift), the optional ``driver`` ABSENT on record 2
(the optional absent arm), and two event dates across the pages (on
``dateTime``, this vertical's event-time field). The ``dutyStatusLog``
ids are the BACK-REFERENCE to the ``duty_status_logs`` vertical.

Synthetic id families are NEW b-prefixed families (``bAL2*`` records,
``bDS4*`` duty-status logs, ``bDR7*`` drivers) not shared with wave one
or two. Shared by the AnnotationLog model tests and the annotation_logs
endpoint drive-through. The JSON literals are the fixtures; the parsed
objects beside them are what tests consume.
"""

import json

from fleetpull.vocabulary import JsonObject, JsonValue
from tests.geotab_feed_pages import feed_records

# Synthetic: the advance page — 2 records (full at the fixtures' page
# size). Record 1 is the full record (driver present); record 2 omits
# the optional driver.
ANNOTATION_LOGS_FEED_PAGE_1_RESPONSE_JSON: str = r"""
{
    "result": {
        "data": [
            {
                "comment": "Synthetic annotation comment one.",
                "dateTime": "2026-07-14T09:15:00.000Z",
                "driver": {
                    "id": "bDR701"
                },
                "dutyStatusLog": {
                    "id": "bDS401"
                },
                "id": "bAL201",
                "version": "0000000000002a01"
            },
            {
                "comment": "Synthetic annotation comment two.",
                "dateTime": "2026-07-14T20:30:00.000Z",
                "dutyStatusLog": {
                    "id": "bDS402"
                },
                "id": "bAL202",
                "version": "0000000000002a02"
            }
        ],
        "toVersion": "0000000000002a02"
    },
    "jsonrpc": "2.0"
}"""

ANNOTATION_LOGS_FEED_PAGE_1_RESPONSE: dict[str, JsonValue] = json.loads(
    ANNOTATION_LOGS_FEED_PAGE_1_RESPONSE_JSON
)

# Synthetic: the terminal page — 1 record (short at the fixtures' page
# size), the second event date.
ANNOTATION_LOGS_FEED_PAGE_2_RESPONSE_JSON: str = r"""
{
    "result": {
        "data": [
            {
                "comment": "Synthetic annotation comment three.",
                "dateTime": "2026-07-15T07:45:00.000Z",
                "driver": {
                    "id": "bDR725"
                },
                "dutyStatusLog": {
                    "id": "bDS403"
                },
                "id": "bAL203",
                "version": "0000000000002a03"
            }
        ],
        "toVersion": "0000000000002a03"
    },
    "jsonrpc": "2.0"
}"""

ANNOTATION_LOGS_FEED_PAGE_2_RESPONSE: dict[str, JsonValue] = json.loads(
    ANNOTATION_LOGS_FEED_PAGE_2_RESPONSE_JSON
)


# The three feed records, in stream order.
ANNOTATION_LOG_RECORDS: list[JsonObject] = [
    *feed_records(ANNOTATION_LOGS_FEED_PAGE_1_RESPONSE),
    *feed_records(ANNOTATION_LOGS_FEED_PAGE_2_RESPONSE),
]

# The designated full-shape record (page 1, first record): every modeled
# field present — the mechanical alias-trap test iterates the model's
# fields against it.
ANNOTATION_LOG_FULL_RECORD: JsonObject = ANNOTATION_LOG_RECORDS[0]

# The designated sparse record (page 1, second record): the optional
# driver absent.
ANNOTATION_LOG_SPARSE_RECORD: JsonObject = ANNOTATION_LOG_RECORDS[1]
