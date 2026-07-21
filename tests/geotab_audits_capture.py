"""The synthetic GeoTab audits feed fixture set (2026-07-21 shapes).

FULLY SYNTHETIC — no capture, no credentials: every value is invented in
the probe-settled shapes (the 2026-07-21 feed wave three SCALE census,
DESIGN §8: six keys, all census-total on 20,000 records, ``version``
included). Every PII-risk string (``userName``, a username) is a
synthetic invention. The envelopes are the verified GETFEED shape —
``result: {data, toVersion}`` — as an ADVANCE page (full at the
fixtures' page size of 2) and a TERMINAL page (short), with
16-hex-lowercase version tokens per the machinery's synthetic-token
convention; the page size is 2 where production declares 50,000 (the
trips-capture ``resultsLimit: 3`` precedent).

Audit is the simplest vertical — NO reference fields. Variant coverage
promised to consumers: the optional ``comment`` ABSENT on record 2 (the
optional absent arm), and two event dates across the pages (on
``dateTime``, this vertical's event-time field).

Synthetic id families are a NEW b-prefixed family (``bAU2*`` records)
not shared with wave one or two. Shared by the Audit model tests and the
audits endpoint drive-through. The JSON literals are the fixtures; the
parsed objects beside them are what tests consume.
"""

import json

from fleetpull.vocabulary import JsonObject, JsonValue
from tests.geotab_feed_pages import feed_records

# Synthetic: the advance page — 2 records (full at the fixtures' page
# size). Record 1 is the full record (comment present); record 2 omits
# the optional comment.
AUDITS_FEED_PAGE_1_RESPONSE_JSON: str = r"""
{
    "result": {
        "data": [
            {
                "comment": "Synthetic audit comment one.",
                "dateTime": "2026-07-14T10:05:00.000Z",
                "id": "bAU201",
                "name": "Synthetic Rule Alpha",
                "userName": "user.synthetic001",
                "version": "0000000000002c01"
            },
            {
                "dateTime": "2026-07-14T21:20:00.000Z",
                "id": "bAU202",
                "name": "Synthetic Zone Beta",
                "userName": "user.synthetic002",
                "version": "0000000000002c02"
            }
        ],
        "toVersion": "0000000000002c02"
    },
    "jsonrpc": "2.0"
}"""

AUDITS_FEED_PAGE_1_RESPONSE: dict[str, JsonValue] = json.loads(
    AUDITS_FEED_PAGE_1_RESPONSE_JSON
)

# Synthetic: the terminal page — 1 record (short at the fixtures' page
# size), the second event date.
AUDITS_FEED_PAGE_2_RESPONSE_JSON: str = r"""
{
    "result": {
        "data": [
            {
                "comment": "Synthetic audit comment three.",
                "dateTime": "2026-07-15T08:35:00.000Z",
                "id": "bAU203",
                "name": "Synthetic Group Gamma",
                "userName": "user.synthetic025",
                "version": "0000000000002c03"
            }
        ],
        "toVersion": "0000000000002c03"
    },
    "jsonrpc": "2.0"
}"""

AUDITS_FEED_PAGE_2_RESPONSE: dict[str, JsonValue] = json.loads(
    AUDITS_FEED_PAGE_2_RESPONSE_JSON
)


# The three feed records, in stream order.
AUDIT_RECORDS: list[JsonObject] = [
    *feed_records(AUDITS_FEED_PAGE_1_RESPONSE),
    *feed_records(AUDITS_FEED_PAGE_2_RESPONSE),
]

# The designated full-shape record (page 1, first record): every modeled
# field present — the mechanical alias-trap test iterates the model's
# fields against it.
AUDIT_FULL_RECORD: JsonObject = AUDIT_RECORDS[0]

# The designated sparse record (page 1, second record): the optional
# comment absent.
AUDIT_SPARSE_RECORD: JsonObject = AUDIT_RECORDS[1]
