"""The synthetic GeoTab text_messages feed fixture set (2026-07-21 shapes).

FULLY SYNTHETIC — no capture, no credentials: every value is invented in
the probe-settled shapes (the 2026-07-21 feed wave three SCALE census,
DESIGN §8: 25,000 records; ``delivered``/``read`` on 24,995/25,000).
Every PII-risk string (``recipient``, a phone/address) is a synthetic
invention (a fake ``+1555…`` number). The envelopes are the verified
GETFEED shape — ``result: {data, toVersion}`` — as an ADVANCE page (full
at the fixtures' page size of 2) and a TERMINAL page (short), with
16-hex-lowercase FEED ``toVersion`` tokens per the machinery's
synthetic-token convention; the page size is 2 where production declares
50,000 (the trips-capture ``resultsLimit: 3`` precedent).

TextMessage carries NO per-record ``version`` key AND NO ``dateTime``
key (the append-only asymmetry) — no record here has either, and the
event time is ``sent``. Variant coverage promised to consumers: the
``delivered``/``read`` receipt datetimes PRESENT on records 1 and 3 and
ABSENT on record 2 (the optional absent arm), the object-only ``device``
ref defensively lifting a bare string (the model tests exercise the
lift), the nested ``messageContent`` block with its ``ids`` list[str],
and two event dates across the pages (on ``sent``, this vertical's
event-time field).

Synthetic id families are NEW b-prefixed families (``bTM2*`` records,
``bTV8*`` devices, ``bMC5*`` content ids) not shared with wave one or
two. Shared by the TextMessage model tests and the text_messages
endpoint drive-through. The JSON literals are the fixtures; the parsed
objects beside them are what tests consume.
"""

import json

from fleetpull.vocabulary import JsonObject, JsonValue
from tests.geotab_feed_pages import feed_records

# Synthetic: the advance page — 2 records (full at the fixtures' page
# size). Record 1 is the full record (delivered/read present); record 2
# omits the optional receipts.
TEXT_MESSAGES_FEED_PAGE_1_RESPONSE_JSON: str = r"""
{
    "result": {
        "data": [
            {
                "activeFrom": "2026-07-14T11:00:00.000Z",
                "activeTo": "2026-07-14T11:05:00.000Z",
                "delivered": "2026-07-14T11:01:30.000Z",
                "device": {
                    "id": "bTV801"
                },
                "id": "bTM201",
                "isDirectionToVehicle": true,
                "messageContent": {
                    "contentType": "Text",
                    "ids": ["bMC501", "bMC502"]
                },
                "messageSize": 128,
                "read": "2026-07-14T11:02:10.000Z",
                "recipient": "+15550000001",
                "sent": "2026-07-14T11:00:00.000Z"
            },
            {
                "activeFrom": "2026-07-14T19:00:00.000Z",
                "activeTo": "2026-07-14T19:05:00.000Z",
                "device": {
                    "id": "bTV802"
                },
                "id": "bTM202",
                "isDirectionToVehicle": false,
                "messageContent": {
                    "contentType": "Text",
                    "ids": ["bMC503"]
                },
                "messageSize": 64,
                "recipient": "+15550000002",
                "sent": "2026-07-14T19:00:00.000Z"
            }
        ],
        "toVersion": "0000000000002d02"
    },
    "jsonrpc": "2.0"
}"""

TEXT_MESSAGES_FEED_PAGE_1_RESPONSE: dict[str, JsonValue] = json.loads(
    TEXT_MESSAGES_FEED_PAGE_1_RESPONSE_JSON
)

# Synthetic: the terminal page — 1 record (short at the fixtures' page
# size), the second event date.
TEXT_MESSAGES_FEED_PAGE_2_RESPONSE_JSON: str = r"""
{
    "result": {
        "data": [
            {
                "activeFrom": "2026-07-15T09:30:00.000Z",
                "activeTo": "2026-07-15T09:35:00.000Z",
                "delivered": "2026-07-15T09:31:15.000Z",
                "device": {
                    "id": "bTV803"
                },
                "id": "bTM203",
                "isDirectionToVehicle": true,
                "messageContent": {
                    "contentType": "Text",
                    "ids": ["bMC504"]
                },
                "messageSize": 96,
                "read": "2026-07-15T09:32:00.000Z",
                "recipient": "+15550000003",
                "sent": "2026-07-15T09:30:00.000Z"
            }
        ],
        "toVersion": "0000000000002d03"
    },
    "jsonrpc": "2.0"
}"""

TEXT_MESSAGES_FEED_PAGE_2_RESPONSE: dict[str, JsonValue] = json.loads(
    TEXT_MESSAGES_FEED_PAGE_2_RESPONSE_JSON
)


# The three feed records, in stream order.
TEXT_MESSAGE_RECORDS: list[JsonObject] = [
    *feed_records(TEXT_MESSAGES_FEED_PAGE_1_RESPONSE),
    *feed_records(TEXT_MESSAGES_FEED_PAGE_2_RESPONSE),
]

# The designated full-shape record (page 1, first record): every modeled
# field present — the mechanical alias-trap test iterates the model's
# fields against it.
TEXT_MESSAGE_FULL_RECORD: JsonObject = TEXT_MESSAGE_RECORDS[0]

# The designated sparse record (page 1, second record): the optional
# delivered/read receipts absent.
TEXT_MESSAGE_SPARSE_RECORD: JsonObject = TEXT_MESSAGE_RECORDS[1]
