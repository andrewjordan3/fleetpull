"""The synthetic GeoTab media_files feed fixture set (2026-07-21 shapes).

FULLY SYNTHETIC — no capture, no credentials: every value is invented in
the probe-settled shapes (the 2026-07-21 feed wave three SCALE census,
DESIGN §8: 55 records over a 730-day window — genuinely thin data;
``device`` PROVEN mixed 42 str / 13 object, ``driver`` string-only,
``metaData``/``tags``/``thumbnails`` EMPTY on all 55). Every PII-risk
string (``name``) is a synthetic invention. The envelopes are the
verified GETFEED shape — ``result: {data, toVersion}`` — as an ADVANCE
page (full at the fixtures' page size of 2) and a TERMINAL page (short),
with 16-hex-lowercase version tokens per the machinery's synthetic-token
convention; the page size is 2 where production declares 50,000 (the
trips-capture ``resultsLimit: 3`` precedent).

MediaFile carries NO ``dateTime`` key — the event time is ``fromDate``.
Variant coverage promised to consumers: BOTH arms of the proven-mixed
``device`` ref — the ``{id}`` object on records 1 and 3, the bare string
on record 2 — the string-only ``driver`` arm on every record, the three
EMPTY-container exclusions (``metaData`` ``{}``, ``tags`` ``[]``,
``thumbnails`` ``[]``) on every record (the model tests pin that a
POPULATED container is still absorbed), and two event dates across the
pages (on ``fromDate``, this vertical's event-time field).

Synthetic id families are NEW b-prefixed families (``bMF2*`` records,
``bMV9*`` devices, ``bMD3*`` drivers) not shared with wave one or two.
Shared by the MediaFile model tests and the media_files endpoint
drive-through. The JSON literals are the fixtures; the parsed objects
beside them are what tests consume.
"""

import json

from fleetpull.vocabulary import JsonObject, JsonValue
from tests.geotab_feed_pages import feed_records

# Synthetic: the advance page — 2 records (full at the fixtures' page
# size). Record 1 is the full record (object device arm); record 2
# carries the bare-string device arm (the proven mixed ref's other arm).
MEDIA_FILES_FEED_PAGE_1_RESPONSE_JSON: str = r"""
{
    "result": {
        "data": [
            {
                "device": {
                    "id": "bMV901"
                },
                "driver": "bMD301",
                "fromDate": "2026-07-14T13:00:00.000Z",
                "id": "bMF201",
                "mediaType": "Image",
                "metaData": {},
                "name": "synthetic-media-001.jpg",
                "solutionId": "bSolutionSynthetic01",
                "status": "Available",
                "tags": [],
                "thumbnails": [],
                "toDate": "2026-07-14T13:00:05.000Z",
                "version": "0000000000002e01"
            },
            {
                "device": "bMV902",
                "driver": "bMD302",
                "fromDate": "2026-07-14T22:15:00.000Z",
                "id": "bMF202",
                "mediaType": "Video",
                "metaData": {},
                "name": "synthetic-media-002.mp4",
                "solutionId": "bSolutionSynthetic01",
                "status": "Available",
                "tags": [],
                "thumbnails": [],
                "toDate": "2026-07-14T22:15:30.000Z",
                "version": "0000000000002e02"
            }
        ],
        "toVersion": "0000000000002e02"
    },
    "jsonrpc": "2.0"
}"""

MEDIA_FILES_FEED_PAGE_1_RESPONSE: dict[str, JsonValue] = json.loads(
    MEDIA_FILES_FEED_PAGE_1_RESPONSE_JSON
)

# Synthetic: the terminal page — 1 record (short at the fixtures' page
# size), the object device arm and the second event date.
MEDIA_FILES_FEED_PAGE_2_RESPONSE_JSON: str = r"""
{
    "result": {
        "data": [
            {
                "device": {
                    "id": "bMV903"
                },
                "driver": "bMD325",
                "fromDate": "2026-07-15T10:45:00.000Z",
                "id": "bMF203",
                "mediaType": "Image",
                "metaData": {},
                "name": "synthetic-media-003.jpg",
                "solutionId": "bSolutionSynthetic01",
                "status": "Available",
                "tags": [],
                "thumbnails": [],
                "toDate": "2026-07-15T10:45:05.000Z",
                "version": "0000000000002e03"
            }
        ],
        "toVersion": "0000000000002e03"
    },
    "jsonrpc": "2.0"
}"""

MEDIA_FILES_FEED_PAGE_2_RESPONSE: dict[str, JsonValue] = json.loads(
    MEDIA_FILES_FEED_PAGE_2_RESPONSE_JSON
)


# The three feed records, in stream order.
MEDIA_FILE_RECORDS: list[JsonObject] = [
    *feed_records(MEDIA_FILES_FEED_PAGE_1_RESPONSE),
    *feed_records(MEDIA_FILES_FEED_PAGE_2_RESPONSE),
]

# The designated full-shape record (page 1, first record): every modeled
# field present with the OBJECT device arm — the mechanical alias-trap
# test iterates the model's fields against it.
MEDIA_FILE_FULL_RECORD: JsonObject = MEDIA_FILE_RECORDS[0]

# The designated string-device record (page 1, second record): the bare
# string device arm (the proven mixed ref's other arm).
MEDIA_FILE_STRING_DEVICE_RECORD: JsonObject = MEDIA_FILE_RECORDS[1]
