"""The synthetic GeoTab dvir_logs feed fixture set (2026-07-21 shapes).

FULLY SYNTHETIC — no capture, no credentials: every value is invented in
the probe-settled shapes (the 2026-07-21 feed wave two census, DESIGN
§8: the census-total identity plus the partial-presence block — the
``device``/``engineHours``/``odometer`` trio 205/500, ``trailer``
295/500, ``location`` 496/500 — and ``defectList.children`` EMPTY on
all 200 sampled ``defectList`` nodes, the documented exclusion). The
envelopes are the
verified GETFEED shape — ``result: {data, toVersion}`` — as an ADVANCE
page (full at the fixtures' page size of 2) and a TERMINAL page
(short), with 16-hex-lowercase version tokens per the machinery's
synthetic-token convention; the page size is 2 where production
declares 50,000 (the trips-capture ``resultsLimit: 3`` precedent).
Coordinates are round open-ocean values corresponding to no real
place; every name and address is an invention.

Variant coverage promised to consumers: the ``device`` trio and
``trailer`` and ``location`` present (records 1 and 3) and absent
(record 2), ``engineHours`` a bare int on both its carriers (the
census's int-only observation — the model's float lift is the
cross-surface decision), ``defectList`` riding ``children: []`` raw on
every record (unmodeled — the model ignores it), and two event dates
across the pages.

Shared by the DvirLog model tests and the dvir_logs endpoint
drive-through (the ``geotab_trips_capture`` precedent). The JSON
literals are the fixtures; the parsed objects beside them are what
tests consume.
"""

import json

from fleetpull.vocabulary import JsonObject, JsonValue
from tests.geotab_feed_pages import feed_records

# Synthetic: the advance page — 2 records (full at the fixtures' page
# size). Record 1 is the full record (the device trio, trailer,
# location); record 2 is the sparse record (all of them absent).
DVIR_LOGS_FEED_PAGE_1_RESPONSE_JSON: str = r"""
{
    "result": {
        "data": [
            {
                "authorityAddress": "100 Example Plaza, Sampletown",
                "authorityName": "Synthetic Carrier Authority",
                "certifyRemark": "Defects corrected.",
                "dateTime": "2026-07-14T05:45:00.000Z",
                "defectList": {
                    "children": [],
                    "id": "bDL41",
                    "name": "Truck Defects"
                },
                "device": {
                    "id": "b8A1"
                },
                "driver": {
                    "id": "b4C11"
                },
                "driverRemark": "No issues observed.",
                "duration": "00:12:30",
                "engineHours": 5320,
                "id": "b24c201",
                "isInspectedByDriver": true,
                "location": {
                    "location": {
                        "x": -140.25,
                        "y": 35.5
                    }
                },
                "logType": "PreTrip",
                "odometer": 482099.7,
                "trailer": {
                    "id": "b9C41"
                },
                "version": "00000000000024c1"
            },
            {
                "authorityAddress": "100 Example Plaza, Sampletown",
                "authorityName": "Synthetic Carrier Authority",
                "certifyRemark": "",
                "dateTime": "2026-07-14T18:15:00.000Z",
                "defectList": {
                    "children": [],
                    "id": "bDL42",
                    "name": "Trailer Defects"
                },
                "driver": {
                    "id": "b4C25"
                },
                "driverRemark": "Brake light inoperative.",
                "duration": "00:08:00",
                "id": "b24c202",
                "isInspectedByDriver": true,
                "logType": "PostTrip",
                "version": "00000000000024c2"
            }
        ],
        "toVersion": "00000000000024c2"
    },
    "jsonrpc": "2.0"
}"""

DVIR_LOGS_FEED_PAGE_1_RESPONSE: dict[str, JsonValue] = json.loads(
    DVIR_LOGS_FEED_PAGE_1_RESPONSE_JSON
)

# Synthetic: the terminal page — 1 record (short at the fixtures' page
# size), the second event date with the device trio and trailer back.
DVIR_LOGS_FEED_PAGE_2_RESPONSE_JSON: str = r"""
{
    "result": {
        "data": [
            {
                "authorityAddress": "100 Example Plaza, Sampletown",
                "authorityName": "Synthetic Carrier Authority",
                "certifyRemark": "Reviewed and certified.",
                "dateTime": "2026-07-15T05:50:00.000Z",
                "defectList": {
                    "children": [],
                    "id": "bDL41",
                    "name": "Truck Defects"
                },
                "device": {
                    "id": "b8A3"
                },
                "driver": {
                    "id": "b4C11"
                },
                "driverRemark": "",
                "duration": "00:10:45",
                "engineHours": 5341,
                "id": "b24c203",
                "isInspectedByDriver": false,
                "location": {
                    "location": {
                        "x": -140.5,
                        "y": 35.75
                    }
                },
                "logType": "PreTrip",
                "odometer": 482410.2,
                "trailer": {
                    "id": "b9C55"
                },
                "version": "00000000000024c3"
            }
        ],
        "toVersion": "00000000000024c3"
    },
    "jsonrpc": "2.0"
}"""

DVIR_LOGS_FEED_PAGE_2_RESPONSE: dict[str, JsonValue] = json.loads(
    DVIR_LOGS_FEED_PAGE_2_RESPONSE_JSON
)


# The three feed records, in stream order.
DVIR_LOG_RECORDS: list[JsonObject] = [
    *feed_records(DVIR_LOGS_FEED_PAGE_1_RESPONSE),
    *feed_records(DVIR_LOGS_FEED_PAGE_2_RESPONSE),
]

# The designated full-shape record (page 1, first record): every modeled
# field present -- the mechanical alias-trap test iterates the model's
# fields against it.
DVIR_LOG_FULL_RECORD: JsonObject = DVIR_LOG_RECORDS[0]

# The designated sparse record (page 1, second record): the device trio,
# trailer, and location all absent.
DVIR_LOG_SPARSE_RECORD: JsonObject = DVIR_LOG_RECORDS[1]
