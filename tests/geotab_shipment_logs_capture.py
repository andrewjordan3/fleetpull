"""The synthetic GeoTab shipment_logs feed fixture set (2026-07-21 shapes).

FULLY SYNTHETIC — no capture, no credentials: every value is invented in
the probe-settled shapes (the 2026-07-21 feed wave three SCALE census,
DESIGN §8: ten keys, all census-total on 2,771 records, ``version``
included). Every PII-risk string (``shipperName``, a company name) is a
synthetic invention. The envelopes are the verified GETFEED shape —
``result: {data, toVersion}`` — as an ADVANCE page (full at the
fixtures' page size of 2) and a TERMINAL page (short), with
16-hex-lowercase version tokens per the machinery's synthetic-token
convention; the page size is 2 where production declares 50,000 (the
trips-capture ``resultsLimit: 3`` precedent).

Variant coverage promised to consumers: the object-only ``device`` and
``driver`` refs each defensively lifting a bare string (the model tests
exercise the lift), the optional ``device`` ABSENT on record 2 (the
optional absent arm), and two event dates across the pages (on
``dateTime``, this vertical's event-time field).

Synthetic id families are NEW b-prefixed families (``bSL2*`` records,
``bSV8*`` devices, ``bSR6*`` drivers) not shared with wave one or two.
Shared by the ShipmentLog model tests and the shipment_logs endpoint
drive-through. The JSON literals are the fixtures; the parsed objects
beside them are what tests consume.
"""

import json

from fleetpull.vocabulary import JsonObject, JsonValue
from tests.geotab_feed_pages import feed_records

# Synthetic: the advance page — 2 records (full at the fixtures' page
# size). Record 1 is the full record (device present); record 2 omits
# the optional device.
SHIPMENT_LOGS_FEED_PAGE_1_RESPONSE_JSON: str = r"""
{
    "result": {
        "data": [
            {
                "activeFrom": "2026-07-14T06:00:00.000Z",
                "activeTo": "2026-07-14T18:00:00.000Z",
                "commodity": "Synthetic Commodity Alpha",
                "dateTime": "2026-07-14T06:00:00.000Z",
                "device": {
                    "id": "bSV801"
                },
                "documentNumber": "SYN-DOC-00001",
                "driver": {
                    "id": "bSR601"
                },
                "id": "bSL201",
                "shipperName": "Synthetic Shipper Co.",
                "version": "0000000000002b01"
            },
            {
                "activeFrom": "2026-07-14T12:30:00.000Z",
                "activeTo": "2026-07-14T22:00:00.000Z",
                "commodity": "Synthetic Commodity Beta",
                "dateTime": "2026-07-14T12:30:00.000Z",
                "documentNumber": "SYN-DOC-00002",
                "driver": {
                    "id": "bSR602"
                },
                "id": "bSL202",
                "shipperName": "Synthetic Freight LLC",
                "version": "0000000000002b02"
            }
        ],
        "toVersion": "0000000000002b02"
    },
    "jsonrpc": "2.0"
}"""

SHIPMENT_LOGS_FEED_PAGE_1_RESPONSE: dict[str, JsonValue] = json.loads(
    SHIPMENT_LOGS_FEED_PAGE_1_RESPONSE_JSON
)

# Synthetic: the terminal page — 1 record (short at the fixtures' page
# size), the second event date.
SHIPMENT_LOGS_FEED_PAGE_2_RESPONSE_JSON: str = r"""
{
    "result": {
        "data": [
            {
                "activeFrom": "2026-07-15T05:15:00.000Z",
                "activeTo": "2026-07-15T15:45:00.000Z",
                "commodity": "Synthetic Commodity Gamma",
                "dateTime": "2026-07-15T05:15:00.000Z",
                "device": {
                    "id": "bSV803"
                },
                "documentNumber": "SYN-DOC-00003",
                "driver": {
                    "id": "bSR625"
                },
                "id": "bSL203",
                "shipperName": "Synthetic Logistics Inc.",
                "version": "0000000000002b03"
            }
        ],
        "toVersion": "0000000000002b03"
    },
    "jsonrpc": "2.0"
}"""

SHIPMENT_LOGS_FEED_PAGE_2_RESPONSE: dict[str, JsonValue] = json.loads(
    SHIPMENT_LOGS_FEED_PAGE_2_RESPONSE_JSON
)


# The three feed records, in stream order.
SHIPMENT_LOG_RECORDS: list[JsonObject] = [
    *feed_records(SHIPMENT_LOGS_FEED_PAGE_1_RESPONSE),
    *feed_records(SHIPMENT_LOGS_FEED_PAGE_2_RESPONSE),
]

# The designated full-shape record (page 1, first record): every modeled
# field present — the mechanical alias-trap test iterates the model's
# fields against it.
SHIPMENT_LOG_FULL_RECORD: JsonObject = SHIPMENT_LOG_RECORDS[0]

# The designated sparse record (page 1, second record): the optional
# device absent.
SHIPMENT_LOG_SPARSE_RECORD: JsonObject = SHIPMENT_LOG_RECORDS[1]
