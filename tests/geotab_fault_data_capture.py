"""The synthetic GeoTab fault_data feed fixture set (2026-07-21 shapes).

FULLY SYNTHETIC — no capture, no credentials: every value is invented in
the probe-settled shapes (the 2026-07-21 feed wave two census, DESIGN
§8: 13 census-total keys plus the 2/2,000 rare quartet, NO per-record
``version`` — the LogRecord asymmetry). The envelopes are the verified
GETFEED shape — ``result: {data, toVersion}`` — as an ADVANCE page
(full at the fixtures' page size of 2) and a TERMINAL page (short),
with 16-hex-lowercase version tokens per the machinery's
synthetic-token convention; the page size is 2 where production
declares 50,000 (the trips-capture ``resultsLimit: 3`` precedent).

Variant coverage promised to consumers: BOTH ``failureMode`` arms (the
``{id}`` object on records 1 and 3, the bare known-id string on record
2 — the proven mixed ref), the rare quartet (``diagnosticSeverity``,
``riskOfBreakdown``, ``severity``, ``sourceAddress``) PRESENT on record
1 and ABSENT on records 2 and 3 (the optional absent arm), and two
event dates across the pages. No record carries a ``version`` key —
the census asymmetry, pinned.

Shared by the FaultData model tests and the fault_data endpoint
drive-through (the ``geotab_trips_capture`` precedent). The JSON
literals are the fixtures; the parsed objects beside them are what
tests consume.
"""

import json

from fleetpull.vocabulary import JsonObject, JsonValue
from tests.geotab_feed_pages import feed_records

# Synthetic: the advance page — 2 records (full at the fixtures' page
# size). Record 1 is the full record (rare quartet present, object
# failureMode); record 2 carries the bare-string failureMode arm and
# no rare quartet.
FAULT_DATA_FEED_PAGE_1_RESPONSE_JSON: str = r"""
{
    "result": {
        "data": [
            {
                "amberWarningLamp": false,
                "controller": {
                    "id": "ControllerObdPowertrainId"
                },
                "count": 3,
                "dateTime": "2026-07-14T08:20:00.000Z",
                "device": {
                    "id": "b8A1"
                },
                "diagnostic": {
                    "id": "DiagnosticEngineOilPressureId"
                },
                "diagnosticSeverity": "Warning",
                "failureMode": {
                    "id": "bFA31"
                },
                "faultState": "Active",
                "faultStates": {
                    "effectiveStatus": "Active"
                },
                "id": "b21f201",
                "malfunctionLamp": false,
                "protectWarningLamp": false,
                "redStopLamp": false,
                "riskOfBreakdown": 0.85,
                "severity": "High",
                "sourceAddress": 0
            },
            {
                "amberWarningLamp": true,
                "controller": {
                    "id": "ControllerObdBodyId"
                },
                "count": 1,
                "dateTime": "2026-07-14T16:45:30.000Z",
                "device": {
                    "id": "b8A3"
                },
                "diagnostic": {
                    "id": "DiagnosticCheckEngineLightId"
                },
                "failureMode": "NoFailureModeId",
                "faultState": "Pending",
                "faultStates": {
                    "effectiveStatus": "Pending"
                },
                "id": "b21f202",
                "malfunctionLamp": true,
                "protectWarningLamp": false,
                "redStopLamp": false
            }
        ],
        "toVersion": "00000000000021f2"
    },
    "jsonrpc": "2.0"
}"""

FAULT_DATA_FEED_PAGE_1_RESPONSE: dict[str, JsonValue] = json.loads(
    FAULT_DATA_FEED_PAGE_1_RESPONSE_JSON
)

# Synthetic: the terminal page — 1 record (short at the fixtures' page
# size), the second event date, the object failureMode arm again.
FAULT_DATA_FEED_PAGE_2_RESPONSE_JSON: str = r"""
{
    "result": {
        "data": [
            {
                "amberWarningLamp": false,
                "controller": {
                    "id": "ControllerObdPowertrainId"
                },
                "count": 7,
                "dateTime": "2026-07-15T09:10:00.000Z",
                "device": {
                    "id": "b8A1"
                },
                "diagnostic": {
                    "id": "DiagnosticEngineOilPressureId"
                },
                "failureMode": {
                    "id": "bFA35"
                },
                "faultState": "Cleared",
                "faultStates": {
                    "effectiveStatus": "Cleared"
                },
                "id": "b21f203",
                "malfunctionLamp": false,
                "protectWarningLamp": false,
                "redStopLamp": false
            }
        ],
        "toVersion": "00000000000021f3"
    },
    "jsonrpc": "2.0"
}"""

FAULT_DATA_FEED_PAGE_2_RESPONSE: dict[str, JsonValue] = json.loads(
    FAULT_DATA_FEED_PAGE_2_RESPONSE_JSON
)


# The three feed records, in stream order.
FAULT_DATA_RECORDS: list[JsonObject] = [
    *feed_records(FAULT_DATA_FEED_PAGE_1_RESPONSE),
    *feed_records(FAULT_DATA_FEED_PAGE_2_RESPONSE),
]

# The designated full-shape record (page 1, first record): every modeled
# field present, rare quartet included -- the mechanical alias-trap test
# iterates the model's fields against it.
FAULT_DATA_FULL_RECORD: JsonObject = FAULT_DATA_RECORDS[0]

# The designated sparse record (page 1, second record): the bare-string
# failureMode arm and the rare quartet absent.
FAULT_DATA_SPARSE_RECORD: JsonObject = FAULT_DATA_RECORDS[1]
