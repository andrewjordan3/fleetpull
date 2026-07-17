"""The committed GeoTab ExceptionEvent capture set (2026-07-13/15 sessions).

The three idling-rule records (the 2026-07-13 window capture), the two
error envelopes the sort discrimination produced (2026-07-15), and the
silent-empty response -- all Captured from live GeoTab and scrubbed
through the Data Hygiene convention, per the Data Hygiene convention (the new
``a``-prefix arm: ``aSYN`` + a zero-padded 19-digit counter, fixed
width preserving captured order; device ``b5`` -> ``bF7C24``, the devices
set's own image; versions ordinally remapped ``0x209``-``0x20b``
preserving order; error-envelope GUIDs -> zero-GUID counters 9-10;
timestamps, durations, distances, and the sentinel/state vocabulary
VERBATIM -- they carry the arithmetic properties under test). The
capture used ``resultsLimit: 3`` where production uses 5000.

Load-bearing properties preserved: ``duration = activeTo - activeFrom``
exactly on every record, including the third record's fractional-second
span (``00:13:05.2500000``) reproducing its fractional ``activeFrom``;
versions strictly ascending in capture order; both bare sentinels
(``"UnknownDriverId"``, ``"NoDiagnosticId"``) present on every record
(the object-form driver is Trip-captured grammar, unobserved on this
type). The ArgumentException message text is the capture that killed
seek paging for this type -- fixture material, never matched on by the
classifier (the read-the-type-never-the-message rule).

Consumed by the ExceptionEvent model tests -- kept as a helper module
under ``tests/`` so future consumers share one capture set (the
``geotab_trips_capture`` precedent). The JSON literals are the
captures; the parsed objects beside them are what tests consume.
"""

import json

from fleetpull.vocabulary import JsonObject, JsonValue

# Captured: the idling-rule window response (2026-07-13, ruleSearch +
# dates, NO sort -- the composition that succeeds on this type;
# resultsLimit 3). Three records, one device, both bare sentinels on
# every record.
EXCEPTION_EVENTS_RESPONSE_JSON: str = r"""
{
    "result": [
        {
            "activeFrom": "2026-07-06T13:24:02.000Z",
            "activeTo": "2026-07-06T13:40:15.000Z",
            "distance": 0.13597468,
            "duration": "00:16:13",
            "rule": {
                "state": "ExceptionRuleStateActiveId",
                "reason": "ExceptionRuleReasonNoneId",
                "id": "RuleIdlingId"
            },
            "device": {
                "id": "bF7C24"
            },
            "diagnostic": "NoDiagnosticId",
            "driver": "UnknownDriverId",
            "state": "ExceptionEventStateValidId",
            "lastModifiedDateTime": "2026-07-06T13:48:28.958Z",
            "createdDateTime": "2026-07-06T13:31:56.783Z",
            "version": "0000000000000209",
            "id": "aSYN0000000000000000001"
        },
        {
            "activeFrom": "2026-07-06T13:40:15.000Z",
            "activeTo": "2026-07-06T13:54:22.000Z",
            "distance": 0.06476391,
            "duration": "00:14:07",
            "rule": {
                "state": "ExceptionRuleStateActiveId",
                "reason": "ExceptionRuleReasonNoneId",
                "id": "RuleIdlingId"
            },
            "device": {
                "id": "bF7C24"
            },
            "diagnostic": "NoDiagnosticId",
            "driver": "UnknownDriverId",
            "state": "ExceptionEventStateValidId",
            "lastModifiedDateTime": "2026-07-06T13:54:43.394Z",
            "createdDateTime": "2026-07-06T13:48:30.126Z",
            "version": "000000000000020a",
            "id": "aSYN0000000000000000002"
        },
        {
            "activeFrom": "2026-07-06T19:19:12.750Z",
            "activeTo": "2026-07-06T19:32:18.000Z",
            "distance": 0.013796482,
            "duration": "00:13:05.2500000",
            "rule": {
                "state": "ExceptionRuleStateActiveId",
                "reason": "ExceptionRuleReasonNoneId",
                "id": "RuleIdlingId"
            },
            "device": {
                "id": "bF7C24"
            },
            "diagnostic": "NoDiagnosticId",
            "driver": "UnknownDriverId",
            "state": "ExceptionEventStateValidId",
            "lastModifiedDateTime": "2026-07-06T20:17:50.011Z",
            "createdDateTime": "2026-07-06T20:17:47.292Z",
            "version": "000000000000020b",
            "id": "aSYN0000000000000000003"
        }
    ],
    "jsonrpc": "2.0"
}
"""

EXCEPTION_EVENTS_RESPONSE: dict[str, JsonValue] = json.loads(
    EXCEPTION_EVENTS_RESPONSE_JSON
)

# Captured: the silent-empty shape -- an unmatched search referent (or
# an empty window) returns a clean empty result, never an error.
EXCEPTION_EVENTS_EMPTY_RESPONSE_JSON: str = r"""
{
    "result": [],
    "jsonrpc": "2.0"
}
"""

EXCEPTION_EVENTS_EMPTY_RESPONSE: dict[str, JsonValue] = json.loads(
    EXCEPTION_EVENTS_EMPTY_RESPONSE_JSON
)

# Captured (2026-07-15): the deterministic crash of sort composed with
# any ExceptionEventSearch -- reproduced on exact retry; HTTP 200 per
# GeoTab's universal posture.
EXCEPTION_EVENTS_GENERIC_ERROR_JSON: str = r"""
{
    "error": {
        "message": "An undefined exception has occurred. Please contact Support for further assistance.",
        "code": -32000,
        "data": {
            "id": "00000000-0000-0000-0000-000000000009",
            "type": "GenericException",
            "requestIndex": 0
        },
        "name": "JSONRPCError",
        "errors": [
            {
                "message": "An undefined exception has occurred. Please contact Support for further assistance.",
                "name": "GenericException"
            }
        ]
    },
    "jsonrpc": "2.0",
    "requestIndex": 0
}
"""

EXCEPTION_EVENTS_GENERIC_ERROR: dict[str, JsonValue] = json.loads(
    EXCEPTION_EVENTS_GENERIC_ERROR_JSON
)

# Captured (2026-07-15): sort with NO search -- the diagnosis the
# generic crash hides: id-sort is unsupported on this type outright.
EXCEPTION_EVENTS_ARGUMENT_ERROR_JSON: str = r"""
{
    "error": {
        "message": "Can not sort by id. Supported sortable fields are version, date.",
        "code": -32000,
        "data": {
            "id": "00000000-0000-0000-0000-000000000010",
            "type": "ArgumentException",
            "requestIndex": 0
        },
        "name": "JSONRPCError",
        "errors": [
            {
                "message": "Can not sort by id. Supported sortable fields are version, date.",
                "name": "ArgumentException"
            }
        ]
    },
    "jsonrpc": "2.0",
    "requestIndex": 0
}
"""

EXCEPTION_EVENTS_ARGUMENT_ERROR: dict[str, JsonValue] = json.loads(
    EXCEPTION_EVENTS_ARGUMENT_ERROR_JSON
)


def _envelope_records(envelope: dict[str, JsonValue]) -> list[JsonObject]:
    result = envelope['result']
    assert isinstance(result, list)
    records: list[JsonObject] = []
    for record in result:
        assert isinstance(record, dict)
        records.append(record)
    return records


# The three records in capture order -- what most tests iterate.
EXCEPTION_EVENT_RECORDS: list[JsonObject] = _envelope_records(EXCEPTION_EVENTS_RESPONSE)
