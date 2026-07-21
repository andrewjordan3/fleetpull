"""The committed Samsara driver_vehicle_assignments capture set
(2026-07-20 probe session).

Five FULLY SYNTHETIC assignment records shaped by the live census of
``GET /fleet/driver-vehicle-assignments`` (a full 24-hour walk under
BOTH ``filterBy`` values -- 216 records each, proven identical as tuple
sets, so the two sweeps are one dataset and ``filterBy=vehicles`` is a
fixed param), arranged as a two-page cursor walk mirroring the server's
FIXED 50-record paging shape (the ``limit`` param is proven ignored --
limit=1/5/100/512/513 and no limit each returned a 50-record first
page; the committed pages carry a representative few records, the
idling precedent).

The variant coverage, against a notional [2026-01-02, 2026-01-03)
window: a MIDNIGHT-SPANNING assignment whose ``startTime`` precedes the
window start (the overlap-retrieval evidence -- the probe's adjacent
day windows shared 5 such spanners as identical tuples), an assignment
whose ``endTime`` lands past the window end (the right-edge
straddler), both observed ``assignmentType`` values (``static`` and
``HOS`` -- census-closed only, NOT API-enforced on output), an
``isPassenger: true`` row (a co-driver sharing another record's
vehicle and span), and recurring drivers/vehicles across pages.

Every record carries the censused shape exactly -- the census was
TOTAL: every key present on 216/216 records (``startTime``/``endTime``
RFC3339 strs with no empty or missing ``endTime`` anywhere,
``assignedAtTime`` the EMPTY STRING on every row -- 6,921/6,921 across
a week-wide value census, live-proven 2026-07-21 --, ``assignmentType`` str, ``isPassenger`` bool,
``driver {id, name}`` both strs, ``vehicle {id, name, externalIds}``
with ``externalIds`` carrying the LITERAL DOTTED wire keys
``samsara.serial``/``samsara.vin``, both strs).

No record values here are scrubbed live values -- every id, name,
timestamp, serial, and VIN is synthetic outright (VIN-shaped fakes,
``SYNTH-`` serials). What IS verbatim wire truth: the ``data`` +
``pagination {endCursor, hasNextPage}`` envelope, the camelCase key
set, the RFC3339 string shapes, the dotted external-id keys on the
NESTED vehicle ref (the wire's own keys, unlike the stats triple's
decoder-synthesized flat ones), and the terminal ``hasNextPage: false``
beside an empty-string ``endCursor``.

Consumed by the DriverVehicleAssignment model tests and the
driver_vehicle_assignments endpoint tests -- kept as a helper module
under ``tests/`` so consumers share one capture set (the
``samsara_vehicles_capture`` precedent). The raw JSON literals are the
captures; the parsed objects beside them are what tests consume.
"""

import json

from fleetpull.vocabulary import JsonObject, JsonValue

# A continuation page: the midnight spanner (startTime before the
# notional window start -- overlap retrieval), an HOS row, and the
# passenger co-driver sharing the HOS row's vehicle and span. The
# endCursor is an opaque synthetic token.
DRIVER_VEHICLE_ASSIGNMENTS_PAGE_RESPONSE_JSON: str = r"""
{
    "data": [
        {
            "driver": {
                "id": "51000001",
                "name": "Synthetic Driver One"
            },
            "vehicle": {
                "id": "281474981110001",
                "name": "SYNTH-TRUCK-001",
                "externalIds": {
                    "samsara.serial": "SYNTH-SER-001",
                    "samsara.vin": "SYNTHVIN000000001"
                }
            },
            "startTime": "2026-01-01T22:00:00Z",
            "endTime": "2026-01-02T06:00:00Z",
            "assignedAtTime": "",
            "assignmentType": "static",
            "isPassenger": false
        },
        {
            "driver": {
                "id": "51000002",
                "name": "Synthetic Driver Two"
            },
            "vehicle": {
                "id": "281474981110002",
                "name": "SYNTH-TRUCK-002",
                "externalIds": {
                    "samsara.serial": "SYNTH-SER-002",
                    "samsara.vin": "SYNTHVIN000000002"
                }
            },
            "startTime": "2026-01-02T08:15:00Z",
            "endTime": "2026-01-02T12:45:00Z",
            "assignedAtTime": "",
            "assignmentType": "HOS",
            "isPassenger": false
        },
        {
            "driver": {
                "id": "51000003",
                "name": "Synthetic Driver Three"
            },
            "vehicle": {
                "id": "281474981110002",
                "name": "SYNTH-TRUCK-002",
                "externalIds": {
                    "samsara.serial": "SYNTH-SER-002",
                    "samsara.vin": "SYNTHVIN000000002"
                }
            },
            "startTime": "2026-01-02T08:15:00Z",
            "endTime": "2026-01-02T12:45:00Z",
            "assignedAtTime": "",
            "assignmentType": "HOS",
            "isPassenger": true
        }
    ],
    "pagination": {
        "endCursor": "c3ludGgtYXNzaWdubWVudC1jdXJzb3ItMDAx",
        "hasNextPage": true
    }
}"""

DRIVER_VEHICLE_ASSIGNMENTS_PAGE_RESPONSE: dict[str, JsonValue] = json.loads(
    DRIVER_VEHICLE_ASSIGNMENTS_PAGE_RESPONSE_JSON
)

# The terminal page shape -- hasNextPage false beside an empty-string
# endCursor (the provider-wide cursor contract) -- carrying the
# right-edge straddler (endTime past the notional window end) and a
# fully-inside row, with a driver and a vehicle recurring from page one.
DRIVER_VEHICLE_ASSIGNMENTS_TERMINAL_RESPONSE_JSON: str = r"""
{
    "data": [
        {
            "driver": {
                "id": "51000001",
                "name": "Synthetic Driver One"
            },
            "vehicle": {
                "id": "281474981110003",
                "name": "SYNTH-TRUCK-003",
                "externalIds": {
                    "samsara.serial": "SYNTH-SER-003",
                    "samsara.vin": "SYNTHVIN000000003"
                }
            },
            "startTime": "2026-01-02T20:30:00Z",
            "endTime": "2026-01-03T04:10:00Z",
            "assignedAtTime": "",
            "assignmentType": "static",
            "isPassenger": false
        },
        {
            "driver": {
                "id": "51000002",
                "name": "Synthetic Driver Two"
            },
            "vehicle": {
                "id": "281474981110001",
                "name": "SYNTH-TRUCK-001",
                "externalIds": {
                    "samsara.serial": "SYNTH-SER-001",
                    "samsara.vin": "SYNTHVIN000000001"
                }
            },
            "startTime": "2026-01-02T13:00:00Z",
            "endTime": "2026-01-02T17:30:00Z",
            "assignedAtTime": "",
            "assignmentType": "static",
            "isPassenger": false
        }
    ],
    "pagination": {
        "endCursor": "",
        "hasNextPage": false
    }
}"""

DRIVER_VEHICLE_ASSIGNMENTS_TERMINAL_RESPONSE: dict[str, JsonValue] = json.loads(
    DRIVER_VEHICLE_ASSIGNMENTS_TERMINAL_RESPONSE_JSON
)


def _envelope_records(envelope: dict[str, JsonValue]) -> list[JsonObject]:
    result = envelope['data']
    assert isinstance(result, list)
    records: list[JsonObject] = []
    for record in result:
        assert isinstance(record, dict)
        records.append(record)
    return records


# All five committed records, in capture order: the midnight spanner,
# the HOS row, the passenger co-driver, the right-edge straddler, the
# fully-inside row.
DRIVER_VEHICLE_ASSIGNMENT_RECORDS: list[JsonObject] = _envelope_records(
    DRIVER_VEHICLE_ASSIGNMENTS_PAGE_RESPONSE
) + _envelope_records(DRIVER_VEHICLE_ASSIGNMENTS_TERMINAL_RESPONSE)
