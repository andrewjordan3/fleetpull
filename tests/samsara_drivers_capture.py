"""The committed Samsara drivers capture set (2026-07-20 probe session).

Three of the 832 swept ``/fleet/drivers`` records -- the maximal active
variant (every observed key, including the excluded ``tags`` and
``eldSettings`` list-of-object blocks; ``externalIds`` was NEVER
observed in 832 records and so appears nowhere), the minimal variant
(the always-present key set only, with the empty-string home-terminal
pair), and a deactivated record (structurally matching the active
shape; the deactivated sweep's 372 records are fully disjoint from and
invisible to the default listing) -- plus a continuation-page envelope,
the captured TERMINAL pagination shape (``hasNextPage: false`` beside
an EMPTY-STRING ``endCursor``, the vehicles contract proven per-type on
drivers), and the HTTP 400 body every malformed
``driverActivationStatus`` value returns (the API-enforced enum
closure: case variants, comma-joins, repeated keys, and bogus values
all produce this shape -- loud, never silent-empty).

Captured from live Samsara and scrubbed per the Data Hygiene convention
before commit: every identifier is FULLY SYNTHETIC -- names, usernames,
licenses, phones, addresses, carrier strings, numeric ids (ascending
order preserved), the DOT number, tag ids/names, and the assigned
vehicle reference; ``driverActivationStatus`` vocabulary, the
millisecond ISO-8601 timestamp shape, the bare-integer ``dotNumber``
shape, the empty-string faces, and the 400 message text are VERBATIM
wire shapes.

Consumed by the Driver model tests and the drivers endpoint tests --
kept as a helper module under ``tests/`` so consumers share one capture
set (the ``samsara_vehicles_capture`` precedent). The raw JSON literals
are the captures; the parsed objects beside them are what tests
consume.
"""

import json

from fleetpull.vocabulary import JsonObject, JsonValue

# Captured: a continuation page (2026-07-20; production limit 512, two
# records committed -- the maximal and minimal active variants). The
# cursor advance was proven live per-type: a limit=5 walk of 92 pages
# returned 460/460 unique ascending ids with no boundary overlap or
# loss, a fresh endCursor per page.
DRIVERS_PAGE_RESPONSE_JSON: str = r"""
{
    "data": [
        {
            "id": "7100001",
            "name": "Driver Example001",
            "username": "example.driver001",
            "driverActivationStatus": "active",
            "timezone": "America/Chicago",
            "createdAtTime": "2021-06-14T18:22:05.114Z",
            "updatedAtTime": "2026-04-30T16:41:12.907Z",
            "hasVehicleUnpinningEnabled": false,
            "carrierSettings": {
                "carrierName": "Example Carrier LLC",
                "dotNumber": 100001,
                "mainOfficeAddress": "100 Example St, Example City, TX 75001",
                "homeTerminalName": "Example Terminal North",
                "homeTerminalAddress": "200 Example Ave, Example City, TX 75001"
            },
            "hosSetting": {
                "heavyHaulExemptionToggleEnabled": false
            },
            "staticAssignedVehicle": {
                "id": "218000000000001",
                "name": "U-901 (Example Truck)"
            },
            "peerGroupTag": {
                "id": "4400001",
                "name": "Peer Group - 01",
                "parentTagId": "4400000"
            },
            "vehicleGroupTag": {
                "id": "4500001",
                "name": "Vehicle Group - 01",
                "parentTagId": "4500000"
            },
            "licenseNumber": "D0000001",
            "licenseState": "TX",
            "phone": "+15550100",
            "locale": "us",
            "notes": "Example note",
            "profileImageUrl": "https://media.example.com/drivers/driver-example-001.png",
            "eldExempt": false,
            "eldExemptReason": "Example short-haul exemption",
            "eldAdverseWeatherExemptionEnabled": false,
            "eldBigDayExemptionEnabled": false,
            "eldPcEnabled": true,
            "eldYmEnabled": true,
            "waitingTimeDutyStatusEnabled": false,
            "tags": [
                {
                    "id": "3000001",
                    "name": "Unit Group - 01",
                    "parentTagId": "3000000"
                }
            ],
            "eldSettings": {
                "rulesets": [
                    {
                        "break": "REQUIRED_30_MIN_BREAK",
                        "cycle": "USA_70_HOUR_8_DAY",
                        "restart": "34_HOUR_RESTART",
                        "shift": "US_INTERSTATE_PROPERTY"
                    }
                ]
            }
        },
        {
            "id": "7100002",
            "name": "Driver Example002",
            "username": "example.driver002",
            "driverActivationStatus": "active",
            "timezone": "America/Denver",
            "createdAtTime": "2022-01-05T09:30:44.021Z",
            "updatedAtTime": "2022-01-05T09:30:44.021Z",
            "hasVehicleUnpinningEnabled": false,
            "carrierSettings": {
                "carrierName": "Example Carrier LLC",
                "dotNumber": 100001,
                "mainOfficeAddress": "100 Example St, Example City, TX 75001",
                "homeTerminalName": "",
                "homeTerminalAddress": ""
            },
            "hosSetting": {
                "heavyHaulExemptionToggleEnabled": false
            }
        }
    ],
    "pagination": {
        "endCursor": "00000000-0000-0000-0000-000000000021",
        "hasNextPage": true
    }
}"""

DRIVERS_PAGE_RESPONSE: dict[str, JsonValue] = json.loads(DRIVERS_PAGE_RESPONSE_JSON)

# Captured: the terminal page shape (2026-07-20) -- hasNextPage false
# beside an empty-string endCursor, the vehicles contract proven
# per-type -- carrying a deactivated-sweep record (limit=50 walk: 8
# pages, 50x7+22, 372/372 unique, every record deactivated, standard
# terminal).
DRIVERS_TERMINAL_RESPONSE_JSON: str = r"""
{
    "data": [
        {
            "id": "7100003",
            "name": "Driver Example003",
            "username": "example.driver003",
            "driverActivationStatus": "deactivated",
            "timezone": "America/Chicago",
            "createdAtTime": "2020-03-19T15:02:11.330Z",
            "updatedAtTime": "2024-11-08T20:15:59.406Z",
            "hasVehicleUnpinningEnabled": false,
            "carrierSettings": {
                "carrierName": "Example Carrier LLC",
                "dotNumber": 100001,
                "mainOfficeAddress": "100 Example St, Example City, TX 75001",
                "homeTerminalName": "Example Terminal South",
                "homeTerminalAddress": ""
            },
            "hosSetting": {
                "heavyHaulExemptionToggleEnabled": false
            }
        }
    ],
    "pagination": {
        "endCursor": "",
        "hasNextPage": false
    }
}"""

DRIVERS_TERMINAL_RESPONSE: dict[str, JsonValue] = json.loads(
    DRIVERS_TERMINAL_RESPONSE_JSON
)

# Captured: the HTTP 400 body for ANY malformed driverActivationStatus
# value (2026-07-20) -- the message text is verbatim wire vocabulary;
# the requestId is synthetic. Every probed variant (case changes,
# comma-joins, repeated keys, bogus values) returned exactly this
# shape: the enum closure is API-enforced and loud, never silent-empty.
DRIVERS_STATUS_ERROR_RESPONSE_JSON: str = r"""
{
    "message": "Invalid value for driverActivationStatus. Can only be 'active' or 'deactivated'",
    "requestId": "req-synthetic-000000000001"
}"""

DRIVERS_STATUS_ERROR_RESPONSE: dict[str, JsonValue] = json.loads(
    DRIVERS_STATUS_ERROR_RESPONSE_JSON
)


def _envelope_records(envelope: dict[str, JsonValue]) -> list[JsonObject]:
    result = envelope['data']
    assert isinstance(result, list)
    records: list[JsonObject] = []
    for record in result:
        assert isinstance(record, dict)
        records.append(record)
    return records


# All three committed records, in capture order: maximal active, minimal
# active, deactivated.
DRIVER_RECORDS: list[JsonObject] = _envelope_records(
    DRIVERS_PAGE_RESPONSE
) + _envelope_records(DRIVERS_TERMINAL_RESPONSE)
