"""Deterministic GeoTab Trip fixtures modeled on provider wire shapes.

The windowed seek-walk boundary pair, the day-prefixed-TimeSpan record,
and the zero-distance degenerate record use purpose-built test values.
They preserve the structural, pagination, parsing, and arithmetic
properties exercised by the tests: strictly ascending ids and versions,
page 2's request offset equal to page 1's last record id, starts inside
``[2026-07-06, 2026-07-13)``, both driver wire variants, ``b106`` on both
sides of the page boundary, day-prefixed TimeSpan parsing, seven-digit
fractional TimeSpan parsing, and interval arithmetic.

Shared by the Trip model tests, the seek-decoder search-survival
regression, and any future e2e consumers -- a multi-consumer fixture set,
so it lives in one helper module under ``tests/``. The JSON literals are
the designed wire-shaped fixtures; the parsed objects beside them are
what tests consume.
"""

import json

from fleetpull.vocabulary import JsonObject, JsonValue

# Fixture: windowed seek walk page 1 request (resultsLimit 3 -- the walk's parameter, not the mechanism;
# production uses 5000). The spec-builder shape: search fromDate/
# toDate beside sort-by-id with the EXPLICIT null offset;
# credentials injected by the session strategy.
TRIP_SEEK_PAGE_1_REQUEST_JSON: str = r"""
{
    "method": "Get",
    "params": {
        "typeName": "Trip",
        "search": {
            "fromDate": "2026-07-06T00:00:00Z",
            "toDate": "2026-07-13T00:00:00Z"
        },
        "resultsLimit": 3,
        "sort": {
            "sortBy": "id",
            "sortDirection": "asc",
            "offset": null
        },
        "credentials": {
            "database": "exampledb",
            "userName": "user@example.com",
            "sessionId": "SyntheticSessionId000001"
        }
    }
}"""

TRIP_SEEK_PAGE_1_REQUEST: dict[str, JsonValue] = json.loads(
    TRIP_SEEK_PAGE_1_REQUEST_JSON
)

# Fixture: windowed seek walk page 2 request -- identical but for
# sort.offset carrying page 1's last record id; search survives the
# advance untouched (the load-bearing seek-rewrite property).
TRIP_SEEK_PAGE_2_REQUEST_JSON: str = r"""
{
    "method": "Get",
    "params": {
        "typeName": "Trip",
        "search": {
            "fromDate": "2026-07-06T00:00:00Z",
            "toDate": "2026-07-13T00:00:00Z"
        },
        "resultsLimit": 3,
        "sort": {
            "sortBy": "id",
            "sortDirection": "asc",
            "offset": "b12AC4214"
        },
        "credentials": {
            "database": "exampledb",
            "userName": "user@example.com",
            "sessionId": "SyntheticSessionId000001"
        }
    }
}"""

TRIP_SEEK_PAGE_2_REQUEST: dict[str, JsonValue] = json.loads(
    TRIP_SEEK_PAGE_2_REQUEST_JSON
)

# Fixture: page 1 response -- three Trips on two devices; both the
# bare UnknownDriverId sentinel and the object-form driver appear.
# The second record (b12AC4055) is TRIP_FULL_RECORD: it carries
# every modeled field including the object-form driver.
TRIP_SEEK_PAGE_1_RESPONSE_JSON: str = r"""
{
    "result": [
        {
            "afterHoursDistance": 1.0,
            "afterHoursDrivingDuration": "00:05:00",
            "afterHoursEnd": true,
            "afterHoursStart": true,
            "afterHoursStopDuration": "00:02:00",
            "averageSpeed": 12.0,
            "distance": 1.0,
            "drivingDuration": "00:05:00",
            "engineHours": 36000.0,
            "idlingDuration": "00:00:00",
            "isSeatBeltOff": false,
            "maximumSpeed": 24,
            "nextTripStart": "2026-07-06T08:07:00.000Z",
            "odometer": 1000000.0,
            "speedRange1": 0,
            "speedRange1Duration": "00:00:00",
            "speedRange2": 0,
            "speedRange2Duration": "00:00:00",
            "speedRange3": 0,
            "speedRange3Duration": "00:00:00",
            "start": "2026-07-06T08:00:00.000Z",
            "stop": "2026-07-06T08:05:00.000Z",
            "stopDuration": "00:02:00",
            "stopPoint": {
                "x": -100.1,
                "y": 40.1
            },
            "workDistance": 0,
            "workDrivingDuration": "00:00:00",
            "workStopDuration": "00:00:00",
            "device": {
                "id": "b131"
            },
            "driver": "UnknownDriverId",
            "version": "0000000000000201",
            "id": "b12AC4053"
        },
        {
            "afterHoursDistance": 2.5,
            "afterHoursDrivingDuration": "00:10:00",
            "afterHoursEnd": true,
            "afterHoursStart": true,
            "afterHoursStopDuration": "00:05:00",
            "averageSpeed": 15.0,
            "distance": 2.5,
            "drivingDuration": "00:10:00",
            "engineHours": 36600.0,
            "idlingDuration": "00:05:00",
            "isSeatBeltOff": false,
            "maximumSpeed": 32,
            "nextTripStart": "2026-07-06T08:22:00.000Z",
            "odometer": 1002500.0,
            "speedRange1": 0,
            "speedRange1Duration": "00:00:00",
            "speedRange2": 0,
            "speedRange2Duration": "00:00:00",
            "speedRange3": 0,
            "speedRange3Duration": "00:00:00",
            "start": "2026-07-06T08:07:00.000Z",
            "stop": "2026-07-06T08:17:00.000Z",
            "stopDuration": "00:05:00",
            "stopPoint": {
                "x": -100.2,
                "y": 40.2
            },
            "workDistance": 0,
            "workDrivingDuration": "00:00:00",
            "workStopDuration": "00:00:00",
            "device": {
                "id": "b131"
            },
            "driver": {
                "id": "b156",
                "isDriver": true
            },
            "version": "0000000000000202",
            "id": "b12AC4055"
        },
        {
            "afterHoursDistance": 0.5,
            "afterHoursDrivingDuration": "00:03:30.0000000",
            "afterHoursEnd": true,
            "afterHoursStart": true,
            "afterHoursStopDuration": "00:00:30.0000000",
            "averageSpeed": 8.571429,
            "distance": 0.5,
            "drivingDuration": "00:03:30.0000000",
            "engineHours": 72000.123,
            "idlingDuration": "00:00:00",
            "isSeatBeltOff": false,
            "maximumSpeed": 18,
            "nextTripStart": "2026-07-06T09:04:00.123Z",
            "odometer": 2000000.0,
            "speedRange1": 0,
            "speedRange1Duration": "00:00:00",
            "speedRange2": 0,
            "speedRange2Duration": "00:00:00",
            "speedRange3": 0,
            "speedRange3Duration": "00:00:00",
            "start": "2026-07-06T09:00:00.123Z",
            "stop": "2026-07-06T09:03:30.123Z",
            "stopDuration": "00:00:30.0000000",
            "stopPoint": {
                "x": -100.3,
                "y": 40.3
            },
            "workDistance": 0,
            "workDrivingDuration": "00:00:00",
            "workStopDuration": "00:00:00",
            "device": {
                "id": "b106"
            },
            "driver": "UnknownDriverId",
            "version": "0000000000000203",
            "id": "b12AC4214"
        }
    ],
    "jsonrpc": "2.0"
}"""

TRIP_SEEK_PAGE_1_RESPONSE: dict[str, JsonValue] = json.loads(
    TRIP_SEEK_PAGE_1_RESPONSE_JSON
)

# Fixture: page 2 response -- ids continue strictly ascending across
# the boundary; b106 appears as the device on both sides of it.
TRIP_SEEK_PAGE_2_RESPONSE_JSON: str = r"""
{
    "result": [
        {
            "afterHoursDistance": 1.5,
            "afterHoursDrivingDuration": "00:06:00",
            "afterHoursEnd": true,
            "afterHoursStart": true,
            "afterHoursStopDuration": "00:02:30",
            "averageSpeed": 15.0,
            "distance": 1.5,
            "drivingDuration": "00:06:00",
            "engineHours": 72360.123,
            "idlingDuration": "00:02:30",
            "isSeatBeltOff": false,
            "maximumSpeed": 30,
            "nextTripStart": "2026-07-06T09:12:30.123Z",
            "odometer": 2001500.0,
            "speedRange1": 0,
            "speedRange1Duration": "00:00:00",
            "speedRange2": 0,
            "speedRange2Duration": "00:00:00",
            "speedRange3": 0,
            "speedRange3Duration": "00:00:00",
            "start": "2026-07-06T09:04:00.123Z",
            "stop": "2026-07-06T09:10:00.123Z",
            "stopDuration": "00:02:30",
            "stopPoint": {
                "x": -100.4,
                "y": 40.4
            },
            "workDistance": 0,
            "workDrivingDuration": "00:00:00",
            "workStopDuration": "00:00:00",
            "device": {
                "id": "b106"
            },
            "driver": {
                "id": "b129",
                "isDriver": true
            },
            "version": "0000000000000204",
            "id": "b12AC423F"
        },
        {
            "afterHoursDistance": 10.0,
            "afterHoursDrivingDuration": "00:20:00",
            "afterHoursEnd": true,
            "afterHoursStart": true,
            "afterHoursStopDuration": "00:05:30.5000000",
            "averageSpeed": 30.0,
            "distance": 10.0,
            "drivingDuration": "00:20:00",
            "engineHours": 73560.0,
            "idlingDuration": "00:00:30",
            "isSeatBeltOff": false,
            "maximumSpeed": 55,
            "nextTripStart": "2026-07-06T10:25:30.500Z",
            "odometer": 2011500.0,
            "speedRange1": 0,
            "speedRange1Duration": "00:00:00",
            "speedRange2": 0,
            "speedRange2Duration": "00:00:00",
            "speedRange3": 0,
            "speedRange3Duration": "00:00:00",
            "start": "2026-07-06T10:00:00.000Z",
            "stop": "2026-07-06T10:20:00.000Z",
            "stopDuration": "00:05:30.5000000",
            "stopPoint": {
                "x": -100.5,
                "y": 40.5
            },
            "workDistance": 0,
            "workDrivingDuration": "00:00:00",
            "workStopDuration": "00:00:00",
            "device": {
                "id": "b106"
            },
            "driver": {
                "id": "b129",
                "isDriver": true
            },
            "version": "0000000000000205",
            "id": "b12AC430C"
        },
        {
            "afterHoursDistance": 0.25,
            "afterHoursDrivingDuration": "00:00:46.5030000",
            "afterHoursEnd": true,
            "afterHoursStart": true,
            "afterHoursStopDuration": "00:00:14.9700000",
            "averageSpeed": 19.354,
            "distance": 0.25,
            "drivingDuration": "00:00:46.5030000",
            "engineHours": 108000.503,
            "idlingDuration": "00:00:00",
            "isSeatBeltOff": false,
            "maximumSpeed": 20,
            "nextTripStart": "2026-07-06T10:31:01.473Z",
            "odometer": 3000000.25,
            "speedRange1": 0,
            "speedRange1Duration": "00:00:00",
            "speedRange2": 0,
            "speedRange2Duration": "00:00:00",
            "speedRange3": 0,
            "speedRange3Duration": "00:00:00",
            "start": "2026-07-06T10:30:00.000Z",
            "stop": "2026-07-06T10:30:46.503Z",
            "stopDuration": "00:00:14.9700000",
            "stopPoint": {
                "x": -100.6,
                "y": 40.6
            },
            "workDistance": 0,
            "workDrivingDuration": "00:00:00",
            "workStopDuration": "00:00:00",
            "device": {
                "id": "b1190F"
            },
            "driver": "UnknownDriverId",
            "version": "0000000000000206",
            "id": "b12AC4374"
        }
    ],
    "jsonrpc": "2.0"
}"""

TRIP_SEEK_PAGE_2_RESPONSE: dict[str, JsonValue] = json.loads(
    TRIP_SEEK_PAGE_2_RESPONSE_JSON
)

# Fixture: the day-prefixed TimeSpan shape -- its stop window spans
# multiple days (stopDuration "4.16:41:16"), and the
# work/after-hours split sums to it exactly.
TRIP_DAY_FORMAT_RECORD_JSON: str = r"""
{
    "afterHoursDistance": 0,
    "afterHoursDrivingDuration": "00:00:00",
    "afterHoursEnd": false,
    "afterHoursStart": false,
    "afterHoursStopDuration": "3.19:36:59",
    "averageSpeed": 12.0,
    "distance": 1.0,
    "drivingDuration": "00:05:00",
    "engineHours": 144000.0,
    "idlingDuration": "00:10:00",
    "isSeatBeltOff": false,
    "maximumSpeed": 24,
    "nextTripStart": "2026-07-06T04:46:16.000Z",
    "odometer": 4000000.0,
    "speedRange1": 0,
    "speedRange1Duration": "00:00:00",
    "speedRange2": 0,
    "speedRange2Duration": "00:00:00",
    "speedRange3": 0,
    "speedRange3Duration": "00:00:00",
    "start": "2026-07-01T12:00:00.000Z",
    "stop": "2026-07-01T12:05:00.000Z",
    "stopDuration": "4.16:41:16",
    "stopPoint": {
        "x": -100.7,
        "y": 40.7
    },
    "workDistance": 1.0,
    "workDrivingDuration": "00:03:55",
    "workStopDuration": "21:04:17",
    "device": {
        "id": "b114C9"
    },
    "driver": "UnknownDriverId",
    "version": "0000000000000207",
    "id": "b12AC4D3D"
}"""

TRIP_DAY_FORMAT_RECORD: JsonObject = json.loads(TRIP_DAY_FORMAT_RECORD_JSON)

# Fixture: the zero-distance degenerate shape -- start == stop and
# NO averageSpeed key at all (absence is a shape, lands as null).
TRIP_ZERO_DISTANCE_RECORD_JSON: str = r"""
{
    "afterHoursDistance": 0,
    "afterHoursDrivingDuration": "00:00:00",
    "afterHoursEnd": true,
    "afterHoursStart": true,
    "afterHoursStopDuration": "00:08:15",
    "distance": 0,
    "drivingDuration": "00:00:00",
    "engineHours": 144600.0,
    "idlingDuration": "00:08:15",
    "isSeatBeltOff": false,
    "maximumSpeed": 0,
    "nextTripStart": "2026-07-06T11:08:15.000Z",
    "odometer": 4001000.0,
    "speedRange1": 0,
    "speedRange1Duration": "00:00:00",
    "speedRange2": 0,
    "speedRange2Duration": "00:00:00",
    "speedRange3": 0,
    "speedRange3Duration": "00:00:00",
    "start": "2026-07-06T11:00:00.000Z",
    "stop": "2026-07-06T11:00:00.000Z",
    "stopDuration": "00:08:15",
    "stopPoint": {
        "x": -100.8,
        "y": 40.8
    },
    "workDistance": 0,
    "workDrivingDuration": "00:00:00",
    "workStopDuration": "00:00:00",
    "device": {
        "id": "b114C9"
    },
    "driver": {
        "id": "b1CA",
        "isDriver": true
    },
    "version": "0000000000000208",
    "id": "b12AC4FF9"
}"""

TRIP_ZERO_DISTANCE_RECORD: JsonObject = json.loads(TRIP_ZERO_DISTANCE_RECORD_JSON)


def _result_records(envelope: dict[str, JsonValue]) -> list[JsonObject]:
    """Narrow a fixture Get envelope's ``result`` to its record list.

    The fixtures are known-good record lists; the asserts exist for the
    type checker (and would fail loudly if a fixture were ever edited
    into a different shape).
    """
    records = envelope['result']
    assert isinstance(records, list)
    narrowed: list[JsonObject] = []
    for record in records:
        assert isinstance(record, dict)
        narrowed.append(record)
    return narrowed


# The six paging Trip records of the seek walk, in walk order.
TRIP_RECORDS: list[JsonObject] = [
    *_result_records(TRIP_SEEK_PAGE_1_RESPONSE),
    *_result_records(TRIP_SEEK_PAGE_2_RESPONSE),
]

# The designated full-shape record (page 1, second record, b12AC4055):
# every modeled field present, driver in object form -- the mechanical
# alias-trap test iterates the model's fields against it.
TRIP_FULL_RECORD: JsonObject = TRIP_RECORDS[1]
