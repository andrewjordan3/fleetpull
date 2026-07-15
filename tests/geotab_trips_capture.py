"""The committed GeoTab trips capture set (2026-07-13 probe session).

The windowed seek-walk boundary pair (six Trip records), the
day-prefixed-TimeSpan record, and the zero-distance degenerate record --
all Captured from live GeoTab and scrubbed through the established
mapping, extended, never restarted (insert-1-after-``b`` ids -- the
devices set's own images, e.g. ``b6`` -> ``b106``; version tokens
ordinally remapped preserving order; coordinates synthetic distinct
pairs; timestamps, durations, distances, odometer, and engine-hours
values kept VERBATIM -- they carry the arithmetic properties under
test). The capture used ``resultsLimit: 3`` where production uses 5000.
Load-bearing properties preserved: ids strictly ascending within and
across the page pair, page 2's request offset equal to page 1's last
record id, versions ascending in id order, every paging-record stop
inside ``[2026-07-06, 2026-07-13)`` (``TripSearch`` matches by STOP
time — prediction-confirmed 2026-07-15; the starts also fell inside
this capture's window but carry no retrieval guarantee), both driver
variants present, and ``b106`` as the device on both sides of the page
boundary.

Shared by the Trip model tests, the seek-decoder search-survival
regression, and any future e2e consumers -- a multi-consumer capture
set, so it lives in one helper module under ``tests/`` (the
``geotab_devices_capture`` precedent). The JSON literals are the
captures; the parsed objects beside them are what tests consume.
"""

import json

from fleetpull.vocabulary import JsonObject, JsonValue

# Captured: windowed seek walk page 1 request (2026-07-13,
# resultsLimit 3 -- the walk's parameter, not the mechanism;
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

# Captured: windowed seek walk page 2 request -- identical but for
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

# Captured: page 1 response -- three Trips on two devices; both the
# bare UnknownDriverId sentinel and the object-form driver appear.
# The second record (b12AC4055) is TRIP_FULL_RECORD: it carries
# every modeled field including the object-form driver.
TRIP_SEEK_PAGE_1_RESPONSE_JSON: str = r"""
{
    "result": [
        {
            "afterHoursDistance": 0.04897735,
            "afterHoursDrivingDuration": "00:04:43.8600000",
            "afterHoursEnd": true,
            "afterHoursStart": true,
            "afterHoursStopDuration": "00:00:45",
            "averageSpeed": 0.62114584,
            "distance": 0.04897735,
            "drivingDuration": "00:04:43.8600000",
            "engineHours": 57808048.863,
            "idlingDuration": "00:00:00",
            "isSeatBeltOff": false,
            "maximumSpeed": 5,
            "nextTripStart": "2026-07-06T05:48:12.000Z",
            "odometer": 1182036289.7453485,
            "speedRange1": 0,
            "speedRange1Duration": "00:00:00",
            "speedRange2": 0,
            "speedRange2Duration": "00:00:00",
            "speedRange3": 0,
            "speedRange3Duration": "00:00:00",
            "start": "2026-07-06T05:42:43.140Z",
            "stop": "2026-07-06T05:47:27.000Z",
            "stopDuration": "00:00:45",
            "stopPoint": {
                "x": -100.0001,
                "y": 40.0001
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
            "afterHoursDistance": 0.119007945,
            "afterHoursDrivingDuration": "00:01:22",
            "afterHoursEnd": true,
            "afterHoursStart": true,
            "afterHoursStopDuration": "00:08:07",
            "averageSpeed": 5.224739,
            "distance": 0.119007945,
            "drivingDuration": "00:01:22",
            "engineHours": 57808617.863,
            "idlingDuration": "00:08:07",
            "isSeatBeltOff": false,
            "maximumSpeed": 10,
            "nextTripStart": "2026-07-06T05:57:41.000Z",
            "odometer": 1182036408.7532907,
            "speedRange1": 0,
            "speedRange1Duration": "00:00:00",
            "speedRange2": 0,
            "speedRange2Duration": "00:00:00",
            "speedRange3": 0,
            "speedRange3Duration": "00:00:00",
            "start": "2026-07-06T05:48:12.000Z",
            "stop": "2026-07-06T05:49:34.000Z",
            "stopDuration": "00:08:07",
            "stopPoint": {
                "x": -100.0002,
                "y": 40.0002
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
            "afterHoursDistance": 0.07463475,
            "afterHoursDrivingDuration": "00:05:22.6900000",
            "afterHoursEnd": true,
            "afterHoursStart": true,
            "afterHoursStopDuration": "00:01:39",
            "averageSpeed": 0.8326416,
            "distance": 0.07463475,
            "drivingDuration": "00:05:22.6900000",
            "engineHours": 37553461.71,
            "idlingDuration": "00:00:00",
            "isSeatBeltOff": false,
            "maximumSpeed": 3,
            "nextTripStart": "2026-07-06T07:32:43.000Z",
            "odometer": 677495874.8907506,
            "speedRange1": 0,
            "speedRange1Duration": "00:00:00",
            "speedRange2": 0,
            "speedRange2Duration": "00:00:00",
            "speedRange3": 0,
            "speedRange3Duration": "00:00:00",
            "start": "2026-07-06T07:25:41.310Z",
            "stop": "2026-07-06T07:31:04.000Z",
            "stopDuration": "00:01:39",
            "stopPoint": {
                "x": -100.0003,
                "y": 40.0003
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

# Captured: page 2 response -- ids continue strictly ascending across
# the boundary; b106 appears as the device on both sides of it.
TRIP_SEEK_PAGE_2_RESPONSE_JSON: str = r"""
{
    "result": [
        {
            "afterHoursDistance": 0.06195259,
            "afterHoursDrivingDuration": "00:04:35",
            "afterHoursEnd": true,
            "afterHoursStart": true,
            "afterHoursStopDuration": "00:06:34",
            "averageSpeed": 0.8110157,
            "distance": 0.06195259,
            "drivingDuration": "00:04:35",
            "engineHours": 37554130.71,
            "idlingDuration": "00:06:34",
            "isSeatBeltOff": false,
            "maximumSpeed": 4,
            "nextTripStart": "2026-07-06T07:43:52.000Z",
            "odometer": 677495936.8433416,
            "speedRange1": 0,
            "speedRange1Duration": "00:00:00",
            "speedRange2": 0,
            "speedRange2Duration": "00:00:00",
            "speedRange3": 0,
            "speedRange3Duration": "00:00:00",
            "start": "2026-07-06T07:32:43.000Z",
            "stop": "2026-07-06T07:37:18.000Z",
            "stopDuration": "00:06:34",
            "stopPoint": {
                "x": -100.0004,
                "y": 40.0004
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
            "afterHoursDistance": 5.4301457,
            "afterHoursDrivingDuration": "00:16:56",
            "afterHoursEnd": true,
            "afterHoursStart": true,
            "afterHoursStopDuration": "00:04:00.1300000",
            "averageSpeed": 19.240673,
            "distance": 5.4301457,
            "drivingDuration": "00:16:56",
            "engineHours": 37555200.003,
            "idlingDuration": "00:00:18",
            "isSeatBeltOff": false,
            "maximumSpeed": 61,
            "nextTripStart": "2026-07-06T08:04:48.130Z",
            "odometer": 677501400.2560003,
            "speedRange1": 0,
            "speedRange1Duration": "00:00:00",
            "speedRange2": 0,
            "speedRange2Duration": "00:00:00",
            "speedRange3": 0,
            "speedRange3Duration": "00:00:00",
            "start": "2026-07-06T07:43:52.000Z",
            "stop": "2026-07-06T08:00:48.000Z",
            "stopDuration": "00:04:00.1300000",
            "stopPoint": {
                "x": -100.0005,
                "y": 40.0005
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
            "afterHoursDistance": 0.018614754,
            "afterHoursDrivingDuration": "00:00:46.5030000",
            "afterHoursEnd": true,
            "afterHoursStart": true,
            "afterHoursStopDuration": "00:00:14.9700000",
            "averageSpeed": 1.4410493,
            "distance": 0.018614754,
            "drivingDuration": "00:00:46.5030000",
            "engineHours": 21673141.477,
            "idlingDuration": "00:00:00",
            "isSeatBeltOff": false,
            "maximumSpeed": 0,
            "nextTripStart": "2026-07-06T08:11:50.000Z",
            "odometer": 413906118.59677917,
            "speedRange1": 0,
            "speedRange1Duration": "00:00:00",
            "speedRange2": 0,
            "speedRange2Duration": "00:00:00",
            "speedRange3": 0,
            "speedRange3Duration": "00:00:00",
            "start": "2026-07-06T08:10:48.527Z",
            "stop": "2026-07-06T08:11:35.030Z",
            "stopDuration": "00:00:14.9700000",
            "stopPoint": {
                "x": -100.0006,
                "y": 40.0006
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

# Captured: the day-prefixed TimeSpan shape -- its stop window spans
# the July 4 holiday weekend (stopDuration "4.16:41:16"), and the
# work/after-hours split sums to it exactly.
TRIP_DAY_FORMAT_RECORD_JSON: str = r"""
{
    "afterHoursDistance": 0,
    "afterHoursDrivingDuration": "00:00:00",
    "afterHoursEnd": false,
    "afterHoursStart": false,
    "afterHoursStopDuration": "3.19:36:59",
    "averageSpeed": 2.6459758,
    "distance": 0.17272343,
    "drivingDuration": "00:03:55",
    "engineHours": 40103395,
    "idlingDuration": "00:08:36",
    "isSeatBeltOff": false,
    "maximumSpeed": 7,
    "nextTripStart": "2026-07-06T10:36:59.000Z",
    "odometer": 717726000,
    "speedRange1": 0,
    "speedRange1Duration": "00:00:00",
    "speedRange2": 0,
    "speedRange2Duration": "00:00:00",
    "speedRange3": 0,
    "speedRange3Duration": "00:00:00",
    "start": "2026-07-01T17:51:48.000Z",
    "stop": "2026-07-01T17:55:43.000Z",
    "stopDuration": "4.16:41:16",
    "stopPoint": {
        "x": -100.0007,
        "y": 40.0007
    },
    "workDistance": 0.17272343,
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

# Captured: the zero-distance degenerate shape -- start == stop and
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
    "engineHours": 40104728,
    "idlingDuration": "00:08:15",
    "isSeatBeltOff": false,
    "maximumSpeed": 0,
    "nextTripStart": "2026-07-06T10:59:12.000Z",
    "odometer": 717726189.9556732,
    "speedRange1": 0,
    "speedRange1Duration": "00:00:00",
    "speedRange2": 0,
    "speedRange2Duration": "00:00:00",
    "speedRange3": 0,
    "speedRange3Duration": "00:00:00",
    "start": "2026-07-06T10:50:57.000Z",
    "stop": "2026-07-06T10:50:57.000Z",
    "stopDuration": "00:08:15",
    "stopPoint": {
        "x": -100.0008,
        "y": 40.0008
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
    """Narrow a captured Get envelope's ``result`` to its record list.

    The captures are known-good record lists; the asserts exist for the
    type checker (and would fail loudly if a capture were ever edited
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
