"""The committed Motive idle_events capture set (2026-07-15 probe session).

One full page (five records) and the single-record ``per_page: 1`` page
carrying the ``rg_match: false`` shape -- all Captured from live Motive
and scrubbed through the Data Hygiene convention, per the Data Hygiene convention
(the same session and id spaces as ``motive_driving_periods_capture``;
the ELD serial arm ``AABL36SYN0000N`` is new with this set; timestamps,
fuel counters, reverse-geocode numerics, and pagination echoes are
VERBATIM). The capture used ``per_page: 5`` (production 100).

Load-bearing properties preserved: record ids strictly ascending and
``end_time`` non-decreasing within the page (the endpoint sorts by end
time ascending -- the opposite of its driving_periods sibling); the
first two records share one driver, vehicle, and ELD device (the
equality-class exhibit); both timestamps present on every record (no
in-progress analogue was observed); the ``rg_match: false`` record's
``location`` carries the distance-direction prefix format verbatim.

The endpoint's company-local overlap window matching (DESIGN section 8)
lives in the endpoint leaf's wire-window pad, not in these shapes.

Consumed by the IdleEvent model tests -- kept as a helper module under
``tests/`` so future consumers share one capture set (the
``geotab_trips_capture`` precedent). The JSON literals are the captures;
the parsed objects beside them are what tests consume.
"""

import json

from fleetpull.vocabulary import JsonObject, JsonValue

# Captured: page 1 response (2026-07-15, per_page 5 -- the walk's
# parameter, not the mechanism; production uses 100). Five records:
# object-driver and null-driver variants, a same-driver/same-vehicle
# pair, and both end_type values.
IDLE_EVENTS_PAGE_1_RESPONSE_JSON: str = r"""
{
    "idle_events": [
        {
            "idle_event": {
                "id": 5000000002,
                "start_time": "2026-07-13T04:28:49Z",
                "end_time": "2026-07-13T05:58:56Z",
                "veh_fuel_start": 365757.53125,
                "veh_fuel_end": 365763.96875,
                "lat": 40.003,
                "lon": -100.003,
                "city": "Synthetic City 001",
                "state": "MD",
                "rg_brg": 249.311681172163,
                "rg_km": 3.97002319881581,
                "rg_match": true,
                "end_type": "engine_stop",
                "driver": {
                    "id": 2000002,
                    "first_name": "Synthetic",
                    "last_name": "Driver002",
                    "username": null,
                    "email": "synthetic.driver002@example.com",
                    "driver_company_id": "10010-SYN",
                    "status": "active",
                    "role": "driver"
                },
                "vehicle": {
                    "id": 500002,
                    "number": "000008",
                    "year": "2012",
                    "make": "Kenworth",
                    "model": "Sleeper",
                    "vin": "4SYNTHV1N00000008",
                    "metric_units": false
                },
                "eld_device": {
                    "id": 900003,
                    "identifier": "AABL36SYN00003",
                    "model": "lbb-3.6ca"
                },
                "location": "Synthetic City 001, MD"
            }
        },
        {
            "idle_event": {
                "id": 5000000003,
                "start_time": "2026-07-13T07:01:07Z",
                "end_time": "2026-07-13T07:05:53Z",
                "veh_fuel_start": 365764.46875,
                "veh_fuel_end": 365765.21875,
                "lat": 40.0031,
                "lon": -100.0031,
                "city": "Synthetic City 001",
                "state": "MD",
                "rg_brg": 249.03625365646,
                "rg_km": 3.98047028033294,
                "rg_match": true,
                "end_type": "vehicle_moving",
                "driver": {
                    "id": 2000002,
                    "first_name": "Synthetic",
                    "last_name": "Driver002",
                    "username": null,
                    "email": "synthetic.driver002@example.com",
                    "driver_company_id": "10010-SYN",
                    "status": "active",
                    "role": "driver"
                },
                "vehicle": {
                    "id": 500002,
                    "number": "000008",
                    "year": "2012",
                    "make": "Kenworth",
                    "model": "Sleeper",
                    "vin": "4SYNTHV1N00000008",
                    "metric_units": false
                },
                "eld_device": {
                    "id": 900003,
                    "identifier": "AABL36SYN00003",
                    "model": "lbb-3.6ca"
                },
                "location": "Synthetic City 001, MD"
            }
        },
        {
            "idle_event": {
                "id": 5000000004,
                "start_time": "2026-07-13T07:07:54Z",
                "end_time": "2026-07-13T07:16:16Z",
                "veh_fuel_start": 23494.359375,
                "veh_fuel_end": 23494.935546875,
                "lat": 40.0032,
                "lon": -100.0032,
                "city": "Synthetic City 002",
                "state": "TX",
                "rg_brg": 33.0583498051172,
                "rg_km": 2.84971759711513,
                "rg_match": true,
                "end_type": "engine_stop",
                "driver": null,
                "vehicle": {
                    "id": 500003,
                    "number": "000009",
                    "year": "2014",
                    "make": "Kenworth",
                    "model": "Daycab",
                    "vin": "4SYNTHV1N00000009",
                    "metric_units": false
                },
                "eld_device": {
                    "id": 900004,
                    "identifier": "AABL36SYN00004",
                    "model": "lbb-3.6ca"
                },
                "location": "Synthetic City 002, TX"
            }
        },
        {
            "idle_event": {
                "id": 5000000005,
                "start_time": "2026-07-13T07:37:44Z",
                "end_time": "2026-07-13T07:39:58Z",
                "veh_fuel_start": 143072.703125,
                "veh_fuel_end": 143072.859375,
                "lat": 40.0033,
                "lon": -100.0033,
                "city": "Synthetic City 003",
                "state": "FL",
                "rg_brg": 243.830189544374,
                "rg_km": 1.6540710398674,
                "rg_match": true,
                "end_type": "engine_stop",
                "driver": null,
                "vehicle": {
                    "id": 500009,
                    "number": "000015",
                    "year": "2018",
                    "make": "Kenworth",
                    "model": "Daycab",
                    "vin": "4SYNTHV1N00000015",
                    "metric_units": false
                },
                "eld_device": {
                    "id": 900001,
                    "identifier": "AABL36SYN00001",
                    "model": "lbb-3.6ca"
                },
                "location": "Synthetic City 003, FL"
            }
        },
        {
            "idle_event": {
                "id": 5000000006,
                "start_time": "2026-07-13T06:34:33Z",
                "end_time": "2026-07-13T07:45:04Z",
                "veh_fuel_start": 225542.4375,
                "veh_fuel_end": 225545.796875,
                "lat": 40.0034,
                "lon": -100.0034,
                "city": "Synthetic City 004",
                "state": "OH",
                "rg_brg": 223.85273664379,
                "rg_km": 2.33642190712434,
                "rg_match": true,
                "end_type": "vehicle_moving",
                "driver": {
                    "id": 2000007,
                    "first_name": "Synthetic",
                    "last_name": "Driver007",
                    "username": "synthetic.driver007",
                    "email": "synthetic.driver007@example.com",
                    "driver_company_id": "10011-SYN",
                    "status": "active",
                    "role": "driver"
                },
                "vehicle": {
                    "id": 500010,
                    "number": "000016",
                    "year": "2019",
                    "make": "Peterbilt",
                    "model": "Daycab",
                    "vin": "4SYNTHV1N00000016",
                    "metric_units": false
                },
                "eld_device": {
                    "id": 900002,
                    "identifier": "AABL36SYN00002",
                    "model": "lbb-3.6ca"
                },
                "location": "Synthetic City 004, OH"
            }
        }
    ],
    "pagination": {
        "per_page": 5,
        "page_no": 1,
        "total": 12869
    }
}
"""

IDLE_EVENTS_PAGE_1_RESPONSE: dict[str, JsonValue] = json.loads(
    IDLE_EVENTS_PAGE_1_RESPONSE_JSON
)

# Captured: a per_page-1 page from the 35-day-window probe (the window
# the 30-day cap did NOT reject on this endpoint) -- the
# ``rg_match: false`` shape, whose ``location`` is the provider's
# distance-direction format rather than a bare place name.
IDLE_EVENTS_SINGLE_PAGE_RESPONSE_JSON: str = r"""
{
    "idle_events": [
        {
            "idle_event": {
                "id": 5000000001,
                "start_time": "2026-06-01T05:14:05Z",
                "end_time": "2026-06-01T05:17:54Z",
                "veh_fuel_start": 2369.837890625,
                "veh_fuel_end": 2370.20678710938,
                "lat": 40.0035,
                "lon": -100.0035,
                "city": "Synthetic City 005",
                "state": "NJ",
                "rg_brg": 315.278574790485,
                "rg_km": 4.13898633414085,
                "rg_match": false,
                "end_type": "engine_stop",
                "driver": null,
                "vehicle": {
                    "id": 500006,
                    "number": "000012",
                    "year": "2015",
                    "make": "Kenworth",
                    "model": "Box",
                    "vin": "4SYNTHV1N00000012",
                    "metric_units": false
                },
                "eld_device": {
                    "id": 900005,
                    "identifier": "AABL36SYN00005",
                    "model": "lbb-3.6ca"
                },
                "location": "2.6 mi NW of Synthetic City 005, NJ"
            }
        }
    ],
    "pagination": {
        "per_page": 1,
        "page_no": 1,
        "total": 154866
    }
}
"""

IDLE_EVENTS_SINGLE_PAGE_RESPONSE: dict[str, JsonValue] = json.loads(
    IDLE_EVENTS_SINGLE_PAGE_RESPONSE_JSON
)


def _page_records(page: dict[str, JsonValue]) -> list[JsonObject]:
    wrappers = page['idle_events']
    assert isinstance(wrappers, list)
    records: list[JsonObject] = []
    for wrapper in wrappers:
        assert isinstance(wrapper, dict)
        record = wrapper['idle_event']
        assert isinstance(record, dict)
        records.append(record)
    return records


# All six records -- the full page then the single-record page.
IDLE_EVENT_RECORDS: list[JsonObject] = [
    *_page_records(IDLE_EVENTS_PAGE_1_RESPONSE),
    *_page_records(IDLE_EVENTS_SINGLE_PAGE_RESPONSE),
]
