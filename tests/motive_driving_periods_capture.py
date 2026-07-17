"""The committed Motive driving_periods capture set (2026-07-15 probe session).

The offset-pagination page pair (ten complete records), the in-progress
record, the terminal empty page, and the 30-day range-cap error envelope
-- all Captured from live Motive and scrubbed through the established
mapping, extended, never restarted (VIN counters 07-22 continue the
devices set; coordinates continue at 09; integer ids remapped ordinally
per id space, preserving order; driver/vehicle images are equality
classes -- one raw identity, one image everywhere; the captured
unit-number-is-VIN-tail relation and the space-after-dash
driver_company_id quirk are preserved; timestamps, durations, kilometers,
and pagination echoes are VERBATIM -- they carry the arithmetic and
ordering properties under test). The capture used ``per_page: 5``
(production 100).

Load-bearing properties preserved: record ids strictly descending within
and across the page pair (the endpoint sorts by start_time descending);
``duration = end_time - start_time`` exactly on every complete record;
the pagination echoes verbatim (page 2 is non-terminal under the
decoder's ``page_no * per_page >= total`` rule, the empty page is past
the boundary); the in-progress record nulls every end-side field while
``duration`` carries a fractional elapsed value; the empty-string
``destination`` and the ``year: "0"`` / empty ``make``/``model`` vehicle
are the coercion-rule exhibits, kept verbatim on the wire side.

Consumed by the DrivingPeriod model tests -- kept as a helper module
under ``tests/`` so future consumers share one capture set (the
``geotab_trips_capture`` precedent). The JSON literals are the captures;
the parsed objects beside them are what tests consume.
"""

import json

from fleetpull.vocabulary import JsonObject, JsonValue

# Captured: page 1 response (2026-07-15, per_page 5 -- the walk's
# parameter, not the mechanism; production uses 100). Five complete
# records: null-driver and object-driver variants, the
# empty-make/model + year-"0" vehicle, and the annotated yard-move
# record. ``source`` and the ``*_hvb_*`` fields arrive null-only and
# are deliberately unmodeled.
DRIVING_PERIODS_PAGE_1_RESPONSE_JSON: str = r"""
{
    "driving_periods": [
        {
            "driving_period": {
                "id": 4000000010,
                "start_time": "2026-07-14T23:59:55Z",
                "end_time": "2026-07-15T00:00:27Z",
                "status": "complete",
                "type": "driving",
                "annotation_status": 3,
                "notes": null,
                "duration": 32.0,
                "start_kilometers": 283774.14758,
                "end_kilometers": 283774.22999,
                "source": null,
                "driver": null,
                "vehicle": {
                    "id": 500008,
                    "number": "000014",
                    "year": "2022",
                    "make": "Kenworth",
                    "model": "Box",
                    "vin": "4SYNTHV1N00000014",
                    "metric_units": false
                },
                "origin": "101 Synthetic St, Exampleville, AZ 10001",
                "origin_lat": 40.0009,
                "origin_lon": -100.0009,
                "destination_lat": 40.001,
                "destination_lon": -100.001,
                "destination": "102 Synthetic St, Exampleville, AZ 10002",
                "distance": "0.1 mi",
                "start_hvb_state_of_charge": null,
                "end_hvb_state_of_charge": null,
                "start_hvb_lifetime_energy_output": null,
                "end_hvb_lifetime_energy_output": null
            }
        },
        {
            "driving_period": {
                "id": 4000000009,
                "start_time": "2026-07-14T23:54:21Z",
                "end_time": "2026-07-15T00:41:46Z",
                "status": "complete",
                "type": "driving",
                "annotation_status": null,
                "notes": null,
                "duration": 2845.0,
                "start_kilometers": 474983.41176,
                "end_kilometers": 475051.29712,
                "source": null,
                "driver": {
                    "id": 2000006,
                    "first_name": "Synthetic",
                    "last_name": "Driver006",
                    "username": null,
                    "email": "synthetic.driver006@example.com",
                    "driver_company_id": "10001-SYN",
                    "status": "active",
                    "role": "driver"
                },
                "vehicle": {
                    "id": 500001,
                    "number": "000007",
                    "year": "2014",
                    "make": "Kenworth",
                    "model": "Sleeper",
                    "vin": "4SYNTHV1N00000007",
                    "metric_units": false
                },
                "origin": "103 Synthetic St, Exampleville, MO 10003",
                "origin_lat": 40.0011,
                "origin_lon": -100.0011,
                "destination_lat": 40.0012,
                "destination_lon": -100.0012,
                "destination": "104 Synthetic St, Exampleville, MO 10004",
                "distance": "42.2 mi",
                "start_hvb_state_of_charge": null,
                "end_hvb_state_of_charge": null,
                "start_hvb_lifetime_energy_output": null,
                "end_hvb_lifetime_energy_output": null
            }
        },
        {
            "driving_period": {
                "id": 4000000008,
                "start_time": "2026-07-14T23:50:06Z",
                "end_time": "2026-07-14T23:51:34Z",
                "status": "complete",
                "type": "driving",
                "annotation_status": null,
                "notes": null,
                "duration": 88.0,
                "start_kilometers": 288796.53144,
                "end_kilometers": 288797.36451,
                "source": null,
                "driver": {
                    "id": 2000008,
                    "first_name": "Synthetic",
                    "last_name": "Driver008",
                    "username": null,
                    "email": "synthetic.driver008@example.com",
                    "driver_company_id": "10002-SYN",
                    "status": "active",
                    "role": "driver"
                },
                "vehicle": {
                    "id": 500004,
                    "number": "000010",
                    "year": "2017",
                    "make": "Kenworth",
                    "model": "Box",
                    "vin": "4SYNTHV1N00000010",
                    "metric_units": false
                },
                "origin": "105 Synthetic St, Exampleville, CA 10005",
                "origin_lat": 40.0013,
                "origin_lon": -100.0013,
                "destination_lat": 40.0014,
                "destination_lon": -100.0014,
                "destination": "106 Synthetic St, Exampleville, CA 10006",
                "distance": "0.5 mi",
                "start_hvb_state_of_charge": null,
                "end_hvb_state_of_charge": null,
                "start_hvb_lifetime_energy_output": null,
                "end_hvb_lifetime_energy_output": null
            }
        },
        {
            "driving_period": {
                "id": 4000000007,
                "start_time": "2026-07-14T23:49:06Z",
                "end_time": "2026-07-14T23:56:37Z",
                "status": "complete",
                "type": "driving",
                "annotation_status": null,
                "notes": null,
                "duration": 451.0,
                "start_kilometers": 107336.80804,
                "end_kilometers": 107341.43219,
                "source": null,
                "driver": {
                    "id": 2000005,
                    "first_name": "Synthetic",
                    "last_name": "Driver005",
                    "username": "synthetic.driver005",
                    "email": "synthetic.driver005@example.com",
                    "driver_company_id": "10003-SYN",
                    "status": "active",
                    "role": "driver"
                },
                "vehicle": {
                    "id": 500016,
                    "number": "R000001",
                    "year": "0",
                    "make": "",
                    "model": "",
                    "vin": "4SYNTHV1N00000022",
                    "metric_units": false
                },
                "origin": "107 Synthetic St, Exampleville, UT 10007",
                "origin_lat": 40.0015,
                "origin_lon": -100.0015,
                "destination_lat": 40.0016,
                "destination_lon": -100.0016,
                "destination": "108 Synthetic St, Exampleville, UT 10008",
                "distance": "2.9 mi",
                "start_hvb_state_of_charge": null,
                "end_hvb_state_of_charge": null,
                "start_hvb_lifetime_energy_output": null,
                "end_hvb_lifetime_energy_output": null
            }
        },
        {
            "driving_period": {
                "id": 4000000006,
                "start_time": "2026-07-14T23:47:07Z",
                "end_time": "2026-07-14T23:47:34Z",
                "status": "complete",
                "type": "driving",
                "annotation_status": 1,
                "notes": "synthetic note 001",
                "duration": 27.0,
                "start_kilometers": 100371.43661,
                "end_kilometers": 100371.48172,
                "source": null,
                "driver": null,
                "vehicle": {
                    "id": 500011,
                    "number": "000017",
                    "year": "2024",
                    "make": "PETERBILT",
                    "model": "Box",
                    "vin": "4SYNTHV1N00000017",
                    "metric_units": false
                },
                "origin": "109 Synthetic St, Exampleville, CA 10009",
                "origin_lat": 40.0017,
                "origin_lon": -100.0017,
                "destination_lat": 40.0018,
                "destination_lon": -100.0018,
                "destination": "110 Synthetic St, Exampleville, CA 10010",
                "distance": "0.0 mi",
                "start_hvb_state_of_charge": null,
                "end_hvb_state_of_charge": null,
                "start_hvb_lifetime_energy_output": null,
                "end_hvb_lifetime_energy_output": null
            }
        }
    ],
    "pagination": {
        "per_page": 5,
        "page_no": 1,
        "total": 10366
    }
}
"""

DRIVING_PERIODS_PAGE_1_RESPONSE: dict[str, JsonValue] = json.loads(
    DRIVING_PERIODS_PAGE_1_RESPONSE_JSON
)

# Captured: page 2 response -- the echo advances, the total holds, and
# ids keep descending across the page boundary.
DRIVING_PERIODS_PAGE_2_RESPONSE_JSON: str = r"""
{
    "driving_periods": [
        {
            "driving_period": {
                "id": 4000000005,
                "start_time": "2026-07-14T23:46:14Z",
                "end_time": "2026-07-15T00:09:53Z",
                "status": "complete",
                "type": "driving",
                "annotation_status": null,
                "notes": null,
                "duration": 1419.0,
                "start_kilometers": 156792.62545,
                "end_kilometers": 156818.15368,
                "source": null,
                "driver": {
                    "id": 2000009,
                    "first_name": "Synthetic",
                    "last_name": "Driver009",
                    "username": null,
                    "email": "synthetic.driver009@example.com",
                    "driver_company_id": "10004- SYN",
                    "status": "active",
                    "role": "driver"
                },
                "vehicle": {
                    "id": 500005,
                    "number": "000011",
                    "year": "2019",
                    "make": "Kenworth",
                    "model": "Vac",
                    "vin": "4SYNTHV1N00000011",
                    "metric_units": false
                },
                "origin": "111 Synthetic St, Exampleville, TN 10011",
                "origin_lat": 40.0019,
                "origin_lon": -100.0019,
                "destination_lat": 40.002,
                "destination_lon": -100.002,
                "destination": "112 Synthetic St, Exampleville, TN 10012",
                "distance": "15.9 mi",
                "start_hvb_state_of_charge": null,
                "end_hvb_state_of_charge": null,
                "start_hvb_lifetime_energy_output": null,
                "end_hvb_lifetime_energy_output": null
            }
        },
        {
            "driving_period": {
                "id": 4000000004,
                "start_time": "2026-07-14T23:44:49Z",
                "end_time": "2026-07-14T23:46:02Z",
                "status": "complete",
                "type": "driving",
                "annotation_status": null,
                "notes": null,
                "duration": 73.0,
                "start_kilometers": 359890.58171,
                "end_kilometers": 359890.72739,
                "source": null,
                "driver": {
                    "id": 2000003,
                    "first_name": "Synthetic",
                    "last_name": "Driver003",
                    "username": "synthetic.driver003",
                    "email": "synthetic.driver003@example.com",
                    "driver_company_id": "10005-SYN",
                    "status": "active",
                    "role": "driver"
                },
                "vehicle": {
                    "id": 500007,
                    "number": "000013",
                    "year": "2020",
                    "make": "Kenworth",
                    "model": "Vac",
                    "vin": "4SYNTHV1N00000013",
                    "metric_units": false
                },
                "origin": "113 Synthetic St, Exampleville, TX 10013",
                "origin_lat": 40.0021,
                "origin_lon": -100.0021,
                "destination_lat": 40.0022,
                "destination_lon": -100.0022,
                "destination": "114 Synthetic St, Exampleville, TX 10014",
                "distance": "0.1 mi",
                "start_hvb_state_of_charge": null,
                "end_hvb_state_of_charge": null,
                "start_hvb_lifetime_energy_output": null,
                "end_hvb_lifetime_energy_output": null
            }
        },
        {
            "driving_period": {
                "id": 4000000003,
                "start_time": "2026-07-14T23:37:54Z",
                "end_time": "2026-07-15T00:25:54Z",
                "status": "complete",
                "type": "driving",
                "annotation_status": null,
                "notes": null,
                "duration": 2880.0,
                "start_kilometers": 36902.31584,
                "end_kilometers": 36975.99106,
                "source": null,
                "driver": {
                    "id": 2000001,
                    "first_name": "Synthetic",
                    "last_name": "Driver001",
                    "username": "010006",
                    "email": "synthetic.driver001@example.com",
                    "driver_company_id": "10006-SYN",
                    "status": "active",
                    "role": "driver"
                },
                "vehicle": {
                    "id": 500015,
                    "number": "000021",
                    "year": "2026",
                    "make": "kenworth",
                    "model": "T880",
                    "vin": "4SYNTHV1N00000021",
                    "metric_units": false
                },
                "origin": "115 Synthetic St, Exampleville, TN 10015",
                "origin_lat": 40.0023,
                "origin_lon": -100.0023,
                "destination_lat": 40.0024,
                "destination_lon": -100.0024,
                "destination": "116 Synthetic St, Exampleville, TN 10016",
                "distance": "45.8 mi",
                "start_hvb_state_of_charge": null,
                "end_hvb_state_of_charge": null,
                "start_hvb_lifetime_energy_output": null,
                "end_hvb_lifetime_energy_output": null
            }
        },
        {
            "driving_period": {
                "id": 4000000002,
                "start_time": "2026-07-14T23:34:47Z",
                "end_time": "2026-07-15T00:20:39Z",
                "status": "complete",
                "type": "driving",
                "annotation_status": null,
                "notes": null,
                "duration": 2752.0,
                "start_kilometers": 61279.231923,
                "end_kilometers": 61309.31122,
                "source": null,
                "driver": {
                    "id": 2000011,
                    "first_name": "Synthetic",
                    "last_name": "Driver011",
                    "username": null,
                    "email": "synthetic.driver011@example.com",
                    "driver_company_id": "10007- SYN",
                    "status": "active",
                    "role": "driver"
                },
                "vehicle": {
                    "id": 500013,
                    "number": "000019",
                    "year": "2024",
                    "make": "MACK",
                    "model": "Box",
                    "vin": "4SYNTHV1N00000019",
                    "metric_units": false
                },
                "origin": "117 Synthetic St, Exampleville, NV 10017",
                "origin_lat": 40.0025,
                "origin_lon": -100.0025,
                "destination_lat": 40.0026,
                "destination_lon": -100.0026,
                "destination": "118 Synthetic St, Exampleville, NV 10018",
                "distance": "18.7 mi",
                "start_hvb_state_of_charge": null,
                "end_hvb_state_of_charge": null,
                "start_hvb_lifetime_energy_output": null,
                "end_hvb_lifetime_energy_output": null
            }
        },
        {
            "driving_period": {
                "id": 4000000001,
                "start_time": "2026-07-14T23:34:46Z",
                "end_time": "2026-07-14T23:52:43Z",
                "status": "complete",
                "type": "driving",
                "annotation_status": null,
                "notes": null,
                "duration": 1077.0,
                "start_kilometers": 180686.13426,
                "end_kilometers": 180700.13719,
                "source": null,
                "driver": {
                    "id": 2000010,
                    "first_name": "Synthetic",
                    "last_name": "Driver010",
                    "username": null,
                    "email": "synthetic.driver010@example.com",
                    "driver_company_id": "10008- SYN",
                    "status": "active",
                    "role": "driver"
                },
                "vehicle": {
                    "id": 500014,
                    "number": "000020",
                    "year": "2024",
                    "make": "KENWORTH",
                    "model": "Oil",
                    "vin": "4SYNTHV1N00000020",
                    "metric_units": false
                },
                "origin": "119 Synthetic St, Exampleville, AZ 10019",
                "origin_lat": 40.0027,
                "origin_lon": -100.0027,
                "destination_lat": 40.0028,
                "destination_lon": -100.0028,
                "destination": "120 Synthetic St, Exampleville, AZ 10020",
                "distance": "8.7 mi",
                "start_hvb_state_of_charge": null,
                "end_hvb_state_of_charge": null,
                "start_hvb_lifetime_energy_output": null,
                "end_hvb_lifetime_energy_output": null
            }
        }
    ],
    "pagination": {
        "per_page": 5,
        "page_no": 2,
        "total": 10366
    }
}
"""

DRIVING_PERIODS_PAGE_2_RESPONSE: dict[str, JsonValue] = json.loads(
    DRIVING_PERIODS_PAGE_2_RESPONSE_JSON
)

# Captured: an in-progress record (a same-day window at 19:09 UTC) --
# the null-end-side shape: end_time/end_kilometers/distance and the
# destination coordinates null, destination an EMPTY STRING, duration a
# fractional running counter (not end - start).
DRIVING_PERIOD_IN_PROGRESS_RECORD_JSON: str = r"""
{
    "id": 4000000011,
    "start_time": "2026-07-15T19:09:51Z",
    "end_time": null,
    "status": "in_progress",
    "type": "driving",
    "annotation_status": null,
    "notes": null,
    "duration": 112.896090996,
    "start_kilometers": 83661.021074,
    "end_kilometers": null,
    "source": null,
    "driver": {
        "id": 2000004,
        "first_name": "Synthetic",
        "last_name": "Driver004",
        "username": "synthetic.driver004",
        "email": null,
        "driver_company_id": "10009-SYN",
        "status": "active",
        "role": "driver"
    },
    "vehicle": {
        "id": 500012,
        "number": "000018",
        "year": "2024",
        "make": "MACK",
        "model": "Box",
        "vin": "4SYNTHV1N00000018",
        "metric_units": false
    },
    "origin": "121 Synthetic St, Exampleville, OH 10021",
    "origin_lat": 40.0029,
    "origin_lon": -100.0029,
    "destination_lat": null,
    "destination_lon": null,
    "destination": "",
    "distance": null,
    "start_hvb_state_of_charge": null,
    "end_hvb_state_of_charge": null,
    "start_hvb_lifetime_energy_output": null,
    "end_hvb_lifetime_energy_output": null
}
"""

DRIVING_PERIOD_IN_PROGRESS_RECORD: JsonObject = json.loads(
    DRIVING_PERIOD_IN_PROGRESS_RECORD_JSON
)

# Captured: a page past the data's end -- 200, empty list, pagination
# echo intact. The decoder never requests it (the computed boundary
# stops first); committed as the provider's terminal shape.
DRIVING_PERIODS_EMPTY_PAGE_RESPONSE_JSON: str = r"""
{
    "driving_periods": [],
    "pagination": {
        "per_page": 5,
        "page_no": 2100,
        "total": 10366
    }
}
"""

DRIVING_PERIODS_EMPTY_PAGE_RESPONSE: dict[str, JsonValue] = json.loads(
    DRIVING_PERIODS_EMPTY_PAGE_RESPONSE_JSON
)

# Captured: the 30-day range-cap rejection -- HTTP 400, the flat
# error_message envelope (the same shape as Motive's 401 body). The cap
# counts the date delta; exactly 30 days is accepted.
DRIVING_PERIODS_RANGE_ERROR_JSON: str = r"""
{
    "error_message": "Date range cannot be greater than 30 days"
}
"""

DRIVING_PERIODS_RANGE_ERROR: dict[str, JsonValue] = json.loads(
    DRIVING_PERIODS_RANGE_ERROR_JSON
)


def _page_records(page: dict[str, JsonValue]) -> list[JsonObject]:
    wrappers = page['driving_periods']
    assert isinstance(wrappers, list)
    records: list[JsonObject] = []
    for wrapper in wrappers:
        assert isinstance(wrapper, dict)
        record = wrapper['driving_period']
        assert isinstance(record, dict)
        records.append(record)
    return records


# The ten complete records in page order -- what most tests iterate.
DRIVING_PERIOD_RECORDS: list[JsonObject] = [
    *_page_records(DRIVING_PERIODS_PAGE_1_RESPONSE),
    *_page_records(DRIVING_PERIODS_PAGE_2_RESPONSE),
]
