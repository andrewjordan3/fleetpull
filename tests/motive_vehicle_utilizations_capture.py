"""The committed Motive vehicle_utilizations capture set (2026-07-21
probe session).

Three FULLY SYNTHETIC vehicle-utilization rollup records shaped by the
live census of ``GET /v2/vehicle_utilization`` (120 records sampled
across the 1,466-vehicle listing, structurally uniform -- every key on
every sampled record), arranged as a two-page offset walk so the
fixture exercises the continuation shape at fixture scale (``per_page``
2 here; production uses the configured page size). No value here is a
scrubbed live value -- every id, number, VIN, metric, and message is
synthetic outright.

What IS verbatim wire truth: the
``vehicle_utilizations``/``vehicle_utilization`` wrapped-list envelope
and the ``pagination {per_page, page_no, total}`` echo shape; the
snake_case key set (ten record keys, seven vehicle-ref sub-keys --
exactly the shared ``VehicleSummary`` shape); the float metric core
(floats on THIS arm; the driver arm's durations are ints); the
str-or-None ``last_located_at`` (its value format is unprobed --
illustrative here, mirrored verbatim as str by the model); and the
whole-fleet population shape: INACTIVE vehicles ride in every window
with zeroed metrics and a populated ``message`` status string, active
vehicles with an empty one.

The variant coverage: an active metric-rich vehicle; an INACTIVE
zeroed vehicle with its no-data ``message`` and null
``last_located_at``; and a NULL-VIN vehicle (the utilization surface's
``vin`` null arm on the shared ``VehicleSummary``).

Rollup rows carry NO date or time identity of any kind -- the row's
time identity is the request window itself, which the decoder stamps on
as ``windowStartDate``/``windowEndDate`` from the SENT spec's
``start_date``/``end_date`` DATE LABELS (inclusive on both ends,
interpreted in COMPANY-LOCAL days -- DESIGN section 8). The raw pages
below are PRE-STAMP wire truth; the stamped records beside them are
what the model tests consume, stamped exactly as the decoder stamps
(verbatim copies of the notional [2026-01-05, 2026-01-06) one-day
unit's labels: ``start_date=end_date='2026-01-05'``).

Consumed by the VehicleUtilization model tests and the
vehicle_utilizations endpoint tests -- kept as a helper module under
``tests/`` so consumers share one capture set (the
``motive_groups_capture`` precedent). The raw JSON literals are the
captures; the parsed objects beside them are what tests consume.
"""

import json

from fleetpull.vocabulary import JsonObject, JsonValue

# The notional sent window's labels -- verbatim what the shared builder
# renders for a [2026-01-05, 2026-01-06) one-day unit (the declared
# fixed_unit_days=1 width): both labels are the unit's day.
WINDOW_START_DATE: str = '2026-01-05'
WINDOW_END_DATE: str = '2026-01-05'

# A continuation page: the active metric-rich vehicle and the INACTIVE
# zeroed vehicle (its message populated, its last_located_at null). The
# echo is non-terminal under the decoder's ``page_no * per_page >=
# total`` rule (1 * 2 < 3).
VEHICLE_UTILIZATIONS_PAGE_1_RESPONSE_JSON: str = r"""
{
    "vehicle_utilizations": [
        {
            "vehicle_utilization": {
                "driving_fuel": 42.7,
                "driving_time": 21540.0,
                "idle_fuel": 3.2,
                "idle_time": 1860.0,
                "last_located_at": "2026-01-05T16:42:11-05:00",
                "message": "",
                "total_distance": 512.6,
                "total_fuel": 45.9,
                "utilization_percentage": 87.3,
                "vehicle": {
                    "id": 9900101,
                    "make": "Kenworth",
                    "metric_units": false,
                    "model": "T680",
                    "number": "TRK-0101",
                    "vin": "4SYNTHV1N00000101",
                    "year": "2020"
                }
            }
        },
        {
            "vehicle_utilization": {
                "driving_fuel": 0.0,
                "driving_time": 0.0,
                "idle_fuel": 0.0,
                "idle_time": 0.0,
                "last_located_at": null,
                "message": "No data available for the vehicle in given time period",
                "total_distance": 0.0,
                "total_fuel": 0.0,
                "utilization_percentage": 0.0,
                "vehicle": {
                    "id": 9900102,
                    "make": "Freightliner",
                    "metric_units": false,
                    "model": "Cascadia",
                    "number": "TRK-0102",
                    "vin": "4SYNTHV1N00000102",
                    "year": "2018"
                }
            }
        }
    ],
    "pagination": {
        "per_page": 2,
        "page_no": 1,
        "total": 3
    }
}
"""

VEHICLE_UTILIZATIONS_PAGE_1_RESPONSE: dict[str, JsonValue] = json.loads(
    VEHICLE_UTILIZATIONS_PAGE_1_RESPONSE_JSON
)

# The terminal page (2 * 2 >= 3): the NULL-VIN vehicle -- the
# utilization surface's vin null arm on the shared VehicleSummary.
VEHICLE_UTILIZATIONS_PAGE_2_RESPONSE_JSON: str = r"""
{
    "vehicle_utilizations": [
        {
            "vehicle_utilization": {
                "driving_fuel": 11.4,
                "driving_time": 7380.0,
                "idle_fuel": 0.9,
                "idle_time": 540.0,
                "last_located_at": "2026-01-05T09:03:47-05:00",
                "message": "",
                "total_distance": 148.2,
                "total_fuel": 12.3,
                "utilization_percentage": 45.1,
                "vehicle": {
                    "id": 9900103,
                    "make": "Volvo",
                    "metric_units": false,
                    "model": "VNL",
                    "number": "TRK-0103",
                    "vin": null,
                    "year": "2022"
                }
            }
        }
    ],
    "pagination": {
        "per_page": 2,
        "page_no": 2,
        "total": 3
    }
}
"""

VEHICLE_UTILIZATIONS_PAGE_2_RESPONSE: dict[str, JsonValue] = json.loads(
    VEHICLE_UTILIZATIONS_PAGE_2_RESPONSE_JSON
)


def _page_records(page: dict[str, JsonValue]) -> list[JsonObject]:
    wrappers = page['vehicle_utilizations']
    assert isinstance(wrappers, list)
    records: list[JsonObject] = []
    for wrapper in wrappers:
        assert isinstance(wrapper, dict)
        record = wrapper['vehicle_utilization']
        assert isinstance(record, dict)
        records.append(record)
    return records


def _stamped(record: JsonObject) -> JsonObject:
    """One record stamped exactly as the decoder stamps: the sent
    window's date labels copied verbatim onto the record."""
    return {
        **record,
        'windowStartDate': WINDOW_START_DATE,
        'windowEndDate': WINDOW_END_DATE,
    }


# All three committed records in page order, WINDOW-STAMPED as the
# decoder emits them (the model's input grain): the active vehicle, the
# inactive zeroed vehicle, the null-VIN vehicle.
VEHICLE_UTILIZATION_RECORDS: list[JsonObject] = [
    _stamped(record)
    for record in _page_records(VEHICLE_UTILIZATIONS_PAGE_1_RESPONSE)
    + _page_records(VEHICLE_UTILIZATIONS_PAGE_2_RESPONSE)
]
