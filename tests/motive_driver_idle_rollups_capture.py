"""The committed Motive driver_idle_rollups capture set (2026-07-21
probe session).

Three FULLY SYNTHETIC driver idle-rollup records shaped by the live
census of ``GET /v2/driver_utilization`` (100 records sampled,
structurally uniform), arranged as a two-page offset walk so the
fixture exercises the continuation shape at fixture scale (``per_page``
2 here; production uses the configured page size). No value here is a
scrubbed live value -- every id, name, email, and metric is synthetic
outright.

What IS verbatim wire truth: the WIRE'S OWN envelope vocabulary --
wrapper ``driver_idle_rollups``/``driver_idle_rollup``, a DIFFERENT
vocabulary from the ``/v2/driver_utilization`` path -- and the
``pagination {per_page, page_no, total}`` echo shape; the snake_case
six-key record shape; the BARE-INT ``idle_time``/``driving_time``
durations (ints on THIS arm; floats on the vehicle arm); the float
``utilization``/``idle_fuel``/``driving_fuel``; and the ``driver`` ref
carrying EXACTLY the shared ``UserSummary`` 8-key shape
(``{id, first_name, last_name, username, email, driver_company_id,
status, role}``) -- populated on attributed rows (99/100 in census) and
NULL on the unattributed rollup bucket row (1/100), which the fixtures
carry.

Rollup rows carry NO date or time identity of any kind -- the row's
time identity is the request window itself, which the decoder stamps on
as ``windowStartDate``/``windowEndDate`` from the SENT spec's
``start_date``/``end_date`` DATE LABELS (inclusive on both ends,
interpreted in COMPANY-LOCAL days -- DESIGN section 8). The raw pages
below are PRE-STAMP wire truth; the stamped records beside them are
what the model tests consume, stamped exactly as the decoder stamps
(verbatim copies of the notional [2026-01-05, 2026-01-06) one-day
unit's labels: ``start_date=end_date='2026-01-05'``).

The variant coverage: an attributed rollup with the fully populated
ref; an attributed rollup whose ref carries the null arms
(``email``/``driver_company_id`` null); and THE NULL-DRIVER BUCKET row.

Consumed by the DriverIdleRollup model tests and the
driver_idle_rollups endpoint tests -- kept as a helper module under
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

# A continuation page: two attributed rollups -- the fully populated
# ref and the ref with null email/driver_company_id arms. The echo is
# non-terminal under the decoder's ``page_no * per_page >= total`` rule
# (1 * 2 < 3).
DRIVER_IDLE_ROLLUPS_PAGE_1_RESPONSE_JSON: str = r"""
{
    "driver_idle_rollups": [
        {
            "driver_idle_rollup": {
                "utilization": 71.4,
                "idle_time": 1740,
                "driving_time": 20460,
                "idle_fuel": 2.8,
                "driving_fuel": 38.1,
                "driver": {
                    "id": 700101,
                    "first_name": "Synthetic",
                    "last_name": "Driver101",
                    "username": "sdriver101",
                    "email": "synthetic.driver101@example.com",
                    "driver_company_id": "SYN-101",
                    "status": "active",
                    "role": "driver"
                }
            }
        },
        {
            "driver_idle_rollup": {
                "utilization": 58.9,
                "idle_time": 2520,
                "driving_time": 13320,
                "idle_fuel": 3.6,
                "driving_fuel": 24.4,
                "driver": {
                    "id": 700102,
                    "first_name": "Synthetic",
                    "last_name": "Driver102",
                    "username": "sdriver102",
                    "email": null,
                    "driver_company_id": null,
                    "status": "deactivated",
                    "role": "driver"
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

DRIVER_IDLE_ROLLUPS_PAGE_1_RESPONSE: dict[str, JsonValue] = json.loads(
    DRIVER_IDLE_ROLLUPS_PAGE_1_RESPONSE_JSON
)

# The terminal page (2 * 2 >= 3): THE NULL-DRIVER BUCKET row -- the
# unattributed rollup the census found beside the per-driver rows (1 of
# 100 sampled), its metrics present, its ref null.
DRIVER_IDLE_ROLLUPS_PAGE_2_RESPONSE_JSON: str = r"""
{
    "driver_idle_rollups": [
        {
            "driver_idle_rollup": {
                "utilization": 3.5,
                "idle_time": 5400,
                "driving_time": 196,
                "idle_fuel": 6.1,
                "driving_fuel": 0.4,
                "driver": null
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

DRIVER_IDLE_ROLLUPS_PAGE_2_RESPONSE: dict[str, JsonValue] = json.loads(
    DRIVER_IDLE_ROLLUPS_PAGE_2_RESPONSE_JSON
)


def _page_records(page: dict[str, JsonValue]) -> list[JsonObject]:
    wrappers = page['driver_idle_rollups']
    assert isinstance(wrappers, list)
    records: list[JsonObject] = []
    for wrapper in wrappers:
        assert isinstance(wrapper, dict)
        record = wrapper['driver_idle_rollup']
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
# decoder emits them (the model's input grain): the fully attributed
# rollup, the null-arm attributed rollup, the null-driver bucket row.
DRIVER_IDLE_ROLLUP_RECORDS: list[JsonObject] = [
    _stamped(record)
    for record in _page_records(DRIVER_IDLE_ROLLUPS_PAGE_1_RESPONSE)
    + _page_records(DRIVER_IDLE_ROLLUPS_PAGE_2_RESPONSE)
]
