"""The committed Motive users capture set (2026-07-21 probe session).

Four FULLY SYNTHETIC user records shaped by the live census (a
whole-population walk of ``GET /v1/users``: 2,665 records, 27 pages at
``per_page`` 100; the shape perfectly role-partitioned — driver records
carry the driver-only key block, admin and fleet_user records carry
exactly the shared keys, zero partial-presence keys within any role),
arranged as a two-page offset walk (``per_page`` 2 here; production uses
the configured page size). This surface is PII-heavy — names, emails,
phones, driver-license numbers, addresses — so NO value here is a
scrubbed live value: every identity is an obvious invention (Synthetic
name families, ``@example.com`` emails, 555 phones, ``D0000000``-pattern
license numbers, invented street names). Free-string categorical tokens
beyond ``role``/``status`` (``duty_status``, ``eld_mode``, ``cycle``,
``violation_alerts``) are typed-correct placeholder inventions, not
censused vocabulary.

What IS verbatim wire truth: the ``users``/``user`` wrapped-list
envelope and the ``pagination {per_page, page_no, total}`` echo shape;
the snake_case key sets and their role partition (the driver-only block
present on exactly the ``role: "driver"`` records, ABSENT — not null —
elsewhere); the censused ``role`` (``driver``/``admin``/``fleet_user``)
and ``status`` (``active``/``deactivated``) vocabularies; the null arms
(``email``, ``phone``, ``time_zone``, the sign-in timestamps, and the
nullable driver keys); the maximal driver's populated ``joined_at``
date-only value and ``cycle2`` HOS token (34 and 37 census carriers,
with the null arms on the second driver); and the six never-populated
keys
(``associated_dispatcher_id``, ``expires_at``, ``external_ids``,
``phone2``, ``phone_country_code2``, ``phone_ext``) riding every record
as null — excluded from the model as value-unobservable, carried here to
prove ``extra='ignore'`` drops them.

The variants cover every modeled arm: a maximal active driver (every
driver key populated where the census allows, populated ``group_ids``),
a deactivated admin (web sign-ins present, mobile null, empty
``group_ids``), an active fleet_user (``email``/``time_zone`` null), and
a deactivated driver exercising the driver block's null arms. Consumed
by the User model tests and the users endpoint tests — kept as a helper
module under ``tests/`` so consumers share one capture set (the
``motive_driving_periods_capture`` precedent). The raw JSON literals are
the captures; the parsed objects beside them are what tests consume.
"""

import json

from fleetpull.vocabulary import JsonObject, JsonValue

# A continuation page: the maximal driver and the deactivated admin.
# The echo is non-terminal under the decoder's
# ``page_no * per_page >= total`` rule (1 * 2 < 4).
USERS_PAGE_1_RESPONSE_JSON: str = r"""
{
    "users": [
        {
            "user": {
                "id": 800001,
                "first_name": "Synthetic",
                "last_name": "Driver001",
                "email": "synthetic.driver001@example.com",
                "phone": "555-0100",
                "phone_country_code": "+1",
                "phone2": null,
                "phone_country_code2": null,
                "phone_ext": null,
                "company_reference_id": "REF-0001",
                "time_zone": "Central Time (US & Canada)",
                "metric_units": false,
                "role": "driver",
                "status": "active",
                "group_ids": [61001, 61002],
                "created_at": "2024-02-01T15:04:05Z",
                "updated_at": "2026-07-01T08:30:00Z",
                "mobile_current_sign_in_at": "2026-07-20T11:05:00Z",
                "mobile_last_active_at": "2026-07-20T11:45:00Z",
                "mobile_last_sign_in_at": "2026-07-18T09:00:00Z",
                "web_current_sign_in_at": null,
                "web_last_active_at": null,
                "web_last_sign_in_at": null,
                "associated_dispatcher_id": null,
                "expires_at": null,
                "external_ids": null,
                "username": "synthetic.driver001",
                "driver_company_id": "10001-SYN",
                "drivers_license_number": "D0000001",
                "drivers_license_state": "TX",
                "joined_at": "2023-04-12",
                "duty_status": "off_duty",
                "eld_mode": "logs",
                "cycle": "70_8",
                "cycle2": "70_8_2020",
                "violation_alerts": "1_hour",
                "carrier_name": "Synthetic Carrier LLC",
                "carrier_street": "500 Synthetic Yard Rd",
                "carrier_city": "Exampleville",
                "carrier_state": "TX",
                "carrier_zip": "10001",
                "terminal_street": "501 Synthetic Terminal Ave",
                "terminal_city": "Exampleville",
                "terminal_state": "TX",
                "terminal_zip": "10002",
                "exception_24_hour_restart": false,
                "exception_8_hour_break": false,
                "exception_adverse_driving": true,
                "exception_ca_farm_school_bus": false,
                "exception_short_haul": false,
                "exception_wait_time": false,
                "exception_24_hour_restart2": false,
                "exception_8_hour_break2": false,
                "exception_adverse_driving2": false,
                "exception_ca_farm_school_bus2": false,
                "exception_short_haul2": false,
                "exception_wait_time2": false,
                "export_combined": true,
                "export_odometers": false,
                "export_recap": true,
                "manual_driving_enabled": false,
                "minute_logs": true,
                "personal_conveyance_enabled": true,
                "yard_moves_enabled": true
            }
        },
        {
            "user": {
                "id": 800002,
                "first_name": "Synthetic",
                "last_name": "Admin001",
                "email": "synthetic.admin001@example.com",
                "phone": null,
                "phone_country_code": null,
                "phone2": null,
                "phone_country_code2": null,
                "phone_ext": null,
                "company_reference_id": null,
                "time_zone": "Eastern Time (US & Canada)",
                "metric_units": false,
                "role": "admin",
                "status": "deactivated",
                "group_ids": [],
                "created_at": "2023-01-15T12:00:00Z",
                "updated_at": "2025-11-02T16:20:00Z",
                "mobile_current_sign_in_at": null,
                "mobile_last_active_at": null,
                "mobile_last_sign_in_at": null,
                "web_current_sign_in_at": "2025-10-30T13:00:00Z",
                "web_last_active_at": "2025-10-30T14:10:00Z",
                "web_last_sign_in_at": "2025-10-28T09:30:00Z",
                "associated_dispatcher_id": null,
                "expires_at": null,
                "external_ids": null
            }
        }
    ],
    "pagination": {
        "per_page": 2,
        "page_no": 1,
        "total": 4
    }
}
"""

USERS_PAGE_1_RESPONSE: dict[str, JsonValue] = json.loads(USERS_PAGE_1_RESPONSE_JSON)

# The terminal page: the fleet_user and the null-arm driver. The echo
# is terminal (2 * 2 >= 4).
USERS_PAGE_2_RESPONSE_JSON: str = r"""
{
    "users": [
        {
            "user": {
                "id": 800003,
                "first_name": "Synthetic",
                "last_name": "FleetUser001",
                "email": null,
                "phone": "555-0101",
                "phone_country_code": "+1",
                "phone2": null,
                "phone_country_code2": null,
                "phone_ext": null,
                "company_reference_id": "REF-0002",
                "time_zone": null,
                "metric_units": true,
                "role": "fleet_user",
                "status": "active",
                "group_ids": [61001],
                "created_at": "2025-03-10T10:00:00Z",
                "updated_at": "2026-06-15T18:45:00Z",
                "mobile_current_sign_in_at": null,
                "mobile_last_active_at": null,
                "mobile_last_sign_in_at": null,
                "web_current_sign_in_at": "2026-06-15T18:00:00Z",
                "web_last_active_at": "2026-06-15T18:44:00Z",
                "web_last_sign_in_at": "2026-06-10T07:15:00Z",
                "associated_dispatcher_id": null,
                "expires_at": null,
                "external_ids": null
            }
        },
        {
            "user": {
                "id": 800004,
                "first_name": "Synthetic",
                "last_name": "Driver002",
                "email": null,
                "phone": null,
                "phone_country_code": null,
                "phone2": null,
                "phone_country_code2": null,
                "phone_ext": null,
                "company_reference_id": null,
                "time_zone": "Central Time (US & Canada)",
                "metric_units": false,
                "role": "driver",
                "status": "deactivated",
                "group_ids": [],
                "created_at": "2022-09-05T09:12:00Z",
                "updated_at": "2024-12-20T17:00:00Z",
                "mobile_current_sign_in_at": null,
                "mobile_last_active_at": null,
                "mobile_last_sign_in_at": null,
                "web_current_sign_in_at": null,
                "web_last_active_at": null,
                "web_last_sign_in_at": null,
                "associated_dispatcher_id": null,
                "expires_at": null,
                "external_ids": null,
                "username": null,
                "driver_company_id": null,
                "drivers_license_number": null,
                "drivers_license_state": null,
                "joined_at": null,
                "duty_status": "off_duty",
                "eld_mode": "logs",
                "cycle": null,
                "cycle2": null,
                "violation_alerts": "immediate",
                "carrier_name": "Synthetic Carrier LLC",
                "carrier_street": "500 Synthetic Yard Rd",
                "carrier_city": "Exampleville",
                "carrier_state": "TX",
                "carrier_zip": "10001",
                "terminal_street": null,
                "terminal_city": null,
                "terminal_state": null,
                "terminal_zip": null,
                "exception_24_hour_restart": false,
                "exception_8_hour_break": false,
                "exception_adverse_driving": false,
                "exception_ca_farm_school_bus": false,
                "exception_short_haul": false,
                "exception_wait_time": false,
                "exception_24_hour_restart2": false,
                "exception_8_hour_break2": false,
                "exception_adverse_driving2": false,
                "exception_ca_farm_school_bus2": false,
                "exception_short_haul2": false,
                "exception_wait_time2": false,
                "export_combined": false,
                "export_odometers": false,
                "export_recap": false,
                "manual_driving_enabled": false,
                "minute_logs": false,
                "personal_conveyance_enabled": false,
                "yard_moves_enabled": false
            }
        }
    ],
    "pagination": {
        "per_page": 2,
        "page_no": 2,
        "total": 4
    }
}
"""

USERS_PAGE_2_RESPONSE: dict[str, JsonValue] = json.loads(USERS_PAGE_2_RESPONSE_JSON)


def _page_records(page: dict[str, JsonValue]) -> list[JsonObject]:
    wrappers = page['users']
    assert isinstance(wrappers, list)
    records: list[JsonObject] = []
    for wrapper in wrappers:
        assert isinstance(wrapper, dict)
        record = wrapper['user']
        assert isinstance(record, dict)
        records.append(record)
    return records


# The four records in page order -- what most tests iterate.
USER_RECORDS: list[JsonObject] = [
    *_page_records(USERS_PAGE_1_RESPONSE),
    *_page_records(USERS_PAGE_2_RESPONSE),
]

# The role split, by fixture position: the maximal driver, the admin,
# the fleet_user, the null-arm driver.
USER_DRIVER_MAXIMAL_RECORD: JsonObject = USER_RECORDS[0]
USER_ADMIN_RECORD: JsonObject = USER_RECORDS[1]
USER_FLEET_USER_RECORD: JsonObject = USER_RECORDS[2]
USER_DRIVER_NULL_ARM_RECORD: JsonObject = USER_RECORDS[3]
