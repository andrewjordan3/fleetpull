"""The committed Motive groups capture set (2026-07-21 probe session).

Five FULLY SYNTHETIC group records shaped by the live census (a
whole-population walk of ``GET /v1/groups``: 152 records, four pages at
``per_page`` 50; every key present on all 152), arranged as a two-page
offset walk so the fixture exercises the continuation shape at fixture
scale (``per_page`` 3 here; production uses the configured page size).
Unlike the driving_periods/idle_events capture sets, no value here is a
scrubbed live value — every id, name, and email is synthetic outright
(the samsara_addresses precedent; group names and owner identities are
PII-adjacent, and this repository is public).

What IS verbatim wire truth: the ``groups``/``group`` wrapped-list
envelope and the ``pagination {per_page, page_no, total}`` echo shape;
the snake_case key set (five record keys, eight owner-ref sub-keys); the
int-id/str-name types; ``parent_id`` null on root groups and an existing
group id otherwise (the tree shape); the owner ref's ``email`` null arm;
and ``username`` / ``driver_company_id`` null on EVERY owner ref — the
census observed them null on all 152 records, so the model excludes both
as value-unobservable and the fixtures carry the nulls to prove
``extra='ignore'`` drops them.

The variants cover every modeled arm: the root group (``parent_id``
null), child groups pointing at it, the owner-ref ``email`` present and
null arms, and a deactivated owner. Consumed by the Group model tests
and the groups endpoint tests — kept as a helper module under ``tests/``
so consumers share one capture set (the ``motive_driving_periods_capture``
precedent). The raw JSON literals are the captures; the parsed objects
beside them are what tests consume.
"""

import json

from fleetpull.vocabulary import JsonObject, JsonValue

# A continuation page: the root group and two children. The echo is
# non-terminal under the decoder's ``page_no * per_page >= total`` rule
# (1 * 3 < 5).
GROUPS_PAGE_1_RESPONSE_JSON: str = r"""
{
    "groups": [
        {
            "group": {
                "id": 90001,
                "company_id": 4200,
                "name": "Synthetic Fleet",
                "parent_id": null,
                "user": {
                    "id": 700001,
                    "first_name": "Synthetic",
                    "last_name": "Admin001",
                    "email": "synthetic.admin001@example.com",
                    "username": null,
                    "role": "admin",
                    "status": "active",
                    "driver_company_id": null
                }
            }
        },
        {
            "group": {
                "id": 90002,
                "company_id": 4200,
                "name": "Synthetic Region North",
                "parent_id": 90001,
                "user": {
                    "id": 700001,
                    "first_name": "Synthetic",
                    "last_name": "Admin001",
                    "email": "synthetic.admin001@example.com",
                    "username": null,
                    "role": "admin",
                    "status": "active",
                    "driver_company_id": null
                }
            }
        },
        {
            "group": {
                "id": 90003,
                "company_id": 4200,
                "name": "Synthetic Region South",
                "parent_id": 90001,
                "user": {
                    "id": 700002,
                    "first_name": "Synthetic",
                    "last_name": "FleetUser001",
                    "email": null,
                    "username": null,
                    "role": "fleet_user",
                    "status": "active",
                    "driver_company_id": null
                }
            }
        }
    ],
    "pagination": {
        "per_page": 3,
        "page_no": 1,
        "total": 5
    }
}
"""

GROUPS_PAGE_1_RESPONSE: dict[str, JsonValue] = json.loads(GROUPS_PAGE_1_RESPONSE_JSON)

# The terminal page: two more children, one owned by a deactivated
# account. The echo is terminal (2 * 3 >= 5).
GROUPS_PAGE_2_RESPONSE_JSON: str = r"""
{
    "groups": [
        {
            "group": {
                "id": 90004,
                "company_id": 4200,
                "name": "Synthetic Yard Crew",
                "parent_id": 90002,
                "user": {
                    "id": 700003,
                    "first_name": "Synthetic",
                    "last_name": "Admin002",
                    "email": "synthetic.admin002@example.com",
                    "username": null,
                    "role": "admin",
                    "status": "deactivated",
                    "driver_company_id": null
                }
            }
        },
        {
            "group": {
                "id": 90005,
                "company_id": 4200,
                "name": "Synthetic Long Haul",
                "parent_id": 90002,
                "user": {
                    "id": 700001,
                    "first_name": "Synthetic",
                    "last_name": "Admin001",
                    "email": "synthetic.admin001@example.com",
                    "username": null,
                    "role": "admin",
                    "status": "active",
                    "driver_company_id": null
                }
            }
        }
    ],
    "pagination": {
        "per_page": 3,
        "page_no": 2,
        "total": 5
    }
}
"""

GROUPS_PAGE_2_RESPONSE: dict[str, JsonValue] = json.loads(GROUPS_PAGE_2_RESPONSE_JSON)


def _page_records(page: dict[str, JsonValue]) -> list[JsonObject]:
    wrappers = page['groups']
    assert isinstance(wrappers, list)
    records: list[JsonObject] = []
    for wrapper in wrappers:
        assert isinstance(wrapper, dict)
        record = wrapper['group']
        assert isinstance(record, dict)
        records.append(record)
    return records


# The five records in page order -- what most tests iterate.
GROUP_RECORDS: list[JsonObject] = [
    *_page_records(GROUPS_PAGE_1_RESPONSE),
    *_page_records(GROUPS_PAGE_2_RESPONSE),
]
