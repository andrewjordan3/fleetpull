"""The committed Samsara addresses capture set (2026-07-20 probe session).

Four FULLY SYNTHETIC address records shaped by the live census (a
full-population walk of ``GET /addresses``: one page, all 25 records,
no null value anywhere), arranged as a two-page cursor walk so the
fixture exercises the continuation shape the production walk never
showed (the standard Samsara cursor contract, proven per-type on
vehicles/drivers). The variants cover every modeled arm: the one
circle-geofence shape (``circle`` was 1/25 in census, mutually
exclusive with ``polygon`` -- never both), three polygon-geofence
shapes (the ``polygon`` block rides the raw fixture UNMODELED -- its
only key is ``vertices``, a list-of-objects, excluded one level down
per the Device/User precedent -- proving ``extra='ignore'`` drops it),
a ``settings`` carrier and a settings-less block (13/25 in census), a
record missing ``addressTypes`` (20/25 in census), and one ``tags``
carrier (9/25, the excluded list-of-objects block).

Unlike the vehicles/drivers capture sets, no record values here are
scrubbed live values -- every id, name, street address, coordinate,
type tag, and timestamp is synthetic outright (the samsara_trips
precedent; address strings and coordinates are PII-adjacent). What IS
verbatim wire truth: the ``data`` + ``pagination {endCursor,
hasNextPage}`` envelope, the camelCase key set, the millisecond
ISO-8601 ``createdAtTime`` shape, the string-id/float-coordinate/
bare-int-``radiusMeters`` types, the vertices-only ``polygon`` shape,
and the TERMINAL pagination shape (``hasNextPage: false`` beside an
EMPTY-STRING ``endCursor``).

Consumed by the Address model tests and the addresses endpoint tests --
kept as a helper module under ``tests/`` so consumers share one capture
set (the ``samsara_vehicles_capture`` precedent). The raw JSON literals
are the captures; the parsed objects beside them are what tests
consume.
"""

import json

from fleetpull.vocabulary import JsonObject, JsonValue

# A continuation page (synthetic split; production returned the whole
# 25-record population in one page at limit 512). Three records: the
# maximal polygon variant (settings, addressTypes, and the excluded
# tags block), the polygon variant MISSING addressTypes (20/25 carried
# it) with no settings, and the one circle-geofence variant (1/25;
# circle and polygon never co-occurred in capture).
ADDRESSES_PAGE_RESPONSE_JSON: str = r"""
{
    "data": [
        {
            "id": "addr-001",
            "name": "Depot North",
            "createdAtTime": "2022-03-15T14:02:33.123Z",
            "formattedAddress": "100 Example St, Example City, TX 75001",
            "latitude": 33.0001,
            "longitude": -97.0001,
            "geofence": {
                "polygon": {
                    "vertices": [
                        {"latitude": 33.0003, "longitude": -97.0003},
                        {"latitude": 33.0003, "longitude": -96.9999},
                        {"latitude": 32.9999, "longitude": -96.9999},
                        {"latitude": 32.9999, "longitude": -97.0003}
                    ]
                },
                "settings": {
                    "showAddresses": true
                }
            },
            "addressTypes": ["yard"],
            "tags": [
                {
                    "id": "6000001",
                    "name": "Region Synthetic - 01",
                    "parentTagId": "6000000"
                }
            ]
        },
        {
            "id": "addr-002",
            "name": "Yard South",
            "createdAtTime": "2023-08-02T09:41:07.456Z",
            "formattedAddress": "200 Example Ave, Example City, TX 75002",
            "latitude": 32.5001,
            "longitude": -96.5001,
            "geofence": {
                "polygon": {
                    "vertices": [
                        {"latitude": 32.5002, "longitude": -96.5002},
                        {"latitude": 32.5002, "longitude": -96.5000},
                        {"latitude": 32.5000, "longitude": -96.5001}
                    ]
                }
            }
        },
        {
            "id": "addr-003",
            "name": "Warehouse East",
            "createdAtTime": "2024-01-19T18:20:59.789Z",
            "formattedAddress": "300 Example Blvd, Example City, TX 75003",
            "latitude": 33.2501,
            "longitude": -96.2501,
            "geofence": {
                "circle": {
                    "latitude": 33.2501,
                    "longitude": -96.2501,
                    "radiusMeters": 150
                },
                "settings": {
                    "showAddresses": false
                }
            },
            "addressTypes": ["yard", "industrial"]
        }
    ],
    "pagination": {
        "endCursor": "00000000-0000-0000-0000-000000000031",
        "hasNextPage": true
    }
}"""

ADDRESSES_PAGE_RESPONSE: dict[str, JsonValue] = json.loads(ADDRESSES_PAGE_RESPONSE_JSON)

# The terminal page shape -- hasNextPage false beside an empty-string
# endCursor (the captured Samsara terminal, verbatim wire truth) --
# carrying one more polygon variant with addressTypes and no settings.
ADDRESSES_TERMINAL_RESPONSE_JSON: str = r"""
{
    "data": [
        {
            "id": "addr-004",
            "name": "Service Center West",
            "createdAtTime": "2025-06-30T21:15:44.012Z",
            "formattedAddress": "400 Example Pkwy, Example City, TX 75004",
            "latitude": 32.7501,
            "longitude": -97.2501,
            "geofence": {
                "polygon": {
                    "vertices": [
                        {"latitude": 32.7503, "longitude": -97.2503},
                        {"latitude": 32.7503, "longitude": -97.2499},
                        {"latitude": 32.7499, "longitude": -97.2501}
                    ]
                }
            },
            "addressTypes": ["yard"]
        }
    ],
    "pagination": {
        "endCursor": "",
        "hasNextPage": false
    }
}"""

ADDRESSES_TERMINAL_RESPONSE: dict[str, JsonValue] = json.loads(
    ADDRESSES_TERMINAL_RESPONSE_JSON
)


def _envelope_records(envelope: dict[str, JsonValue]) -> list[JsonObject]:
    result = envelope['data']
    assert isinstance(result, list)
    records: list[JsonObject] = []
    for record in result:
        assert isinstance(record, dict)
        records.append(record)
    return records


# All four committed records, in fixture order: maximal polygon,
# addressTypes-less polygon, circle, terminal-page polygon.
ADDRESS_RECORDS: list[JsonObject] = _envelope_records(
    ADDRESSES_PAGE_RESPONSE
) + _envelope_records(ADDRESSES_TERMINAL_RESPONSE)
