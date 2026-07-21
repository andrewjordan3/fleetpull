"""The committed Samsara asset_locations capture set (2026-07-20 probe session).

Five FULLY SYNTHETIC reading records shaped by the live census of
``GET /assets/location-and-speed/stream`` (a 454-record page census,
nested blocks censused over 300; a 50-id one-hour walk of 2 pages /
701 records), arranged as a two-page cursor walk. Records are already
at the reading grain -- one record per fix, with per-record asset
attribution -- so both pages carry MULTIPLE assets and the same asset
recurs across pages (the cursor walks readings, not assets; no
disjointness exists to mirror). Reading times sit strictly inside a
12:00-13:00Z window -- the probe's half-open ``[startTime, endTime)``
anchoring evidence (min 12:00:03Z / max 12:59:56Z on the live walk).

Every record carries the censused shape exactly: ``happenedAtTime``
(RFC3339 str), ``asset`` whose ONLY key is ``id`` (a STRING -- note
the contrast with idling_events' bare-int ``asset.id``), and
``location`` with ``accuracyMeters`` int-or-float (the live walk
proved floats the census never showed; one fixture record carries
one), ``headingDegrees`` int,
``latitude``/``longitude`` floats, and ``geofence`` an EMPTY OBJECT --
mirrored raw on every fixture record (300/300 zero-keys in census) to
prove the model's extra-ignore drops it. NO speed key appears anywhere
despite the surface's name (``location-and-speed``) -- unobserved in
census, so unmodeled and absent here too.

No record values here are scrubbed live values -- every id, timestamp,
coordinate, heading, and accuracy is synthetic outright (coordinates
are PII-adjacent; the trips precedent). What IS verbatim wire truth:
the ``data`` + ``pagination {endCursor, hasNextPage}`` envelope, the
record key set, the empty ``geofence`` object, the str-shaped
``asset.id``, the fat composite ``endCursor`` shape (opaque, passed
back verbatim as ``after``), and the terminal ``hasNextPage: false``
beside an empty-string ``endCursor``.

Consumed by the AssetLocation model tests and the asset_locations
endpoint tests -- kept as a helper module under ``tests/`` so consumers
share one capture set (the ``samsara_vehicles_capture`` precedent). The
raw JSON literals are the captures; the parsed objects beside them are
what tests consume.
"""

import json

from fleetpull.vocabulary import JsonObject, JsonValue

# A continuation page: three readings across two assets (the same asset
# recurs -- reading grain, per-record attribution), every record
# carrying the empty geofence object. The endCursor mirrors the live
# fat-composite token shape, synthetic content.
ASSET_LOCATIONS_PAGE_RESPONSE_JSON: str = r"""
{
    "data": [
        {
            "happenedAtTime": "2026-01-01T12:00:03.000Z",
            "asset": {
                "id": "281474981110001"
            },
            "location": {
                "accuracyMeters": 4,
                "geofence": {},
                "headingDegrees": 270,
                "latitude": 33.1001,
                "longitude": -96.1001
            }
        },
        {
            "happenedAtTime": "2026-01-01T12:10:20.000Z",
            "asset": {
                "id": "281474981110002"
            },
            "location": {
                "accuracyMeters": 12,
                "geofence": {},
                "headingDegrees": 95,
                "latitude": 33.2001,
                "longitude": -96.2001
            }
        },
        {
            "happenedAtTime": "2026-01-01T12:30:10.000Z",
            "asset": {
                "id": "281474981110001"
            },
            "location": {
                "accuracyMeters": 4,
                "geofence": {},
                "headingDegrees": 0,
                "latitude": 33.1501,
                "longitude": -96.1501
            }
        }
    ],
    "pagination": {
        "endCursor": "eyJzeW50aGV0aWMiOiJjb21wb3NpdGUtY3Vyc29yLTAwMDEiLCJvZmZzZXQiOjN9",
        "hasNextPage": true
    }
}"""

ASSET_LOCATIONS_PAGE_RESPONSE: dict[str, JsonValue] = json.loads(
    ASSET_LOCATIONS_PAGE_RESPONSE_JSON
)

# The terminal page shape -- hasNextPage false beside an empty-string
# endCursor (the provider-wide cursor contract) -- carrying a repeat
# asset from page one plus a third asset (multi-asset pages, reading
# grain).
ASSET_LOCATIONS_TERMINAL_RESPONSE_JSON: str = r"""
{
    "data": [
        {
            "happenedAtTime": "2026-01-01T12:45:33.000Z",
            "asset": {
                "id": "281474981110002"
            },
            "location": {
                "accuracyMeters": 8,
                "geofence": {},
                "headingDegrees": 182,
                "latitude": 33.3001,
                "longitude": -96.3001
            }
        },
        {
            "happenedAtTime": "2026-01-01T12:59:56.000Z",
            "asset": {
                "id": "281474981110003"
            },
            "location": {
                "accuracyMeters": 3.9,
                "geofence": {},
                "headingDegrees": 41,
                "latitude": 33.4001,
                "longitude": -96.4001
            }
        }
    ],
    "pagination": {
        "endCursor": "",
        "hasNextPage": false
    }
}"""

ASSET_LOCATIONS_TERMINAL_RESPONSE: dict[str, JsonValue] = json.loads(
    ASSET_LOCATIONS_TERMINAL_RESPONSE_JSON
)


def _envelope_records(envelope: dict[str, JsonValue]) -> list[JsonObject]:
    result = envelope['data']
    assert isinstance(result, list)
    records: list[JsonObject] = []
    for record in result:
        assert isinstance(record, dict)
        records.append(record)
    return records


# All five committed reading records, in capture order across the walk.
ASSET_LOCATION_RECORDS: list[JsonObject] = _envelope_records(
    ASSET_LOCATIONS_PAGE_RESPONSE
) + _envelope_records(ASSET_LOCATIONS_TERMINAL_RESPONSE)
