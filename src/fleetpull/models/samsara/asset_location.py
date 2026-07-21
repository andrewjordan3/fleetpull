# src/fleetpull/models/samsara/asset_location.py
"""Samsara AssetLocation response model
(``GET /assets/location-and-speed/stream``).

Written from captured live responses (2026-07-20 probe session: a
454-record page census with every nested block censused over 300
records; a 50-id one-hour walk of 2 pages / 701 records), never from
docs. The legacy hub called this surface ``location_stream``; the
catalog name is ``asset_locations`` per the name=plural-of-entity
invariant -- the stored entity is the asset-location reading, one row
per fix.

The record census (454/454 unless noted): ``happenedAtTime`` (RFC3339
str, recovered tz-aware UTC -- the event-time column; readings fall
strictly inside the requested half-open ``[startTime, endTime)``
window, probe-proven on a 12:00-13:00Z window returning min 12:00:03Z /
max 12:59:56Z), ``asset`` (an object whose ONLY observed key is ``id``,
a STRING on the wire, 300/300 -- note the contrast with idling_events'
bare-int ``asset.id``: per-endpoint wire truth, mirrored per endpoint),
and ``location`` (300/300 census within the block: ``accuracyMeters``
int on every censused record but FLOAT on the live walk -- the
2026-07-20 full-day live proof failed validation on a float value at
record 351, so the field is float, the census sample proven narrower
than the wire; ``headingDegrees`` int, ``latitude`` float,
``longitude`` float).

Requiredness posture: 300/300 on a 454-record page is NOT a
whole-population oath (the drivers conservative posture would leave
everything optional), but the location core is required anyway by
structural judgment -- a location record without coordinates mirrors
nothing and is structurally meaningless, so a future record omitting
them should fail loudly rather than land an all-null coordinate row.
The same judgment covers ``asset``/``asset.id`` (an unattributed
reading cannot be a reading of anything) and ``happenedAtTime`` (the
event-time column the watermark routes on). This is the one deliberate
departure from the pure conservative posture, recorded here and in
DESIGN section 8.

Not modeled, with different provenance:

- ``location.geofence`` -- OBSERVED-EMPTY, not excluded: present
  300/300 but an empty object with ZERO keys on every censused record,
  so there is nothing to mirror (``extra='ignore'`` drops it). Revisit
  on a capture showing content.
- Any speed key -- UNOBSERVED despite the surface's name
  (``location-and-speed``): no speed key appeared anywhere in the
  census. Unmodeled as unobserved (never excluded); revisit on a
  capture that shows one.

Wire keys are camelCase; fields are snake_case via the ``to_camel``
alias generator.
"""

from datetime import datetime

from pydantic import ConfigDict
from pydantic.alias_generators import to_camel

from fleetpull.model_contract import ResponseModel

__all__: list[str] = [
    'AssetLocation',
    'AssetLocationAssetRef',
    'AssetLocationFix',
]


class AssetLocationAssetRef(ResponseModel):
    """The ``asset`` block: the reading's asset reference.

    Its ONLY observed key is ``id`` (300/300) -- a STRING on the wire,
    unlike idling_events' bare-int ``asset.id`` (per-endpoint wire
    truth, mirrored per endpoint).

    Attributes:
        id: Samsara's asset id -- a string, mirrored as string.
    """

    id: str


class AssetLocationFix(ResponseModel):
    """The ``location`` block: one position fix.

    All four modeled keys were 300/300 in the block census and are
    required by structural judgment (module docstring): a fix without
    coordinates mirrors nothing. The block's ``geofence`` key is
    observed-empty (an empty object on every censused record) and is
    not modeled -- there is nothing to mirror.

    Attributes:
        accuracy_meters: The fix's reported accuracy in meters --
            float: the census saw only bare ints (300/300), but the
            live full-day walk carried floats (proven 2026-07-20, the
            validation failure that widened this field).
        heading_degrees: Heading in degrees, a bare int.
        latitude: Fix latitude, decimal degrees.
        longitude: Fix longitude, decimal degrees.
    """

    model_config = ConfigDict(alias_generator=to_camel)

    accuracy_meters: float
    heading_degrees: int
    latitude: float
    longitude: float


class AssetLocation(ResponseModel):
    """One location reading of one asset, at the reading grain.

    A pure mirror of the captured record (``location.geofence``
    observed-empty and any speed key unobserved -- module docstring).
    Field semantics and units are Samsara's; no value is derived or
    interpreted here.

    Attributes:
        happened_at_time: The reading instant (RFC3339, recovered
            tz-aware UTC) -- the event-time column; readings fall
            strictly inside the requested window.
        asset: The asset reference (string id) -- the per-record
            attribution that makes the batched fan-out pure transport
            packing.
        location: The position fix block.
    """

    model_config = ConfigDict(alias_generator=to_camel)

    happened_at_time: datetime
    asset: AssetLocationAssetRef
    location: AssetLocationFix
