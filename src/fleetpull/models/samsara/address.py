# src/fleetpull/models/samsara/address.py
"""Samsara Address response model (``GET /addresses``).

Written from captured live responses (2026-07-20 probe session: a
full-population walk -- one page, all 25 records), never from docs. The
walk observed no null value anywhere -- Samsara omits absent keys
rather than nulling them (the vehicles posture). Because the walk was
the whole population, the vehicles optionality posture applies: the
seven 25/25 keys (``id``, ``name``, ``createdAtTime``,
``formattedAddress``, ``latitude``, ``longitude``, ``geofence``) are
required; ``addressTypes`` (20/25) is optional. ``createdAtTime`` is
UTC ISO-8601 with milliseconds, recovered as a tz-aware datetime by
Pydantic's standard parse.

Within the required ``geofence`` block (presence out of 25 blocks):
``circle`` 1/25 (``{latitude, longitude, radiusMeters}``, all three in
the one carrying block), ``polygon`` 24/25, ``settings`` 13/25
(``{showAddresses}``, present in every carrying block). ``circle`` and
``polygon`` were mutually exclusive in capture (1 vs 24, never both) --
both are mirrored as independent optionals with NO XOR enforcement
(mirror, never interpret).

Excluded fields (``extra='ignore'`` makes exclusion exactly "don't
model it"):

- ``tags`` -- a list of tag objects (9/25); the records layer's schema
  derivation supports scalars, enums, ``list[scalar]``, and nested
  models only (the GeoTab Device/User exclusion precedent, same as the
  vehicles/drivers models) -- modeled when the list-of-structs
  derivation vertical lands.
- ``geofence.polygon`` -- excluded WHOLESALE (24/25): its ONLY key is
  ``vertices``, a list of ``{latitude, longitude}`` objects, so the
  same exclusion precedent applies one level down and an empty polygon
  model would mirror nothing. The top-level ``latitude``/``longitude``
  still carry the address's center point on every record, so a
  polygon-fenced address keeps its location while the boundary awaits
  the list-of-structs vertical.

Wire keys are camelCase; fields are snake_case via the ``to_camel``
alias generator.
"""

from datetime import datetime

from pydantic import ConfigDict
from pydantic.alias_generators import to_camel

from fleetpull.model_contract import ResponseModel

__all__: list[str] = [
    'Address',
    'AddressGeofence',
    'AddressGeofenceCircle',
    'AddressGeofenceSettings',
]


class AddressGeofenceCircle(ResponseModel):
    """The ``geofence.circle`` block: a circular boundary (1/25).

    All three keys were present in the one carrying block. Mutually
    exclusive with ``polygon`` in capture, mirrored without enforcement
    (module docstring).

    Attributes:
        latitude: The circle center's latitude, decimal degrees.
        longitude: The circle center's longitude, decimal degrees.
        radius_meters: The circle's radius in meters, a bare int.
    """

    model_config = ConfigDict(alias_generator=to_camel)

    latitude: float
    longitude: float
    radius_meters: int


class AddressGeofenceSettings(ResponseModel):
    """The ``geofence.settings`` block: geofence display settings (13/25).

    Attributes:
        show_addresses: Whether the geofence displays addresses --
            present in every carrying block.
    """

    model_config = ConfigDict(alias_generator=to_camel)

    show_addresses: bool


class AddressGeofence(ResponseModel):
    """The ``geofence`` block: boundary and settings (present on all 25).

    Models ``circle`` and ``settings`` only -- ``polygon`` is excluded
    wholesale because its only content is a list-of-objects vertex list
    (module docstring), so a polygon-fenced address validates to a
    geofence with both modeled fields ``None`` while its center point
    survives on the parent's ``latitude``/``longitude``.

    Attributes:
        circle: The circular boundary (1/25; mutually exclusive with
            the unmodeled ``polygon`` in capture, not enforced).
        settings: The display-settings block (13/25).
    """

    circle: AddressGeofenceCircle | None = None
    settings: AddressGeofenceSettings | None = None


class Address(ResponseModel):
    """One Samsara defined address (a named location with a geofence).

    A pure mirror of the captured fields (``tags`` and
    ``geofence.polygon`` excluded, module docstring). Field semantics
    are Samsara's; no value is derived or interpreted here. The walk
    was the whole 25-record population, so the always-present keys are
    required (the vehicles posture).

    Attributes:
        id: Samsara's address id -- a string, mirrored as string.
        name: The address's display name.
        created_at_time: Record creation (UTC, millisecond ISO-8601).
        formatted_address: The full street address, one formatted
            string.
        latitude: The address's center-point latitude, decimal degrees
            -- carried on every record, polygon-fenced ones included.
        longitude: The address's center-point longitude, decimal
            degrees.
        geofence: The geofence block (circle/settings modeled; polygon
            excluded).
        address_types: The address's type tags, a list of strings
            (20/25).
    """

    model_config = ConfigDict(alias_generator=to_camel)

    # Identity and lifecycle.
    id: str
    name: str
    created_at_time: datetime

    # Location.
    formatted_address: str
    latitude: float
    longitude: float

    # Boundary and classification.
    geofence: AddressGeofence
    address_types: list[str] | None = None
