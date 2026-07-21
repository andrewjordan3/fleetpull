# src/fleetpull/models/geotab/duty_status_log.py
"""GeoTab DutyStatusLog response model (``GetFeed`` on ``typeName: DutyStatusLog``).

Written from the 2026-07-21 feed wave two census (30-day seeded pulls at
the probed tenant), never from docs. A DutyStatusLog is one HOS
duty-status event — an EDITABLE log: ``editDateTime`` is the edit
trail, re-emission under newer ``version`` tokens is expected, and the
consumer reconciles by ``(id, max version)`` (DESIGN §4).

Requiredness posture (the wave-two conservative stance, DESIGN §8): the
census is a TENANT-SCOPED observation, so structural requiredness is
limited to the record identity — ``id``, ``dateTime`` (the event
time), ``version``, and the primary entity ref (``driver``, the log's
subject) — and every other field is optional EVEN where the census was
total (2,000/2,000). The observed arms:

- ``device`` and ``driver`` are PROVEN mixed object-or-string
  (2,000/2,000 each, both arms observed); every reference field rides
  the shared ``bare_id_to_reference`` lift.
- ``annotations`` (126/2,000) carried elements that are EXACTLY
  ``{"id": <str>}`` on every sampled record; the records layer supports
  list-of-scalar only, so the field is an ID-LIST — ``list[str]`` via a
  STRICT element lift (``_annotation_ids``): a shape change fails
  loudly, never silently drops sibling keys. The ids join to the
  ``annotation_logs`` vertical (feed wave three) for the full
  annotation records.
- ``location`` (1,859/2,000) is the shared ``GeotabAddressedLocation``
  wrapper (DVIRLog is the second consumer; DESIGN §8): it carries a
  double-nested ``{location: {x, y}}`` COORDINATE arm or an
  ``{address: {formattedAddress}}`` arm. The 200-sample census saw only
  coordinates; a 24,860-block live-proof walk found 14 address-arm
  blocks (the census-scope lesson), so both arms are modeled optional.
- Mixed int-or-float numerics (``distanceSinceValidCoordinates``,
  ``engineHours``, ``odometer``) model ``float``.

``dateTime``, ``editDateTime``, and ``verifyDateTime`` are recovered
tz-aware by validation, the GeoTab sibling idiom. ``deferralStatus``,
``malfunction``, ``origin``, ``state``, and ``status`` are census-open
vocabularies — plain strs, never enums.
"""

from datetime import datetime
from typing import Annotated

from pydantic import BeforeValidator, ConfigDict
from pydantic.alias_generators import to_camel

from fleetpull.model_contract import ResponseModel
from fleetpull.models.geotab.shared import GeotabAddressedLocation, bare_id_to_reference
from fleetpull.vocabulary import JsonValue

__all__: list[str] = [
    'DutyStatusLog',
    'DutyStatusLogDeviceRef',
    'DutyStatusLogDriverRef',
]


def _annotation_ids(value: JsonValue) -> JsonValue:
    """Strictly lift the ``annotations`` element list to its id list.

    The census shows elements carrying ONLY ``{"id": <str>}``; the lift
    is deliberately STRICT so a wire-shape change fails validation
    loudly instead of silently dropping sibling keys the model never
    saw (the loud-failure doctrine).

    Args:
        value: The raw ``annotations`` wire value.

    Returns:
        ``None`` passthrough (the field is optional), or the element
        ids: a bare-string element passes verbatim, an EXACTLY
        ``{'id': <str>}`` element becomes its id.

    Raises:
        ValueError: The value is not a list, or an element is neither a
            bare string nor exactly ``{'id': <str>}`` (extra keys, other
            shapes); the message names the offending element.
    """
    if value is None:
        return None
    if not isinstance(value, list):
        raise ValueError(f'annotations must be a list, got {type(value).__name__}')
    lifted: list[JsonValue] = []
    for element in value:
        if isinstance(element, str):
            lifted.append(element)
            continue
        if (
            isinstance(element, dict)
            and set(element) == {'id'}
            and isinstance(element['id'], str)
        ):
            lifted.append(element['id'])
            continue
        raise ValueError(
            f'annotations element must be a bare id string or exactly '
            f"{{'id': <str>}}, got {element!r}"
        )
    return lifted


class DutyStatusLogDeviceRef(ResponseModel):
    """The recording device's reference.

    PROVEN mixed on this census: an ``{id}`` object or a bare id
    string, the bare form lifted by the shared coercion so both arms
    land as ``device__id``.
    """

    model_config = ConfigDict(alias_generator=to_camel)

    id: str


class DutyStatusLogDriverRef(ResponseModel):
    """The log's driver reference.

    PROVEN mixed on this census: an ``{id}`` object or a bare id
    string (e.g. the ``"UnknownDriverId"`` sentinel), the bare form
    lifted by the shared coercion so both arms land as ``driver__id``.
    """

    model_config = ConfigDict(alias_generator=to_camel)

    id: str


class DutyStatusLog(ResponseModel):
    """One GeoTab HOS duty-status event from the DutyStatusLog feed.

    The wave-two conservative mirror (the module docstring's posture):
    ``id`` / ``date_time`` / ``version`` / ``driver`` required,
    everything else optional even where census-total.

    Attributes:
        annotations: The annotation-log ids attached to the event — the
            strict id-list reduction (126/2,000 carriers); join to the
            ``annotation_logs`` vertical for the records.
        date_time: The event's UTC instant — the endpoint's event time.
        deferral_minutes: Deferred off-duty minutes.
        deferral_status: The deferral-status token (census-open).
        device: The recording device's reference — proven
            object-or-string.
        distance_since_valid_coordinates: Distance since the last valid
            GPS fix (308/2,000; mixed int-or-float, modeled float).
        driver: The log's driver reference — proven object-or-string.
        edit_date_time: The last edit's UTC instant — the edit trail.
        engine_hours: The engine-hours reading (1,844/2,000; mixed
            int-or-float, modeled float).
        event_code: The ELD event code (1,623/2,000).
        event_record_status: The ELD event-record status.
        event_type: The ELD event type (1,753/2,000).
        id: GeoTab's record id.
        is_ignored: Whether the log is ignored.
        is_transitioning: Whether the log is mid-transition.
        location: The event's nested location (1,859/2,000; the shared
            wrapper — a ``{x, y}`` coordinate arm, x longitude / y
            latitude, or a ``formattedAddress`` arm).
        malfunction: The malfunction token (census-open plain str).
        odometer: The odometer reading (1,863/2,000; mixed
            int-or-float, modeled float).
        origin: The origin token (census-open plain str).
        sequence: The ELD sequence token (1,753/2,000).
        state: The state token (census-open plain str).
        status: The duty-status token (census-open plain str).
        verify_date_time: The driver-verification UTC instant
            (765/2,000).
        version: The record's version token — the editable-log
            reconcile key beside ``id``.
    """

    model_config = ConfigDict(alias_generator=to_camel)

    annotations: Annotated[list[str] | None, BeforeValidator(_annotation_ids)] = None
    date_time: datetime
    deferral_minutes: int | None = None
    deferral_status: str | None = None
    device: Annotated[
        DutyStatusLogDeviceRef | None, BeforeValidator(bare_id_to_reference)
    ] = None
    distance_since_valid_coordinates: float | None = None
    driver: Annotated[DutyStatusLogDriverRef, BeforeValidator(bare_id_to_reference)]
    edit_date_time: datetime | None = None
    engine_hours: float | None = None
    event_code: int | None = None
    event_record_status: int | None = None
    event_type: int | None = None
    id: str
    is_ignored: bool | None = None
    is_transitioning: bool | None = None
    location: GeotabAddressedLocation | None = None
    malfunction: str | None = None
    odometer: float | None = None
    origin: str | None = None
    sequence: str | None = None
    state: str | None = None
    status: str | None = None
    verify_date_time: datetime | None = None
    version: str
