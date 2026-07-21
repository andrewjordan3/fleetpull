# src/fleetpull/models/geotab/dvir_log.py
"""GeoTab DVIRLog response model (``GetFeed`` on ``typeName: DVIRLog``).

Written from the 2026-07-21 feed wave two census (30-day seeded pulls at
the probed tenant), never from docs. A DVIRLog is one driver vehicle
inspection report. The house class casing is ``DvirLog``; the wire
``typeName`` stays ``'DVIRLog'`` (the binding's constant). DVIRs are
certified and edited after creation, so re-emission under newer
``version`` tokens is expected and the consumer reconciles by
``(id, max version)`` (DESIGN §4).

Requiredness posture (the wave-two conservative stance, DESIGN §8): the
census is a TENANT-SCOPED observation (500 records), so structural
requiredness is limited to the record identity — ``id``, ``dateTime``
(the event time), ``version``, and the primary entity ref (``driver``,
500/500; ``device`` is only 205/500 and could not be it) — and every
other field is optional EVEN where the census was total. The observed
arms and exclusions:

- ``device`` is OPTIONAL and commonly absent (205/500 carriers) — a
  plain wire fact of this census, recorded without speculation.
  ``engineHours`` and ``odometer`` travel with it (205/500 each);
  ``engineHours`` was int-only on its carriers, but its sibling
  surface (``DutyStatusLog``) proved the same physical quantity MIXED
  int-or-float, so it models ``float`` here too (cross-surface dtype
  consistency over a thin single-surface census).
- ``defectList`` is a WIRE-PLURAL NAME carrying ONE defect node
  ``{children, id, name}``. The node models ``id`` + ``name`` ONLY:
  ``children`` is EXCLUDED under the documented-exclusion doctrine —
  an EMPTY list on every one of the 200 ``defectList`` nodes the
  census sampled (the nested-block sample depth; ``defectList`` itself
  was present 500/500), so its element shape is unobservable at this
  tenant, and the records layer deliberately supports only observable
  shapes; ``extra='ignore'`` absorbs it wire-side (a record with
  populated children still validates — the pinned absorption test).
  REVISIT when a tenant shows populated ``children``: capture the
  element shape and model it then.
- ``location`` (496/500) is the shared ``GeotabAddressedLocation``
  wrapper (DutyStatusLog is the co-consumer; DESIGN §8): a
  ``{location: {x, y}}`` coordinate arm or an
  ``{address: {formattedAddress}}`` arm. This surface showed only the
  coordinate arm on the live-proof walk, but rides the shared wrapper
  whose address arm DutyStatusLog proved.
- ``duration`` is an opaque duration STRING mirrored verbatim, NOT
  parsed through ``GeotabTimeSpan`` despite that sharing GeoTab's
  provider. The census observed only the wire TYPE (``str``, 500/500),
  never the value FORMAT — unlike ``Trip.duration``, whose TimeSpan
  grammar was probe-confirmed (2026-07-13). Applying the strict
  TimeSpan parser to an unobserved format would crash every record if
  it differs, so the conservative mirror holds until a probe settles
  the format. ``trailer`` (295/500) is an ``{id}`` ref.
- Every reference field (``device``, ``driver``, ``trailer``) rides
  the shared ``bare_id_to_reference`` lift (the census-scope lesson: a
  tenant census cannot prove the string arm absent).

``dateTime`` is recovered tz-aware by validation, the GeoTab sibling
idiom. ``logType`` is a census-open vocabulary — a plain str, never an
enum.
"""

from datetime import datetime
from typing import Annotated

from pydantic import BeforeValidator, ConfigDict
from pydantic.alias_generators import to_camel

from fleetpull.model_contract import ResponseModel
from fleetpull.models.geotab.shared import GeotabAddressedLocation, bare_id_to_reference

__all__: list[str] = [
    'DvirLog',
    'DvirLogDefectList',
    'DvirLogDeviceRef',
    'DvirLogDriverRef',
    'DvirLogTrailerRef',
]


class DvirLogDefectList(ResponseModel):
    """The ``defectList`` block: a WIRE-PLURAL NAME, ONE defect node.

    Despite the plural wire key, the census shows ONE object —
    ``{children, id, name}`` — mirrored as a single nested model, never
    a list. ``children`` is excluded (never populated on the whole
    census; the module docstring's documented-exclusion record); ``id``
    and ``name`` are required within the block — a defect node without
    its identity is a shape change and must fail loudly.
    """

    model_config = ConfigDict(alias_generator=to_camel)

    id: str
    name: str


class DvirLogDeviceRef(ResponseModel):
    """The inspected vehicle unit's reference.

    Census-observed as an ``{id}`` object on every carrier; the shared
    coercion lifts a bare string defensively (the census-scope lesson).
    """

    model_config = ConfigDict(alias_generator=to_camel)

    id: str


class DvirLogDriverRef(ResponseModel):
    """The inspecting driver's reference: the id alone, on every record."""

    model_config = ConfigDict(alias_generator=to_camel)

    id: str


class DvirLogTrailerRef(ResponseModel):
    """The inspected trailer's reference: the id alone, on every carrier."""

    model_config = ConfigDict(alias_generator=to_camel)

    id: str


class DvirLog(ResponseModel):
    """One GeoTab driver vehicle inspection report from the DVIRLog feed.

    The wave-two conservative mirror (the module docstring's posture):
    ``id`` / ``date_time`` / ``version`` / ``driver`` required,
    everything else optional even where census-total.

    Attributes:
        authority_address: The certifying authority's address.
        authority_name: The certifying authority's name.
        certify_remark: The certification remark.
        date_time: The inspection's UTC instant — the endpoint's event
            time.
        defect_list: The plural-named single defect node (``children``
            excluded — the module docstring's record).
        device: The inspected vehicle unit's reference (205/500 — the
            observed wire fact).
        driver: The inspecting driver's reference.
        driver_remark: The driver's remark.
        duration: The inspection duration — an opaque wire string
            mirrored verbatim (the value format is unobserved, so it is
            not parsed through ``GeotabTimeSpan`` the way ``Trip`` is;
            module docstring).
        engine_hours: The engine-hours reading (205/500; modeled float
            per the cross-surface mixed-numeric proof).
        id: GeoTab's record id.
        is_inspected_by_driver: Whether the driver performed the
            inspection.
        location: The inspection's nested location (496/500; the shared
            wrapper — a ``{x, y}`` coordinate arm, x longitude / y
            latitude, or a ``formattedAddress`` arm).
        log_type: The inspection-type token (census-open plain str).
        odometer: The odometer reading (205/500; modeled float).
        trailer: The inspected trailer's reference (295/500).
        version: The record's version token — the certified/edited-log
            reconcile key beside ``id``.
    """

    model_config = ConfigDict(alias_generator=to_camel)

    authority_address: str | None = None
    authority_name: str | None = None
    certify_remark: str | None = None
    date_time: datetime
    defect_list: DvirLogDefectList | None = None
    device: Annotated[
        DvirLogDeviceRef | None, BeforeValidator(bare_id_to_reference)
    ] = None
    driver: Annotated[DvirLogDriverRef, BeforeValidator(bare_id_to_reference)]
    driver_remark: str | None = None
    duration: str | None = None
    engine_hours: float | None = None
    id: str
    is_inspected_by_driver: bool | None = None
    location: GeotabAddressedLocation | None = None
    log_type: str | None = None
    odometer: float | None = None
    trailer: Annotated[
        DvirLogTrailerRef | None, BeforeValidator(bare_id_to_reference)
    ] = None
    version: str
