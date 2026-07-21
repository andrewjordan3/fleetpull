# src/fleetpull/models/geotab/driver_change.py
"""GeoTab DriverChange response model (``GetFeed`` on ``typeName: DriverChange``).

Written from the 2026-07-21 feed wave two census (30-day seeded pulls at
the probed tenant), never from docs. A DriverChange is one
driver-to-device assignment event. The type carries a per-record
``version``, and DriverChange records are user-editable through the
provider, so re-emission under newer versions is expected and the
consumer reconciles by ``(id, max version)`` (DESIGN §4).

Requiredness posture (the wave-two conservative stance, DESIGN §8): the
census is a TENANT-SCOPED observation (1,114 records), so structural
requiredness is limited to the record identity — ``id``, ``dateTime``
(the event time), ``version``, and the primary entity ref (``driver``,
the entity the type names) — and every other field is optional EVEN
where the census was total. The observed arms:

- ``driver`` is PROVEN mixed object-or-string (1,114/1,114, both arms
  observed); its object arm carries ``isDriver`` beside the id, so the
  ref model mirrors it (null exactly on string-arm rows). ``device``
  was object-only on this census; both refs ride the shared
  ``bare_id_to_reference`` lift (the census-scope lesson: a tenant
  census cannot prove the string arm absent).

``dateTime`` is recovered tz-aware by validation, the GeoTab sibling
idiom. ``type`` is a census-open vocabulary — a plain str, never an
enum.
"""

from datetime import datetime
from typing import Annotated

from pydantic import BeforeValidator, ConfigDict
from pydantic.alias_generators import to_camel

from fleetpull.model_contract import ResponseModel
from fleetpull.models.geotab.shared import bare_id_to_reference

__all__: list[str] = [
    'DriverChange',
    'DriverChangeDeviceRef',
    'DriverChangeDriverRef',
]


class DriverChangeDeviceRef(ResponseModel):
    """The assignment's device reference.

    Census-observed as an ``{id}`` object on every record; the shared
    coercion lifts a bare string defensively (the census-scope lesson).
    """

    model_config = ConfigDict(alias_generator=to_camel)

    id: str


class DriverChangeDriverRef(ResponseModel):
    """The assignment's driver reference.

    PROVEN mixed on this census: an ``{id, isDriver}`` object or a bare
    id string (e.g. the ``"UnknownDriverId"`` sentinel); the shared
    coercion lifts the bare form to ``{"id": <string>}``, so
    ``is_driver`` is null exactly on string-arm rows.
    """

    model_config = ConfigDict(alias_generator=to_camel)

    id: str
    is_driver: bool | None = None


class DriverChange(ResponseModel):
    """One GeoTab driver-to-device assignment event.

    The wave-two conservative mirror (the module docstring's posture):
    ``id`` / ``date_time`` / ``version`` / ``driver`` required,
    everything else optional even where census-total.

    Attributes:
        date_time: The change's UTC instant — the endpoint's event time.
        device: The assignment's device reference (object-only on this
            census; defensively lifted).
        driver: The assignment's driver reference — proven
            object-or-string, ``is_driver`` null exactly on string-arm
            rows.
        id: GeoTab's record id.
        type: The change-type token (census-open plain str).
        version: The record's version token — the reconcile key beside
            ``id``.
    """

    model_config = ConfigDict(alias_generator=to_camel)

    date_time: datetime
    device: Annotated[
        DriverChangeDeviceRef | None, BeforeValidator(bare_id_to_reference)
    ] = None
    driver: Annotated[DriverChangeDriverRef, BeforeValidator(bare_id_to_reference)]
    id: str
    type: str | None = None
    version: str
