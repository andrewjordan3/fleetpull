# src/fleetpull/models/geotab/shipment_log.py
"""GeoTab ShipmentLog response model (``GetFeed`` on ``typeName: ShipmentLog``).

Written from the 2026-07-21 feed wave three SCALE census (walked at
scale at the probed tenant), never from docs. A ShipmentLog is one
shipment-manifest record attached to a driver and device over an active
window — a versioned feed, so re-emission under newer ``version``
tokens is expected and the consumer reconciles by ``(id, max version)``
(DESIGN §4).

Requiredness posture (the wave-two conservative stance, DESIGN §8): the
census is a TENANT-SCOPED observation (2,771 records), so structural
requiredness is limited to the record identity — ``id``, ``dateTime``
(the event time), ``version``, and the primary entity ref (``driver``,
consistent with the log family) — and every other field is optional
EVEN where the census was total. The observed arms:

- ``device`` and ``driver`` were object-only (``{id}``) at scale; both
  ride the shared ``bare_id_to_reference`` lift regardless (the
  census-scope lesson, DESIGN §8: a tenant census cannot prove the
  string arm absent).

``activeFrom``/``activeTo`` (the shipment's active window) and
``dateTime`` are recovered tz-aware by validation, the GeoTab sibling
idiom. ``commodity``, ``documentNumber``, and ``shipperName`` are
census-open free-text strings.
"""

from datetime import datetime
from typing import Annotated

from pydantic import BeforeValidator, ConfigDict
from pydantic.alias_generators import to_camel

from fleetpull.model_contract import ResponseModel
from fleetpull.models.geotab.shared import bare_id_to_reference

__all__: list[str] = [
    'ShipmentLog',
    'ShipmentLogDeviceRef',
    'ShipmentLogDriverRef',
]


class ShipmentLogDeviceRef(ResponseModel):
    """The shipment's device reference.

    Census-observed as an ``{id}`` object at scale; the shared coercion
    lifts a bare string defensively (the census-scope lesson).
    """

    model_config = ConfigDict(alias_generator=to_camel)

    id: str


class ShipmentLogDriverRef(ResponseModel):
    """The shipment's driver reference.

    Census-observed as an ``{id}`` object at scale; the shared coercion
    lifts a bare string defensively (the census-scope lesson).
    """

    model_config = ConfigDict(alias_generator=to_camel)

    id: str


class ShipmentLog(ResponseModel):
    """One GeoTab shipment-manifest record from the ShipmentLog feed.

    The wave-two conservative mirror (the module docstring's posture):
    ``id`` / ``date_time`` / ``version`` / ``driver`` required,
    everything else optional even where census-total.

    Attributes:
        active_from: The shipment window's UTC start.
        active_to: The shipment window's UTC end.
        commodity: The shipped commodity (census-open str).
        date_time: The record's UTC instant — the endpoint's event time.
        device: The shipment's device reference (object-only at scale;
            defensively lifted).
        document_number: The manifest document number (census-open str).
        driver: The shipment's driver reference (object-only at scale;
            defensively lifted).
        id: GeoTab's record id.
        shipper_name: The shipper company name (census-open str).
        version: The record's version token — the reconcile key beside
            ``id``.
    """

    model_config = ConfigDict(alias_generator=to_camel)

    active_from: datetime | None = None
    active_to: datetime | None = None
    commodity: str | None = None
    date_time: datetime
    device: Annotated[
        ShipmentLogDeviceRef | None, BeforeValidator(bare_id_to_reference)
    ] = None
    document_number: str | None = None
    driver: Annotated[ShipmentLogDriverRef, BeforeValidator(bare_id_to_reference)]
    id: str
    shipper_name: str | None = None
    version: str
