# src/fleetpull/models/geotab/status_data.py
"""GeoTab StatusData response model (``GetFeed`` on ``typeName: StatusData``).

Written from the 2026-07-21 live probe session, never from docs. A
StatusData record is one engine-diagnostic reading — the LogRecord
stream's sibling ACTIVE feed, with one deliberate asymmetry: StatusData
DOES carry a per-record ``version`` (LogRecord carries none), observed on
every census record and mirrored — the consumer still reconciles this
active feed by ``id``, the version riding along as wire truth
(DESIGN §4).

Requiredness posture: the census is a large uniform whole-page total —
2,000/2,000 records carried every key — so every field is required with
no nullable arm (none was observed). The census is a TENANT-SCOPED
observation (DESIGN §8). Volume on the probed tenant is ~24,500
records/hour, which is why the leaf declares the 50,000
protocol-maximum ``resultsLimit``.

``dateTime`` (the event time) is recovered tz-aware by validation, the
GeoTab sibling idiom. ``data`` — the diagnostic's value — is MIXED
int-or-float on the wire and modeled ``float`` (the one dtype that
carries both arms losslessly). ``controller`` is STRING-OR-OBJECT:
the ``"ControllerNoneId"`` sentinel string on 49,745 of a 50,000-record
live full-page census (2026-07-21) and an ``{id}`` reference on the
other 255 — the Trip ``UnknownDriverId`` mechanism verbatim (the
initial one-hour census saw only the sentinel arm; the live proof's
full walk surfaced the object arm — a census-scope lesson recorded in
the section 8 block). The shared ``bare_id_to_reference`` coercion
lifts the sentinel to ``{"id": <string>}``.
"""

from datetime import datetime
from typing import Annotated

from pydantic import BeforeValidator, ConfigDict
from pydantic.alias_generators import to_camel

from fleetpull.model_contract import ResponseModel
from fleetpull.models.geotab.shared import bare_id_to_reference

__all__: list[str] = [
    'StatusData',
    'StatusDataControllerRef',
    'StatusDataDeviceRef',
    'StatusDataDiagnosticRef',
]


class StatusDataControllerRef(ResponseModel):
    """The source controller reference.

    Arrives as an ``{id}`` object (255 of the 50,000-record live
    census) or the bare ``"ControllerNoneId"`` sentinel string
    (49,745), which the shared coercion lifts to ``{"id": <string>}``.
    """

    id: str | None = None


class StatusDataDeviceRef(ResponseModel):
    """The reading's device reference: the id alone, on every record."""

    model_config = ConfigDict(alias_generator=to_camel)

    id: str


class StatusDataDiagnosticRef(ResponseModel):
    """The reading's diagnostic reference: the id alone, on every record."""

    model_config = ConfigDict(alias_generator=to_camel)

    id: str


class StatusData(ResponseModel):
    """One GeoTab diagnostic reading from the StatusData feed.

    A pure mirror of the 2,000/2,000 whole-page census: seven keys, all
    present and non-null on every record, so all seven are required.

    Attributes:
        controller: The source controller reference — an ``{id}``
            object or the bare ``"ControllerNoneId"`` sentinel string,
            lifted by the shared coercion so the sentinel lands as
            ``controller__id`` (census-open
            plain str).
        data: The diagnostic's value — mixed int-or-float on the wire,
            modeled float.
        date_time: The reading's UTC instant — the endpoint's event time.
        device: The emitting vehicle unit's reference.
        diagnostic: The diagnostic definition's reference.
        id: GeoTab's record id.
        version: The record's version token — present on this active
            feed (unlike LogRecord), mirrored as wire truth.
    """

    model_config = ConfigDict(alias_generator=to_camel)

    controller: Annotated[
        StatusDataControllerRef, BeforeValidator(bare_id_to_reference)
    ]
    data: float
    date_time: datetime
    device: StatusDataDeviceRef
    diagnostic: StatusDataDiagnosticRef
    id: str
    version: str
