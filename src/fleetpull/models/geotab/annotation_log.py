# src/fleetpull/models/geotab/annotation_log.py
"""GeoTab AnnotationLog response model (``GetFeed`` on ``typeName: AnnotationLog``).

Written from the 2026-07-21 feed wave three SCALE census (walked at
scale at the probed tenant), never from docs. An AnnotationLog is one
free-text annotation attached to an HOS duty-status log — a versioned
feed (annotations are user-editable), so re-emission under newer
``version`` tokens is expected and the consumer reconciles by
``(id, max version)`` (DESIGN §4).

This vertical COMPLETES the wave-two ``duty_status_logs`` loop: a
DutyStatusLog carries an ``annotations`` id-list (the strict
``list[str]`` reduction), and each AnnotationLog's ``dutyStatusLog.id``
points BACK to the DutyStatusLog it annotates — a bidirectional join
across the two verticals (``annotation_logs.duty_status_log__id`` ↔
``duty_status_logs.annotations``).

Requiredness posture (the wave-two conservative stance, DESIGN §8): the
census is a TENANT-SCOPED observation (8,857 records), so structural
requiredness is limited to the record identity — ``id``, ``dateTime``
(the event time), ``version``, and the primary entity ref
(``dutyStatusLog``, the annotated log — the annotation's subject) — and
every other field is optional EVEN where the census was total. The
observed arms:

- ``dutyStatusLog`` and ``driver`` were object-only (``{id}``) at
  scale; both ride the shared ``bare_id_to_reference`` lift regardless
  (the census-scope lesson, DESIGN §8: a tenant census cannot prove
  the string arm absent, and the lift is structural and
  sentinel-agnostic).

``dateTime`` is recovered tz-aware by validation, the GeoTab sibling
idiom. ``comment`` is a census-open free-text string.
"""

from datetime import datetime
from typing import Annotated

from pydantic import BeforeValidator, ConfigDict
from pydantic.alias_generators import to_camel

from fleetpull.model_contract import ResponseModel
from fleetpull.models.geotab.shared import bare_id_to_reference

__all__: list[str] = [
    'AnnotationLog',
    'AnnotationLogDriverRef',
    'AnnotationLogDutyStatusLogRef',
]


class AnnotationLogDriverRef(ResponseModel):
    """The annotating driver's reference.

    Census-observed as an ``{id}`` object at scale; the shared coercion
    lifts a bare string defensively (the census-scope lesson).
    """

    model_config = ConfigDict(alias_generator=to_camel)

    id: str


class AnnotationLogDutyStatusLogRef(ResponseModel):
    """The annotated duty-status log's reference — the annotation's subject.

    The BACK-REFERENCE completing the wave-two loop: ``id`` points to
    the ``duty_status_logs`` record this annotation belongs to.
    Census-observed as an ``{id}`` object at scale; the shared coercion
    lifts a bare string defensively (the census-scope lesson).
    """

    model_config = ConfigDict(alias_generator=to_camel)

    id: str


class AnnotationLog(ResponseModel):
    """One GeoTab duty-status-log annotation from the AnnotationLog feed.

    The wave-two conservative mirror (the module docstring's posture):
    ``id`` / ``date_time`` / ``version`` / ``duty_status_log`` required,
    everything else optional even where census-total.

    Attributes:
        comment: The annotation's free text (census-open str).
        date_time: The annotation's UTC instant — the endpoint's event
            time.
        driver: The annotating driver's reference (object-only at scale;
            defensively lifted).
        duty_status_log: The annotated duty-status log's reference — the
            back-reference to the ``duty_status_logs`` vertical.
        id: GeoTab's record id.
        version: The record's version token — the reconcile key beside
            ``id``.
    """

    model_config = ConfigDict(alias_generator=to_camel)

    comment: str | None = None
    date_time: datetime
    driver: Annotated[
        AnnotationLogDriverRef | None, BeforeValidator(bare_id_to_reference)
    ] = None
    duty_status_log: Annotated[
        AnnotationLogDutyStatusLogRef, BeforeValidator(bare_id_to_reference)
    ]
    id: str
    version: str
