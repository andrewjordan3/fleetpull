# src/fleetpull/models/geotab/media_file.py
"""GeoTab MediaFile response model (``GetFeed`` on ``typeName: MediaFile``).

Written from the 2026-07-21 feed wave three SCALE census (walked at
scale at the probed tenant), never from docs. A MediaFile is one media
attachment (image, video, ...) captured by a device or driver — a
versioned feed, so re-emission under newer ``version`` tokens is
expected and the consumer reconciles by ``(id, max version)``
(DESIGN §4).

THIN EVIDENCE CAVEAT: only 55 records over a 730-day window at this
tenant — genuinely thin data. The model is conservative accordingly;
every arm below is what those 55 records showed, and the census cannot
speak for a tenant that uses media at volume.

NO ``dateTime`` key — the event time is ``fromDate`` (the media start,
55/55), so the binding anchors ``event_time_column='from_date'`` and
``from_date`` is a REQUIRED datetime (storage partitions on it).

Requiredness posture (the wave-two conservative stance, DESIGN §8):
structural requiredness is limited to the record identity — ``id``,
``fromDate`` (the event time), and ``version`` — and every other field
is optional. Both ``device`` and ``driver`` are OPTIONAL: a media
file's primary entity is ambiguous — it may attach to a device or a
driver — so neither is promoted to a required primary ref. The observed
arms and exclusions:

- ``device`` is PROVEN MIXED object-or-string (42 string / 13 object at
  scale); ``driver`` was string-only observed (55/55). Both ride the
  shared ``bare_id_to_reference`` lift, so both land as ``*__id`` —
  the mixed ``device`` because the census proved both arms, the
  string-only ``driver`` defensively (the census-scope lesson: a
  tenant census cannot prove the object arm absent either).
- THREE DOCUMENTED EXCLUSIONS (the ``defectList.children`` doctrine,
  DESIGN §8): ``metaData`` (empty object on all 55), ``tags`` (empty
  list on all 55), and ``thumbnails`` (empty list on all 55). Their
  element/content shape is unobservable at this tenant, the records
  layer supports only observable shapes, and ``extra='ignore'`` absorbs
  them wire-side (pinned: a record populating any of the three still
  validates). REVISIT when a tenant populates them: capture the shape
  and model it then.

``fromDate`` and ``toDate`` are recovered tz-aware by validation, the
GeoTab sibling idiom. ``mediaType``, ``name``, ``solutionId``, and
``status`` are census-open strings.
"""

from datetime import datetime
from typing import Annotated

from pydantic import BeforeValidator, ConfigDict
from pydantic.alias_generators import to_camel

from fleetpull.model_contract import ResponseModel
from fleetpull.models.geotab.shared import bare_id_to_reference

__all__: list[str] = [
    'MediaFile',
    'MediaFileDeviceRef',
    'MediaFileDriverRef',
]


class MediaFileDeviceRef(ResponseModel):
    """The capturing device's reference.

    PROVEN mixed on this census (42 bare-string / 13 ``{id}`` object);
    the shared coercion lifts the bare form to ``{"id": <string>}``, so
    both arms land as ``device__id``.
    """

    model_config = ConfigDict(alias_generator=to_camel)

    id: str


class MediaFileDriverRef(ResponseModel):
    """The capturing driver's reference.

    String-only observed at scale (55/55 bare strings); the shared
    coercion lifts the bare form, and an unobserved object arm would
    pass through defensively (the census-scope lesson). Both arms land
    as ``driver__id``.
    """

    model_config = ConfigDict(alias_generator=to_camel)

    id: str


class MediaFile(ResponseModel):
    """One GeoTab media attachment from the MediaFile feed.

    The wave-two conservative mirror on thin evidence (the module
    docstring's posture): ``id`` / ``from_date`` / ``version`` required,
    both refs and everything else optional; ``metaData`` / ``tags`` /
    ``thumbnails`` documented-excluded.

    Attributes:
        device: The capturing device's reference — proven
            object-or-string, both arms landing as ``device__id``.
            Optional (the ambiguous-primary-entity choice).
        driver: The capturing driver's reference (string-only observed;
            defensively lifted). Optional (the ambiguous-primary-entity
            choice).
        from_date: The media's UTC start — the endpoint's event time (in
            place of the absent ``dateTime``).
        id: GeoTab's record id.
        media_type: The media type token (census-open str).
        name: The media file name (census-open str).
        solution_id: The originating solution id (census-open str).
        status: The media status token (census-open str).
        to_date: The media's UTC end.
        version: The record's version token — the reconcile key beside
            ``id``.
    """

    model_config = ConfigDict(alias_generator=to_camel)

    device: Annotated[
        MediaFileDeviceRef | None, BeforeValidator(bare_id_to_reference)
    ] = None
    driver: Annotated[
        MediaFileDriverRef | None, BeforeValidator(bare_id_to_reference)
    ] = None
    from_date: datetime
    id: str
    media_type: str | None = None
    name: str | None = None
    solution_id: str | None = None
    status: str | None = None
    to_date: datetime | None = None
    version: str
