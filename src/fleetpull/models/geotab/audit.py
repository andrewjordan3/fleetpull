# src/fleetpull/models/geotab/audit.py
"""GeoTab Audit response model (``GetFeed`` on ``typeName: Audit``).

Written from the 2026-07-21 feed wave three SCALE census (walked at
scale at the probed tenant), never from docs. An Audit is one
configuration-change audit-trail entry — a versioned feed, so
re-emission under newer ``version`` tokens is expected and the consumer
reconciles by ``(id, max version)`` (DESIGN §4).

The SIMPLEST feed vertical: NO reference fields at all (no ref models),
six flat keys. Requiredness posture (the wave-two conservative stance,
DESIGN §8): the census is a TENANT-SCOPED observation (20,000 records),
so structural requiredness is limited to the record identity — ``id``,
``dateTime`` (the event time), and ``version`` — and every other field
is optional EVEN where the census was total.

``dateTime`` is recovered tz-aware by validation, the GeoTab sibling
idiom. ``comment``, ``name``, and ``userName`` are census-open
free-text strings.
"""

from datetime import datetime

from pydantic import ConfigDict
from pydantic.alias_generators import to_camel

from fleetpull.model_contract import ResponseModel

__all__: list[str] = ['Audit']


class Audit(ResponseModel):
    """One GeoTab audit-trail entry from the Audit feed.

    The wave-two conservative mirror (the module docstring's posture):
    ``id`` / ``date_time`` / ``version`` required, everything else
    optional even where census-total.

    Attributes:
        comment: The audit entry's free text (census-open str).
        date_time: The entry's UTC instant — the endpoint's event time.
        id: GeoTab's record id.
        name: The audited object's name (census-open str).
        user_name: The acting user's name (census-open str).
        version: The record's version token — the reconcile key beside
            ``id``.
    """

    model_config = ConfigDict(alias_generator=to_camel)

    comment: str | None = None
    date_time: datetime
    id: str
    name: str | None = None
    user_name: str | None = None
    version: str
