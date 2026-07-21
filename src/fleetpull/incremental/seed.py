# src/fleetpull/incremental/seed.py
"""The feed arm's cold-start resume value — the historical seed.

A pure, stdlib-only leaf beside the cursors and the window (DESIGN §4).
``FeedSeed`` is the resume value a tokenless feed endpoint's FIRST request is
built from: the spec-builder renders it as ``search.fromDate``, which starts
the feed at a version covering all entities with date >= that instant
(wire-proven 2026-07-21 — the probe record in §4). It is never persisted —
the moment the first page returns, the page's ``toVersion`` commits as the
``FeedToken`` cursor and the seed is gone forever, which is what makes the
seed-once invariant (§14's I4) structural: only a run that read no stored
token can ever construct one.

``FeedResume`` is the union a feed spec-builder consumes — the seed on the
cold first run, the stored token on every run after. There is no ``None``
arm: a feed endpoint always resumes from something, so an absent resume
reaching a feed builder is a wiring bug the shared guard
(``require_feed_resume``) rejects loudly.
"""

from dataclasses import dataclass
from datetime import datetime

from fleetpull.incremental.cursor import FeedToken

__all__: list[str] = ['FeedResume', 'FeedSeed']


@dataclass(frozen=True, slots=True)
class FeedSeed:
    """
    The feed arm's cold-start resume value: seed the feed at a date.

    Rendered by the spec-builder as the first request's ``search.fromDate``
    (and never a ``fromVersion``); superseded by the ``FeedToken`` cursor the
    moment the first page commits. UTC validity defers to the codec boundary
    when the builder serializes ``start``, exactly as ``DateWatermark`` and
    ``DateWindow`` defer it.

    Attributes:
        start: The instant the feed's history begins from — the run's
            configured cold-start anchor (``sync.default_start_date``),
            timezone-aware UTC.
    """

    start: datetime


# The resume value a feed spec-builder consumes: the cold-start seed, or the
# stored token. Deliberately no None arm — a feed endpoint always resumes
# from one of the two (the runner constructs the seed exactly when no token
# is stored).
type FeedResume = FeedSeed | FeedToken
