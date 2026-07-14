# src/fleetpull/incremental/cursor.py
"""Per-endpoint incremental resume state: the cursor a fetch resumes from.

A pure, dependency-free leaf: ``DateWatermark`` imports only stdlib
``datetime``; ``FeedToken`` imports nothing. The persisted cursor members are the
closed set of durable incremental strategies (DESIGN §4):

    - ``DateWatermark`` (Motive/Samsara) — the maximum event timestamp seen.
      The next fetch resumes from ``watermark - lookback`` (the orchestrator's
      arithmetic, not this module's).
    - ``FeedBootstrap`` (GeoTab GetFeed) — a non-persisted first-fetch resume
      carrier built from the global sync cold-start anchor.
    - ``FeedToken`` (GeoTab GetFeed) — the opaque ``toVersion`` cursor, sent
      back as ``fromVersion``. fleetpull never interprets it.

Pure data, no behavior. Resume arithmetic is the orchestrator's,
resume-param construction is the endpoint's, and serialization is the state
layer's (it owns the SQLite representation and calls ``timing`` to turn a
watermark into ISO text) — keeping all three out of here is what lets the
cursor import nothing internal.

An endpoint with no persisted feed token has no cursor row; the orchestrator
turns that absence into ``FeedBootstrap`` for feed endpoints only.
"""

from dataclasses import dataclass
from datetime import datetime

__all__: list[str] = [
    'DateWatermark',
    'FeedBootstrap',
    'FeedToken',
    'IncrementalCursor',
]


@dataclass(frozen=True, slots=True)
class DateWatermark:
    """
    Date-windowed resume state (Motive/Samsara).

    Attributes:
        watermark: The maximum event timestamp seen in the data fetched so
            far, timezone-aware UTC. The next fetch resumes from
            ``watermark - lookback``; that arithmetic belongs to the
            orchestrator, and UTC validity is enforced at the codec
            serialization boundary, so this carrier neither computes nor
            re-checks.
    """

    watermark: datetime


@dataclass(frozen=True, slots=True)
class FeedBootstrap:
    """
    Non-persisted feed first-fetch resume state.

    Attributes:
        from_date: The global sync cold-start anchor used by a feed endpoint before
            any provider ``toVersion`` token has been committed.
    """

    from_date: datetime


@dataclass(frozen=True, slots=True)
class FeedToken:
    """
    Feed-cursor resume state (GeoTab GetFeed).

    Attributes:
        from_version: The opaque version cursor — GeoTab's ``toVersion`` from
            the last page, sent back as ``fromVersion`` to resume. fleetpull
            never parses or orders it; it is a token, not a timestamp.
    """

    from_version: str


type IncrementalCursor = DateWatermark | FeedToken
