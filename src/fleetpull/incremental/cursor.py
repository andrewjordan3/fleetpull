# src/fleetpull/incremental/cursor.py
"""Per-endpoint incremental resume state: the cursor a fetch resumes from.

A pure, dependency-free leaf: ``DateWatermark`` imports only stdlib
``datetime``; ``FeedToken`` imports nothing. The two members are the closed
set of incremental strategies (DESIGN §4):

    - ``DateWatermark`` (Motive/Samsara) — the maximum event timestamp seen.
      The next fetch resumes from ``watermark - lookback`` (the orchestrator's
      arithmetic, not this module's).
    - ``FeedToken`` (GeoTab GetFeed) — the opaque ``toVersion`` cursor, sent
      back as ``fromVersion``. fleetpull never interprets it.

Pure data, no behavior. Resume arithmetic is the orchestrator's,
resume-param construction is the endpoint's, and serialization is the state
layer's (it owns the SQLite representation and calls ``timing`` to turn a
watermark into ISO text) — keeping all three out of here is what lets the
cursor import nothing internal.

An endpoint with no incremental state has no cursor — absence, handled at the
endpoint/orchestrator layer, not a third union member. The set is exactly
two.
"""

from dataclasses import dataclass
from datetime import datetime

__all__: list[str] = [
    'DateWatermark',
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
