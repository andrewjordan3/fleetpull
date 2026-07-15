# src/fleetpull/endpoints/shared/bisection.py
"""The window-bisection declaration for capped, unsortable Get endpoints.

Some provider endpoints hard-cap their responses with no continuation
signal AND support no sort a seek walk could ride (GeoTab
ExceptionEvent: the silent 5,000 cap plus id-sort rejected outright —
DESIGN §8, captured 2026-07-15). The remaining complete-fetch strategy
is adaptive window bisection: fetch the window whole; a response of
exactly the requested limit is the overflow signal; discard it, halve
the window, recurse; a floor-width window still coming back full fails
loudly. The declaration lives on the ``EndpointDefinition`` and the
orchestrator's ``BisectingWindowDriver`` executes it — the binding
carries the provider facts the provider-agnostic driver cannot know.
"""

from dataclasses import dataclass
from datetime import timedelta

__all__: list[str] = ['WindowBisection']


@dataclass(frozen=True, slots=True)
class WindowBisection:
    """Declare adaptive window bisection for one endpoint.

    Attributes:
        results_limit: The per-request record limit the endpoint's spec
            builder writes; a response of exactly this many records is
            the overflow signal. Sound only where the provider's silent
            cap is Captured at or above this value for the entity type
            (a lower cap would make every page look partial and overflow
            undetectable).
        floor: The minimum window width. A floor-width window still
            returning ``results_limit`` records raises loudly — the
            data is denser than windowed fetching can enumerate, or more
            than ``results_limit`` records overlap a single instant,
            which no window width resolves.
        event_time_wire_key: The raw wire key carrying each record's
            owning timestamp (e.g. ``'activeFrom'``) — pre-model, so the
            driver can assign each record to exactly one leaf window
            under overlap-matched retrieval instead of leaning on
            write-time dedup for correctness.
    """

    results_limit: int
    floor: timedelta
    event_time_wire_key: str
