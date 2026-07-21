# src/fleetpull/endpoints/samsara/_spec_builders.py
"""The shared Samsara vehicle-stats spec-builder.

The builder for the three ``/fleet/vehicles/stats/history`` leaves
(engine_states, gps_readings, odometer_readings) -- one wire surface
serving three endpoints, each requesting exactly its own stat type, so
the one varying fact (``types``) is a builder field and the window
rendering is written once (the Motive ``_spec_builders`` promotion
precedent, arriving here with three users at birth). The idling_events
leaf keeps its own builder: it carries no ``types`` param, and folding
it in would trade a real one-surface bundle for a provider-wide
window-builder abstraction nobody asked for.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final

from fleetpull.endpoints.shared import ResumeValue, require_date_window
from fleetpull.network.contract import HttpMethod, RequestSpec
from fleetpull.timing import to_iso8601

__all__: list[str] = [
    'RECORDS_KEY',
    'RESULTS_LIMIT',
    'STATS_HISTORY_PATH',
    'SamsaraVehicleStatsSpecBuilder',
]

# The one probed wire surface's coinciding facts, stated once for the
# three leaves (only the stat type varies per leaf). A re-probe that
# moves any of these lands here and reaches every leaf.
STATS_HISTORY_PATH: Final[str] = '/fleet/vehicles/stats/history'
RECORDS_KEY: Final[str] = 'data'

# The per-page vehicle count. 512 is THIS surface's probed maximum
# (limit=512 -> HTTP 200, limit=513 -> HTTP 400, captured 2026-07-20):
# the vehicles/drivers tier, NOT idling's 200. Never assume a sibling's
# limit.
RESULTS_LIMIT: Final[int] = 512

# The stat-type selector, API-enforced on input: an unknown value is a
# loud HTTP 400 naming the bogus type ('Invalid stat type(s): ...'),
# never a silent empty page (captured 2026-07-20).
_TYPES_PARAM: Final[str] = 'types'


@dataclass(frozen=True, slots=True)
class SamsaraVehicleStatsSpecBuilder:
    """Build the fleet-wide, date-windowed first request for one stat type.

    The ``SpecBuilder`` for a vehicle-stats single chain: a fixed
    ``GET base_url + path`` carrying the resume window as RFC3339
    ``startTime``/``endTime`` (the timing codec's ``to_iso8601``) plus
    the FIXED ``types=<stat_type>`` selector baked into every request.
    The decoder owns pagination: its ``first_request`` merges ``limit``
    onto this spec and its ``after`` advance merges onto the sent spec,
    so the window and the type selector persist across the whole
    vehicle-axis cursor walk (the idling_events mechanism).

    The canonical half-open ``[start, end)`` window maps to the wire as
    ``startTime = start`` and ``endTime = end``. Retrieval is
    READING-TIME anchored on exactly that half-open window
    (probe-proven: a 12:00-13:00Z window returned min 12:00:03.062Z,
    max 12:59:56.881Z), so a window's readings are exactly those
    timestamped inside it; the runner's post-fetch window filter is
    pure hygiene, never load-bearing.

    Attributes:
        base_url: Root of the Samsara API, trailing-slash-normalized by
            the provider config so the leading-slash path joins
            directly.
        path: The endpoint's leading-slash request path
            (``'/fleet/vehicles/stats/history'``).
        stat_type: The one stat type this endpoint requests -- the
            verbatim ``types`` wire value, which is also the
            per-vehicle series key the decoder unnests.
    """

    base_url: str
    path: str
    stat_type: str

    def build_spec(
        self, resume: ResumeValue, member_values: Mapping[str, str]
    ) -> RequestSpec:
        """Build the fleet-wide, date-windowed, single-type GET.

        Args:
            resume: The run's resume window. Must be a ``DateWindow`` --
                a watermark endpoint always resumes from one; any other
                value is a wiring bug.
            member_values: Accepted to satisfy the protocol; unused -- a
                fleet-wide single chain binds no member.

        Returns:
            A credential-less ``GET`` for ``base_url + path`` carrying
            ``types=<stat_type>`` and the window's bounds as RFC3339
            ``startTime``/``endTime``. Auth headers are layered on by
            the client's ``ProviderProfile``; pagination parameters are
            injected by the page decoder's ``first_request``.

        Raises:
            TypeError: ``resume`` is not a ``DateWindow``.
            ValueError: A window bound is not canonical UTC.

        Side Effects:
            None.
        """
        resume_window = require_date_window(resume, type(self).__name__)
        params = {
            _TYPES_PARAM: self.stat_type,
            'startTime': to_iso8601(resume_window.start),
            'endTime': to_iso8601(resume_window.end),
        }
        return RequestSpec(
            method=HttpMethod.GET,
            url=f'{self.base_url}{self.path}',
            params=params,
        )
