# src/fleetpull/endpoints/samsara/driver_vehicle_assignments.py
"""The Samsara driver_vehicle_assignments binding: the fleet-wide
windowed cursor walk with a fixed ``filterBy`` param -- the
idling_events species carrying the trips overlap-anchoring decisions.

``GET /fleet/driver-vehicle-assignments`` is a modern-envelope surface
(``data`` + ``pagination {endCursor, hasNextPage}`` -- the standard
cursor contract). Assignments are fleet-wide with per-record driver AND
vehicle attribution, so there is NO fan-out -- the default
``SingleFetch`` shape, declared by declaring nothing.

``filterBy`` is REQUIRED and API-enforced to a two-value vocabulary:
omitting it is HTTP 400, and ``filterBy=bogus`` is HTTP 400 naming the
vocabulary (``value of filterBy must be one of "drivers", "vehicles"
but got value "bogus"``) -- loud, never silent-empty. The two sweeps
are ONE DATASET: full 24-hour walks under ``filterBy=vehicles`` and
``filterBy=drivers`` returned IDENTICAL row sets (216 = 216, proven
equal as tuple sets; captured 2026-07-20), so the axis is a traversal
choice, not a data partition -- one endpoint, ``filterBy=vehicles``
baked into every request as a FIXED param (the stats triple's ``types``
builder idiom), no sweep, no second endpoint (DESIGN section 8).

The server pages at a FIXED 50 records and the ``limit`` param is
PROVEN IGNORED: limit=1, 5, 100, 512, 513 -- and no limit at all --
each returned a 50-record first page with ``hasNextPage: true``, and
513 was NOT rejected (no enforced tier on this surface, unlike every
probed sibling). The declared ``results_limit=50`` is
documentation-by-declaration of the server's OWN observed page size,
not a working knob.

Retrieval is OVERLAP-anchored, probe-proven on adjacent day windows:
two neighboring 24-hour windows shared 5 midnight-spanning assignments
(identical tuples in both), and the later window carried 5 rows whose
``startTime`` precedes the window start plus 2 whose ``endTime`` is
at/after the window end. Per DESIGN section 4 (the trips decisions,
mirrored): overlap retrieval supersets start-anchored ownership, so
``event_time_column='start_time'``, the runner's post-fetch window
filter assigns each assignment to the single chunk owning its start,
no wire pad is needed, and wholesale partition replacement handles
midnight-spanning intervals exactly as it does for trips.

No range cap was probed on this surface; the default 7-day chunk
width is live-proven (a 7-day unit fetched 6,897 records clean,
2026-07-21).
"""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import Final

from fleetpull.config import SamsaraConfig
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    ResumeValue,
    StorageKind,
    WatermarkMode,
    require_date_window,
)
from fleetpull.models.samsara import DriverVehicleAssignment
from fleetpull.network.contract import HttpMethod, RequestSpec
from fleetpull.network.decoders import SamsaraCursorPageDecoder
from fleetpull.timing import to_iso8601
from fleetpull.vocabulary import Provider, QuotaScope

__all__: list[str] = [
    'SamsaraDriverVehicleAssignmentsSpecBuilder',
    'build_endpoint',
]

_ASSIGNMENTS_PATH: Final[str] = '/fleet/driver-vehicle-assignments'
_RECORDS_KEY: Final[str] = 'data'

# The per-page record count. 50 is the server's OWN observed page size,
# and the `limit` param is PROVEN IGNORED on this surface: limit=1, 5,
# 100, 512, 513 -- and no limit at all -- each returned a 50-record
# first page with hasNextPage: true; 513 was NOT rejected (no enforced
# tier, captured 2026-07-20). Declaring 50 documents the server's
# paging; it is not a working knob.
_RESULTS_LIMIT: Final[int] = 50

# The traversal-axis selector, REQUIRED and API-enforced on input to a
# two-value vocabulary (missing -> HTTP 400; filterBy=bogus -> HTTP 400
# naming 'drivers'/'vehicles'; captured 2026-07-20). Both sweeps return
# the IDENTICAL row set, so one value is baked into every request --
# the axis is traversal, not partition (DESIGN section 8).
_FILTER_BY_PARAM: Final[str] = 'filterBy'
_FILTER_BY_VALUE: Final[str] = 'vehicles'


@dataclass(frozen=True, slots=True)
class SamsaraDriverVehicleAssignmentsSpecBuilder:
    """Build the fleet-wide, date-windowed first request for assignments.

    The ``SpecBuilder`` for the driver_vehicle_assignments single
    chain: a fixed ``GET base_url + path`` carrying the resume window
    as RFC3339 ``startTime``/``endTime`` (the timing codec's
    ``to_iso8601``) plus the FIXED ``filterBy=vehicles`` selector baked
    into every request (the stats triple's ``types`` idiom -- module
    docstring for the one-dataset proof). The decoder owns pagination:
    its ``first_request`` merges ``limit`` onto this spec and its
    ``after`` advance merges onto the sent spec, so the window and the
    selector persist across the whole cursor walk.

    The canonical half-open ``[start, end)`` window maps to the wire as
    ``startTime = start`` and ``endTime = end``. Retrieval is
    OVERLAP-anchored (module docstring), so a window also returns
    assignments merely straddling its bounds -- deliberately unpadded:
    overlap retrieval supersets start-anchored ownership, and the
    runner's post-fetch window filter keeps only assignments whose
    start lies in ``[start, end)``, so each assignment lands in exactly
    the one chunk owning its start and boundary straddlers never
    persist twice.

    Attributes:
        base_url: Root of the Samsara API, trailing-slash-normalized by
            the provider config so the leading-slash path joins
            directly.
        path: The endpoint's leading-slash request path
            (``'/fleet/driver-vehicle-assignments'``).
    """

    base_url: str
    path: str

    def build_spec(
        self, resume: ResumeValue, member_values: Mapping[str, str]
    ) -> RequestSpec:
        """Build the fleet-wide, date-windowed, fixed-filter GET.

        Args:
            resume: The run's resume window. Must be a ``DateWindow`` --
                a watermark endpoint always resumes from one; any other
                value is a wiring bug.
            member_values: Accepted to satisfy the protocol; unused -- a
                fleet-wide single chain binds no member.

        Returns:
            A credential-less ``GET`` for ``base_url + path`` carrying
            ``filterBy=vehicles`` and the window's bounds as RFC3339
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
            _FILTER_BY_PARAM: _FILTER_BY_VALUE,
            'startTime': to_iso8601(resume_window.start),
            'endTime': to_iso8601(resume_window.end),
        }
        return RequestSpec(
            method=HttpMethod.GET,
            url=f'{self.base_url}{self.path}',
            params=params,
        )


def build_endpoint(
    config: SamsaraConfig,
) -> EndpointDefinition[DriverVehicleAssignment]:
    """Build the Samsara driver_vehicle_assignments watermark binding.

    Fleet-wide driver-vehicle assignment intervals fetched
    incrementally: the run resumes from a ``DateWindow`` (watermark
    with the provider's late-arrival lookback from config), the fetched
    assignments are written to ``date=YYYY-MM-DD`` partitions on
    ``start_time``, and each refetched partition is replaced --
    absorbing the overlap-retrieved midnight straddlers exactly as
    trips does (module docstring). Records arrive as a top-level list
    under ``data``, walked by explicit cursor pages (``limit`` on page
    one, ``after`` merged thereafter, the window and ``filterBy``
    persisting throughout), terminal on ``hasNextPage: false``. No
    request shape is declared -- the endpoint is a fleet-wide
    ``SingleFetch``, the default.

    Args:
        config: The validated Samsara configuration; supplies the base
            URL the spec-builder joins to the assignments path and the
            lookback and cutoff the watermark mode carries.

    Returns:
        The frozen driver_vehicle_assignments ``EndpointDefinition``.
        Construction validates the ``WatermarkMode`` /
        ``DATE_PARTITIONED`` / ``event_time_column`` triple against the
        response model.
    """
    return EndpointDefinition(
        provider=Provider.SAMSARA,
        name='driver_vehicle_assignments',
        spec_builder=SamsaraDriverVehicleAssignmentsSpecBuilder(
            base_url=config.base_url, path=_ASSIGNMENTS_PATH
        ),
        page_decoder=SamsaraCursorPageDecoder(
            records_key=_RECORDS_KEY, results_limit=_RESULTS_LIMIT
        ),
        response_model=DriverVehicleAssignment,
        quota_scope=QuotaScope.SAMSARA,
        storage_kind=StorageKind.DATE_PARTITIONED,
        sync_mode=WatermarkMode(
            lookback=timedelta(days=config.lookback_days),
            cutoff=timedelta(days=config.cutoff_days),
        ),
        event_time_column='start_time',
    )
