# src/fleetpull/endpoints/samsara/asset_locations.py
"""The Samsara asset_locations binding: the windowed batched roster
fan-out -- the first ``BatchedRosterFanOut`` consumer.

``GET /assets/location-and-speed/stream`` is a modern-envelope surface
(``data`` + ``pagination {endCursor, hasNextPage}`` -- the standard
cursor contract; the ``endCursor`` is a fat composite token, opaque,
passed back verbatim as ``after`` like every other cursor). The legacy
hub called this surface ``location_stream``; the shipped endpoint is
``asset_locations`` per the name=plural-of-entity invariant.

The ``ids`` parameter is REQUIRED: omitting it is a loud HTTP 400
(``"Need to include asset IDs to filter by."``), and the batch cap is
API-ENFORCED AT 50 -- 50 comma-joined ids returned 200 while 100,
200, and 609 each returned HTTP 400 naming the bound (``"Need to
filter by 50 or less asset IDs or syncTokens."``; captured
2026-07-20). So the binding declares
``request_shape=BatchedRosterFanOut`` over the Samsara ``vehicle_ids``
roster (declared beside its feeder in ``endpoints/samsara/vehicles.py``;
this module knows only the ``RosterKey``): one cursor-walk chain per
sorted 50-member batch, the batch comma-joined into the ``ids`` query
parameter. The batch is transport packing only -- every record carries
its own ``asset.id`` attribution, so no member attribution rides on the
request mapping, and the fan-out driver's progress narration counts
BATCHES for this shape (a deliberate, recorded consequence -- DESIGN
section 8).

The windowed leaf builder carries the resume window as RFC3339
``startTime``/``endTime`` (the idling_events builder precedent) and
merges the batch binding verbatim as a query parameter (the trips
member-merge precedent); the decoder's ``first_request`` merges
``limit`` and its ``after`` advance merges onto the SENT spec, so the
window and the batch ride every page of each chain's walk.

The per-endpoint ``limit`` maximum is 512, probed directly on THIS
surface: limit=512 returned HTTP 200 and limit=513 a loud HTTP 400 --
the vehicles/drivers tier, NOT idling's 200 (the per-endpoint
limit-tier rule, honored by probing rather than assuming).

Retrieval is READING-TIME anchored on the half-open ``[startTime,
endTime)`` window, probe-proven: a 12:00-13:00Z window returned
readings spanning exactly 12:00:03Z..12:59:56Z. Consequence:
``event_time_column='happened_at_time'``, the retrieval anchor and the
routing anchor coincide natively, no wire pad exists, and the runner's
post-fetch window filter is pure hygiene.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import Final

from fleetpull.config import SamsaraConfig
from fleetpull.endpoints.shared import (
    BatchedRosterFanOut,
    EndpointDefinition,
    ResumeValue,
    StorageKind,
    WatermarkMode,
    require_date_window,
)
from fleetpull.models.samsara import AssetLocation
from fleetpull.network.contract import HttpMethod, RequestSpec
from fleetpull.network.decoders import SamsaraCursorPageDecoder
from fleetpull.roster import RosterKey
from fleetpull.timing import to_iso8601
from fleetpull.vocabulary import Provider, QuotaScope

__all__: list[str] = [
    'SamsaraAssetLocationsSpecBuilder',
    'build_endpoint',
]

_ASSET_LOCATIONS_PATH: Final[str] = '/assets/location-and-speed/stream'
_RECORDS_KEY: Final[str] = 'data'

# The per-page record count. 512 is THIS endpoint's probed maximum
# (limit=512 -> HTTP 200, limit=513 -> HTTP 400, captured 2026-07-20):
# the vehicles/drivers tier, NOT idling's 200. Never assume a sibling's
# limit.
_RESULTS_LIMIT: Final[int] = 512

# The fan-out member key IS the wire query parameter, verbatim: the spec
# builder merges member_values into params unchanged, so declaring the
# exact wire token here leaves no translation seam to drift (the trips
# stance). Each member value is one sorted, comma-joined 50-id batch.
_IDS_PARAM: Final[str] = 'ids'

# The batch cap is API-ENFORCED AT 50, probed directly: 50 comma-joined
# ids -> HTTP 200; 100/200/609 -> HTTP 400 '{"message": "Need to filter
# by 50 or less asset IDs or syncTokens."}' (captured 2026-07-20).
_BATCH_SIZE: Final[int] = 50


@dataclass(frozen=True, slots=True)
class SamsaraAssetLocationsSpecBuilder:
    """Build one batch chain's date-windowed first request.

    The ``SpecBuilder`` for the asset_locations ``BatchedRosterFanOut``:
    a fixed ``GET base_url + path`` carrying the chain's batch binding
    verbatim as query parameters (``{'ids': '<id>,<id>,...'}`` -- the
    trips member-merge precedent) plus the resume window as RFC3339
    ``startTime``/``endTime`` (the timing codec's ``to_iso8601``). The
    decoder owns pagination: its ``first_request`` merges ``limit`` onto
    this spec and its ``after`` advance merges onto the sent spec, so
    the window and the batch persist across the whole cursor walk.

    The canonical half-open ``[start, end)`` window maps to the wire as
    ``startTime = start`` and ``endTime = end``. Retrieval is
    reading-time anchored on exactly that half-open window (module
    docstring), so a window's readings are exactly those timestamped
    inside it; the runner's post-fetch window filter is pure hygiene,
    never load-bearing.

    Attributes:
        base_url: Root of the Samsara API, trailing-slash-normalized by
            the provider config so the leading-slash path joins
            directly.
        path: The endpoint's leading-slash request path
            (``'/assets/location-and-speed/stream'``).
    """

    base_url: str
    path: str

    def build_spec(
        self, resume: ResumeValue, member_values: Mapping[str, str]
    ) -> RequestSpec:
        """Build one batch chain's windowed GET.

        Args:
            resume: The run's resume window. Must be a ``DateWindow`` --
                a watermark endpoint always resumes from one; any other
                value is a wiring bug.
            member_values: The batch binding, merged verbatim as query
                parameters -- ``{'ids': <comma-joined batch>}`` for a
                batched roster chain (``ids`` is REQUIRED by the wire;
                an empty binding would earn the provider's own loud
                400).

        Returns:
            A credential-less ``GET`` for ``base_url + path`` carrying
            the batch binding plus the window's bounds as RFC3339
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
        params = dict(member_values)
        params['startTime'] = to_iso8601(resume_window.start)
        params['endTime'] = to_iso8601(resume_window.end)
        return RequestSpec(
            method=HttpMethod.GET,
            url=f'{self.base_url}{self.path}',
            params=params,
        )


def build_endpoint(config: SamsaraConfig) -> EndpointDefinition[AssetLocation]:
    """Build the Samsara asset_locations watermark binding.

    Per-asset location readings fetched incrementally: the run resumes
    from a ``DateWindow`` (watermark with the provider's late-arrival
    lookback from config), the fetched readings are written to
    ``date=YYYY-MM-DD`` partitions on ``happened_at_time``, and each
    refetched partition is replaced. Records arrive as a top-level list
    under ``data``, already at the reading grain, walked by explicit
    cursor pages per batch chain (``limit`` on page one, ``after``
    merged thereafter, the window and batch parameters persisting
    throughout), terminal on ``hasNextPage: false``. The
    ``request_shape`` declaration (``BatchedRosterFanOut``) names the
    Samsara ``vehicle_ids`` roster; the orchestration entry resolves it
    to members and the shape seam fans one chain per sorted 50-member
    comma-joined batch, passing each batch string to the spec-builder's
    ``member_values`` -- this binding only declares the strategies, the
    roster key, and the probed cap, never the roster's feeder.

    Args:
        config: The validated Samsara configuration; supplies the base
            URL the spec-builder joins to the stream path and the
            lookback and cutoff the watermark mode carries.

    Returns:
        The frozen asset_locations ``EndpointDefinition``. Construction
        validates the ``WatermarkMode`` / ``DATE_PARTITIONED`` /
        ``event_time_column`` triple against the response model.
    """
    return EndpointDefinition(
        provider=Provider.SAMSARA,
        name='asset_locations',
        spec_builder=SamsaraAssetLocationsSpecBuilder(
            base_url=config.base_url, path=_ASSET_LOCATIONS_PATH
        ),
        page_decoder=SamsaraCursorPageDecoder(
            records_key=_RECORDS_KEY, results_limit=_RESULTS_LIMIT
        ),
        response_model=AssetLocation,
        quota_scope=QuotaScope.SAMSARA,
        storage_kind=StorageKind.DATE_PARTITIONED,
        sync_mode=WatermarkMode(
            lookback=timedelta(days=config.lookback_days),
            cutoff=timedelta(days=config.cutoff_days),
        ),
        event_time_column='happened_at_time',
        request_shape=BatchedRosterFanOut(
            roster=RosterKey(Provider.SAMSARA, 'vehicle_ids'),
            member_key=_IDS_PARAM,
            batch_size=_BATCH_SIZE,
        ),
    )
