# src/fleetpull/endpoints/samsara/trips.py
"""The Samsara trips binding: the per-vehicle windowed fan-out -- the
roster machinery's first cross-provider consumer.

The trips surface is the LEGACY v1 API only: ``GET /v1/fleet/trips``
(the modern candidates 404 -- ``/fleet/trips``, ``/beta/fleet/trips``,
``/preview/fleet/trips``; captured 2026-07-20). ``vehicleId`` is
REQUIRED -- omitting it is a loud HTTP 400 text/plain rpc-error, so the
endpoint is structurally per-vehicle and the binding declares
``request_shape=RosterFanOut`` over the Samsara ``vehicle_ids`` roster
(declared beside its feeder in ``endpoints/samsara/vehicles.py``; this
module knows only the ``RosterKey``, never the feeder). The fan-out
member merges as a QUERY parameter -- the drivers-leaf precedent, not
the Motive path-template one.

The envelope is ``{"trips": [...]}`` with no pagination of any kind:
one response per (vehicle, window), so the shared ``SinglePageDecoder``
fits (the GeoTab exception_events pairing precedent). Retrieval is
OVERLAP-anchored, re-verified per-type 2026-07-20 (a 60-second window
strictly inside a trip's span returned that trip; start- and
end-anchoring falsified -- DESIGN §4's historical record for this exact
endpoint, confirmed live). Per §4: overlap retrieval supersets
start-anchored ownership, so ``event_time_column='start_time'``, the
runner's post-fetch window filter assigns each trip to the single chunk
owning its start, and no wire pad is needed (the exception_events
reasoning verbatim).

The provider enforces a LOUD, exactly-90-day range cap -- HTTP 400
text/plain ``rpc error: ... requested time range cannot exceed 90
days``; a 90-day window succeeded. No builder guard exists for it (the
Motive driving_periods stance): default 7-day backfill chunks sit far
inside, and a ``backfill_chunk_days`` raised past 90 fails loudly on
the first request rather than losing data silently -- the fix is a
smaller chunk width.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Final

from fleetpull.config import SamsaraConfig
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    ResumeValue,
    RosterFanOut,
    StorageKind,
    WatermarkMode,
    require_date_window,
)
from fleetpull.models.samsara import Trip
from fleetpull.network.contract import HttpMethod, RequestSpec
from fleetpull.network.decoders import SinglePageDecoder
from fleetpull.roster import RosterKey
from fleetpull.timing import require_utc
from fleetpull.vocabulary import Provider, QuotaScope

__all__: list[str] = [
    'SamsaraTripsSpecBuilder',
    'build_endpoint',
]

_TRIPS_PATH: Final[str] = '/v1/fleet/trips'
_RECORDS_KEY: Final[str] = 'trips'

# The integer-arithmetic epoch origin: float `.timestamp()` math can land
# a millisecond-precision bound one ms low (float rounding + int()
# truncation), so the conversion below is exact integer division --
# symmetric with the model's no-float-epoch-math decode stance.
_UNIX_EPOCH: Final[datetime] = datetime(1970, 1, 1, tzinfo=UTC)
_ONE_MILLISECOND: Final[timedelta] = timedelta(milliseconds=1)

# The fan-out member key IS the wire query parameter, verbatim: the spec
# builder merges member_values into params unchanged, so declaring the
# exact wire token here leaves no translation seam to drift (the drivers
# sweep-param stance). The RosterFanOut declaration below carries it.
_VEHICLE_ID_PARAM: Final[str] = 'vehicleId'


def _to_epoch_milliseconds(bound: datetime) -> int:
    """Render one window bound as the epoch-millisecond int the wire takes.

    The UTC guard is the codec-boundary discipline ``DateWindow`` defers
    to its serialization point (DESIGN §4): a naive or non-UTC bound
    reaching a request is a missed ingress, failed loudly.

    Args:
        bound: A window bound; must be canonical UTC.

    Returns:
        Milliseconds since the Unix epoch.

    Raises:
        ValueError: ``bound`` is naive or not canonical UTC.

    Side Effects:
        None.
    """
    return (require_utc(bound) - _UNIX_EPOCH) // _ONE_MILLISECOND


@dataclass(frozen=True, slots=True)
class SamsaraTripsSpecBuilder:
    """Build the per-vehicle, date-windowed first request for trips.

    The ``SpecBuilder`` for the trips ``RosterFanOut``: a fixed
    ``GET base_url + path`` carrying the chain's member binding as query
    parameters, verbatim (``{'vehicleId': <member>}`` -- the drivers-leaf
    precedent), plus the resume window as ``startMs``/``endMs`` epoch
    milliseconds. The endpoint is not paginated, so this first request
    is the chain's only request; the page decoder returns no successor.

    The canonical half-open ``[start, end)`` window maps to the wire as
    ``startMs = start`` and ``endMs = end`` in epoch milliseconds. At
    the millisecond boundary the wire's inclusive reading of ``endMs``
    mismatches the half-open convention (a trip touching the exact end
    instant may be returned) -- deliberately unadjusted: retrieval is
    overlap-anchored and supersets start-anchored ownership anyway, and
    the runner's post-fetch window filter keeps only trips whose start
    lies in ``[start, end)``, so each trip lands in exactly the one
    chunk owning its start and the boundary duplicate never persists.

    Attributes:
        base_url: Root of the Samsara API, trailing-slash-normalized by
            the provider config so the leading-slash path joins
            directly.
        path: The endpoint's leading-slash request path
            (``'/v1/fleet/trips'``).
    """

    base_url: str
    path: str

    def build_spec(
        self, resume: ResumeValue, member_values: Mapping[str, str]
    ) -> RequestSpec:
        """Build one vehicle chain's windowed GET.

        Args:
            resume: The run's resume window. Must be a ``DateWindow`` --
                a watermark endpoint always resumes from one; any other
                value is a wiring bug.
            member_values: The fan-out member binding, merged verbatim
                as query parameters -- ``{'vehicleId': <id>}`` for a
                roster chain (``vehicleId`` is REQUIRED by the wire; an
                empty binding would earn the provider's own loud 400).

        Returns:
            A credential-less ``GET`` for ``base_url + path`` carrying
            the member binding plus ``startMs``/``endMs`` epoch
            milliseconds for the window's bounds. Auth headers are
            layered on by the client's ``ProviderProfile``.

        Raises:
            TypeError: ``resume`` is not a ``DateWindow``.
            ValueError: A window bound is not canonical UTC.

        Side Effects:
            None.
        """
        resume_window = require_date_window(resume, type(self).__name__)
        params = dict(member_values)
        params['startMs'] = str(_to_epoch_milliseconds(resume_window.start))
        params['endMs'] = str(_to_epoch_milliseconds(resume_window.end))
        return RequestSpec(
            method=HttpMethod.GET,
            url=f'{self.base_url}{self.path}',
            params=params,
        )


def build_endpoint(config: SamsaraConfig) -> EndpointDefinition[Trip]:
    """Build the Samsara trips watermark binding.

    Per-vehicle trip history fetched incrementally: the run resumes from
    a ``DateWindow`` (watermark with the provider's late-arrival
    lookback from config -- which also absorbs trips materializing on
    completion), the fetched trips are written to ``date=YYYY-MM-DD``
    partitions on ``start_time``, and each refetched partition is
    replaced. Records arrive as a top-level list under ``trips``,
    unpaginated -- one response per (vehicle, window). The
    ``request_shape`` declaration (``RosterFanOut``) names the Samsara
    ``vehicle_ids`` roster; the orchestration entry resolves it to
    members and fans one request chain per vehicle, passing each id to
    the spec-builder's ``member_values`` -- this binding only declares
    the strategies and the roster key, never the roster's feeder.

    Args:
        config: The validated Samsara configuration; supplies the base
            URL the spec-builder joins to the trips path and the
            lookback and cutoff the watermark mode carries.

    Returns:
        The frozen trips ``EndpointDefinition``. Construction validates
        the ``WatermarkMode`` / ``DATE_PARTITIONED`` /
        ``event_time_column`` triple against the response model.
    """
    return EndpointDefinition(
        provider=Provider.SAMSARA,
        name='trips',
        spec_builder=SamsaraTripsSpecBuilder(
            base_url=config.base_url, path=_TRIPS_PATH
        ),
        page_decoder=SinglePageDecoder(records_key=_RECORDS_KEY),
        response_model=Trip,
        quota_scope=QuotaScope.SAMSARA,
        storage_kind=StorageKind.DATE_PARTITIONED,
        sync_mode=WatermarkMode(
            lookback=timedelta(days=config.lookback_days),
            cutoff=timedelta(days=config.cutoff_days),
        ),
        event_time_column='start_time',
        request_shape=RosterFanOut(
            roster=RosterKey(Provider.SAMSARA, 'vehicle_ids'),
            member_key=_VEHICLE_ID_PARAM,
        ),
    )
