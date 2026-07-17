# src/fleetpull/endpoints/geotab/trips.py
"""The GeoTab trips binding: the first windowed (watermark) GeoTab endpoint.

A date-windowed, seek-paged pull of the ``Trip`` entity: the run resumes
from a ``DateWindow`` (watermark with the provider's late-arrival
lookback from config -- for trips, the same margin absorbs GeoTab's Trip
recalculation), the window rides a ``TripSearch`` (``search.fromDate`` /
``search.toDate``) beside the id-ascending ``sort`` of the seek walk,
and the fetched days land in ``date=YYYY-MM-DD`` partitions replaced
wholesale. The decoder is the existing ``GeotabGetPageDecoder``
unchanged: its advance spreads the sent params when rewriting
``sort.offset``, so ``search`` survives every page (live-verified
2026-07-13 -- a windowed, sorted, seeked page pair returned
strictly-ascending ids across the boundary with every record inside the
window).

``TripSearch`` matches trips by their STOP time (captured 2026-07-06
via a discriminating window pair; prediction-confirmed 2026-07-15,
DESIGN §8): a trip whose start precedes ``fromDate`` returns when its
stop falls inside, and a trip stopping past ``toDate`` never returns.
The event-time column is therefore ``stop`` — retrieval (stop in
window) and routing (the runner's per-batch window filter over the
event-time column) coincide, so every completed trip belongs to exactly
one chunk: its stop's day. The watermark advances on stop, which
matches record-materialization order — a Trip exists only once it has
stopped. Anchoring on ``start`` instead would drop every
chunk-boundary-crossing trip: the chunk owning its start never receives
it, and the chunk that fetches it filters it out.

Every request here is a JSON-RPC POST whose ``params.credentials`` and
resolved host are the session strategy's injections (the devices-leaf
convention); the host this module writes is a pre-auth placeholder.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import Final

from fleetpull.config import GeotabConfig
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    ResumeValue,
    StorageKind,
    WatermarkMode,
)
from fleetpull.incremental import DateWindow
from fleetpull.models.geotab import Trip
from fleetpull.network.contract import HttpMethod, RequestSpec
from fleetpull.network.decoders import GeotabGetPageDecoder
from fleetpull.timing import to_iso8601
from fleetpull.vocabulary import JsonValue, Provider, QuotaScope

__all__: list[str] = ['build_endpoint']

# The JSON-RPC ingress path every GeoTab method POSTs to.
_API_PATH: Final[str] = '/apiv1'

# Pre-auth placeholder host for a default-constructed (credential-less)
# config -- mirrors GeotabAuthConfig's server default; the session
# strategy retargets every prepared request, so no request ever leaves
# for this host un-retargeted. Duplicated from the devices leaf per its
# stated colocation policy (module-private constants, deliberately
# unshared).
_DEFAULT_SERVER: Final[str] = 'my.geotab.com'

# The largest sound page under Get's silent 5,000-record cap.
_RESULTS_LIMIT: Final[int] = 5000

# Wire-protocol tokens: module-private Final constants, colocated with
# the strategy that emits them (the constants-scope precedent;
# deliberately unshared, even with the devices leaf's own copies).
_METHOD_KEY: Final[str] = 'method'
_PARAMS_KEY: Final[str] = 'params'
_TYPE_NAME_KEY: Final[str] = 'typeName'
_SEARCH_KEY: Final[str] = 'search'
_FROM_DATE_KEY: Final[str] = 'fromDate'
_TO_DATE_KEY: Final[str] = 'toDate'
_RESULTS_LIMIT_KEY: Final[str] = 'resultsLimit'
_SORT_KEY: Final[str] = 'sort'
_SORT_BY_KEY: Final[str] = 'sortBy'
_SORT_DIRECTION_KEY: Final[str] = 'sortDirection'
_OFFSET_KEY: Final[str] = 'offset'
_GET_METHOD: Final[str] = 'Get'
_ID_SORT: Final[str] = 'id'
_ASCENDING: Final[str] = 'asc'

_TRIP_TYPE_NAME: Final[str] = 'Trip'


def _server_host(config: GeotabConfig) -> str:
    """The authentication host the spec URLs are built on.

    Args:
        config: The validated GeoTab configuration.

    Returns:
        ``auth.server`` when a credential is configured; the placeholder
        default otherwise (a credential-less config still builds every
        discovered leaf -- the registry walk requires it -- but can never
        fetch, so the placeholder never reaches the wire).
    """
    if config.auth is not None:
        return config.auth.server
    return _DEFAULT_SERVER


@dataclass(frozen=True, slots=True)
class _GeotabWindowedGetSpecBuilder:
    """Build the windowed seek walk's first ``Get`` request.

    The probed first-request shape (captured 2026-07-13, the trips
    boundary fixture): the resume window as ``search.fromDate`` /
    ``search.toDate`` beside ``sort`` with ``sortBy: id``,
    ``sortDirection: asc``, and an EXPLICIT null ``offset``. ``lastId``
    is never written (probe-settled decision 1). Every request after
    this one is the decoder's, whose advance preserves ``search``.

    Attributes:
        server: The pre-auth authentication host (retargeted by the
            session strategy after Authenticate).
        type_name: The GeoTab entity to walk (``'Trip'``).
        results_limit: The page size; 5000 -- the largest sound page
            under the silent cap.
    """

    server: str
    type_name: str
    results_limit: int

    def build_spec(
        self, resume: ResumeValue, path_values: Mapping[str, str]
    ) -> RequestSpec:
        """Build the walk's first request from the resume window.

        Args:
            resume: The run's resume window. Must be a ``DateWindow`` --
                a watermark endpoint always resumes from one; any other
                value is a wiring bug.
            path_values: Accepted to satisfy the protocol; unused --
                there is no URL-path fan-out.

        Returns:
            A credential-less JSON-RPC POST carrying the window as
            ``search.fromDate`` / ``search.toDate`` (UTC ISO-8601 ``Z``
            strings; the half-open end passes verbatim -- the runner's
            per-batch window filter owns the boundary).

        Raises:
            TypeError: ``resume`` is not a ``DateWindow``.
        """
        if not isinstance(resume, DateWindow):
            raise TypeError(
                '_GeotabWindowedGetSpecBuilder requires a DateWindow resume, '
                f'got {type(resume).__name__}.'
            )
        json_body: dict[str, JsonValue] = {
            _METHOD_KEY: _GET_METHOD,
            _PARAMS_KEY: {
                _TYPE_NAME_KEY: self.type_name,
                _SEARCH_KEY: {
                    _FROM_DATE_KEY: to_iso8601(resume.start),
                    _TO_DATE_KEY: to_iso8601(resume.end),
                },
                _RESULTS_LIMIT_KEY: self.results_limit,
                _SORT_KEY: {
                    _SORT_BY_KEY: _ID_SORT,
                    _SORT_DIRECTION_KEY: _ASCENDING,
                    _OFFSET_KEY: None,
                },
            },
        }
        return RequestSpec(
            method=HttpMethod.POST,
            url=f'https://{self.server}{_API_PATH}',
            json_body=json_body,
        )


def build_endpoint(config: GeotabConfig) -> EndpointDefinition[Trip]:
    """Build the GeoTab trips watermark binding.

    Movement-interval history fetched incrementally: the run resumes
    from a ``DateWindow``, each window is walked in id-ascending seek
    pages under the silent 5,000-record ``Get`` cap with the window
    filter riding ``search``, and the fetched days are written to
    ``date=YYYY-MM-DD`` partitions replaced wholesale. No
    ``completeness_check``: the guard is snapshot-only by construction
    -- a ``GetCountOf`` compares only against a complete listing, and a
    date window is not one.

    Args:
        config: The validated GeoTab configuration; supplies the
            authentication host the pre-auth spec URLs are built on and
            the lookback and cutoff the watermark mode carries.

    Returns:
        The frozen trips ``EndpointDefinition``. Construction validates
        the ``WatermarkMode`` / ``DATE_PARTITIONED`` /
        ``event_time_column`` triple against the response model.
    """
    return EndpointDefinition(
        provider=Provider.GEOTAB,
        name='trips',
        spec_builder=_GeotabWindowedGetSpecBuilder(
            server=_server_host(config),
            type_name=_TRIP_TYPE_NAME,
            results_limit=_RESULTS_LIMIT,
        ),
        page_decoder=GeotabGetPageDecoder(),
        response_model=Trip,
        quota_scope=QuotaScope.GEOTAB_GET,
        storage_kind=StorageKind.DATE_PARTITIONED,
        sync_mode=WatermarkMode(
            lookback=timedelta(days=config.lookback_days),
            cutoff=timedelta(days=config.cutoff_days),
        ),
        event_time_column='stop',
    )
