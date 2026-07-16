# src/fleetpull/endpoints/geotab/exception_events.py
"""The GeoTab exception_events binding: the bisected windowed endpoint.

A date-windowed pull of the ``ExceptionEvent`` entity — the UNFILTERED
stream, every rule (DESIGN §8's 2026-07-15 decision block: no
server-side rule filter in version one; rule selection is the
consumer's one-expression job on the delivered stream). The trips seek
template is structurally unavailable here — id-sort is rejected
outright for this type (captured 2026-07-15: ``ArgumentException``,
"Can not sort by id") — so the binding declares ``WindowBisection`` and
the orchestrator's bisecting driver fetches each unit window whole,
halving on the exactly-full overflow signal down to the floor.

``ExceptionEventSearch`` window matching is OVERLAP-anchored (captured
2026-07-15): retrieval supersets start-anchored ownership, so
``active_from`` is the event-time column, the driver's leaf filter and
the runner's window filter assign every record exactly one owner, and
no wire-window pad is needed. Events mutate after creation (~1 h
observed envelope), which the provider-level lookback absorbs.

Every request here is a JSON-RPC POST whose ``params.credentials`` and
resolved host are the session strategy's injections (the devices-leaf
convention); the host this module writes is a pre-auth placeholder.
"""

import logging
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
    WindowBisection,
)
from fleetpull.incremental import DateWindow
from fleetpull.models.geotab import ExceptionEvent
from fleetpull.network.contract import HttpMethod, RequestSpec
from fleetpull.network.decoders import SinglePageDecoder
from fleetpull.timing import to_iso8601
from fleetpull.vocabulary import JsonValue, Provider, QuotaScope

__all__: list[str] = ['build_endpoint']

logger = logging.getLogger(__name__)

# The JSON-RPC ingress path every GeoTab method POSTs to.
_API_PATH: Final[str] = '/apiv1'

# Pre-auth placeholder host for a default-constructed (credential-less)
# config -- mirrors GeotabAuthConfig's server default; the session
# strategy retargets every prepared request, so no request ever leaves
# for this host un-retargeted. Duplicated from the devices leaf per its
# stated colocation policy (module-private constants, deliberately
# unshared).
_DEFAULT_SERVER: Final[str] = 'my.geotab.com'

# The per-request record limit AND the bisection overflow threshold.
# The silent cap is Captured on this type (2026-07-15: GetCountOf
# 304,716 vs a bare Get returning exactly 5,000); per-type provenance,
# not a global GeoTab fact. A strong candidate for a user config knob.
_RESULTS_LIMIT: Final[int] = 5000

# The bisection floor: a one-minute window still returning a full page
# fails loudly (sustained >5,000 events/minute fleet-wide, or >5,000
# events overlapping one instant -- feed territory either way). A
# strong candidate for a user config knob.
_FLOOR: Final[timedelta] = timedelta(minutes=1)

# The wire key the bisecting driver anchors leaf ownership by.
_EVENT_TIME_WIRE_KEY: Final[str] = 'activeFrom'

# Wire-protocol tokens: module-private Final constants, colocated with
# the strategy that emits them (the constants-scope precedent;
# deliberately unshared, even with the sibling leaves' own copies).
_METHOD_KEY: Final[str] = 'method'
_PARAMS_KEY: Final[str] = 'params'
_TYPE_NAME_KEY: Final[str] = 'typeName'
_SEARCH_KEY: Final[str] = 'search'
_FROM_DATE_KEY: Final[str] = 'fromDate'
_TO_DATE_KEY: Final[str] = 'toDate'
_RESULTS_LIMIT_KEY: Final[str] = 'resultsLimit'
_GET_METHOD: Final[str] = 'Get'
_RESULT_KEY: Final[str] = 'result'

_EXCEPTION_EVENT_TYPE_NAME: Final[str] = 'ExceptionEvent'


def _server_host(config: GeotabConfig) -> str:
    """The authentication host the spec URLs are built on.

    Args:
        config: The validated GeoTab configuration.

    Returns:
        The configured auth server, or the placeholder default for a
        credential-less config (the session strategy retargets every
        prepared request, so the placeholder never reaches the wire).
    """
    if config.auth is not None:
        return config.auth.server
    return _DEFAULT_SERVER


@dataclass(frozen=True, slots=True)
class _GeotabUnsortedWindowedGetSpecBuilder:
    """Build one windowed, unsorted, capped ``Get`` request.

    The bisecting driver's request shape (captured 2026-07-15 as the
    composition that SUCCEEDS on this type): the window as
    ``search.fromDate`` / ``search.toDate`` beside ``resultsLimit``,
    and deliberately NO ``sort`` member — id-sort is rejected outright
    for ExceptionEvent, and any sort composed with a search degrades to
    the deterministic ``-32000 GenericException``. The driver re-invokes
    this builder per sub-window; there is no page advance.

    Attributes:
        server: The pre-auth authentication host (retargeted by the
            session strategy after Authenticate).
        type_name: The GeoTab entity to fetch (``'ExceptionEvent'``).
        results_limit: The per-request record limit — the bisection
            overflow threshold.
    """

    server: str
    type_name: str
    results_limit: int

    def build_spec(
        self, resume: ResumeValue, path_values: Mapping[str, str]
    ) -> RequestSpec:
        """Build one window's request.

        Args:
            resume: The window to fetch. Must be a ``DateWindow`` — the
                unit's resume window at the recursion top, a bisected
                half below it; any other value is a wiring bug.
            path_values: Accepted to satisfy the protocol; unused —
                there is no URL-path fan-out.

        Returns:
            A credential-less JSON-RPC POST carrying the window as
            ``search.fromDate`` / ``search.toDate`` (UTC ISO-8601 ``Z``
            strings) with ``resultsLimit`` and no ``sort``.

        Raises:
            TypeError: ``resume`` is not a ``DateWindow``.
        """
        if not isinstance(resume, DateWindow):
            raise TypeError(
                '_GeotabUnsortedWindowedGetSpecBuilder requires a DateWindow '
                f'resume, got {type(resume).__name__}.'
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
            },
        }
        return RequestSpec(
            method=HttpMethod.POST,
            url=f'https://{self.server}{_API_PATH}',
            json_body=json_body,
        )


def build_endpoint(config: GeotabConfig) -> EndpointDefinition[ExceptionEvent]:
    """Build the GeoTab exception_events bisected watermark binding.

    The unfiltered ExceptionEvent stream fetched incrementally: the run
    resumes from a ``DateWindow`` (watermark with the provider's
    late-arrival lookback from config, which also absorbs the observed
    post-creation mutation), the bisecting driver fetches each unit
    window whole (halving on overflow per the declared
    ``WindowBisection``), and the kept records land in
    ``date=YYYY-MM-DD`` partitions on ``active_from``, each refetched
    partition replaced. Responses are single pages under the JSON-RPC
    ``result`` key — the cap that once disqualified single-page decoding
    is the driver's overflow signal now.

    Args:
        config: The validated GeoTab configuration; supplies the auth
            host and the lookback and cutoff the watermark mode carries.

    Returns:
        The frozen exception_events ``EndpointDefinition``. Construction
        validates the watermark / partitioned / event-time triple and the
        bisection pairing.
    """
    return EndpointDefinition(
        provider=Provider.GEOTAB,
        name='exception_events',
        spec_builder=_GeotabUnsortedWindowedGetSpecBuilder(
            server=_server_host(config),
            type_name=_EXCEPTION_EVENT_TYPE_NAME,
            results_limit=_RESULTS_LIMIT,
        ),
        page_decoder=SinglePageDecoder(records_key=_RESULT_KEY),
        response_model=ExceptionEvent,
        quota_scope=QuotaScope.GEOTAB_GET,
        storage_kind=StorageKind.DATE_PARTITIONED,
        sync_mode=WatermarkMode(
            lookback=timedelta(days=config.lookback_days),
            cutoff=timedelta(days=config.cutoff_days),
        ),
        event_time_column='active_from',
        window_bisection=WindowBisection(
            results_limit=_RESULTS_LIMIT,
            floor=_FLOOR,
            event_time_wire_key=_EVENT_TIME_WIRE_KEY,
        ),
    )
