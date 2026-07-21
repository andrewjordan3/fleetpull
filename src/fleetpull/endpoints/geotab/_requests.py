# src/fleetpull/endpoints/geotab/_requests.py
"""The shared GeoTab JSON-RPC request machinery for the endpoint leaves.

One module carries everything a GeoTab leaf composes on the request side
(renamed from ``_get_requests`` when the ``GetFeed`` builder joined,
2026-07-21): the JSON-RPC POST envelope (every request is a POST to
``https://{server}/apiv1``), the snapshot seek-walk builder, the
windowed builder with its per-type sort declaration, the ``GetFeed``
seed-or-resume builder every feed leaf shares, the ``server_host``
resolver, and the ``GetCountOfCheck`` truth instrument. Underscore-
prefixed so the registry walk skips it: this module is machinery, not an
endpoint leaf.

The probe provenance the shapes rest on:

- Plain ``Get`` silently hard-caps at 5,000 records with no continuation
  signal; a captured ``GetCountOf`` above the cap proved records beyond
  5,000 are invisible to bare ``Get``.
- Id-sortability is PER-TYPE, never assumed: ``Device`` supports it
  (captured 2026-07-09), ``User`` supports it (re-proven 2026-07-16),
  ``ExceptionEvent`` rejects it outright (captured 2026-07-15:
  ``ArgumentException``, "Can not sort by id") -- and any sort composed
  with a search degrades to the deterministic ``-32000
  GenericException`` for that type.
- The sorted first-request shape is the probed one: ``sort`` inside
  ``params`` with ``sortBy: id``, ``sortDirection: asc``, and an
  EXPLICIT null ``offset`` -- never an absent key. ``lastId`` is never
  written (probe-settled decision; the docs name it an
  ``ArgumentException`` beside id-sort).
- ``GetFeed`` rides the same JSON-RPC POST but is its OWN method class
  (its own ~60/min rate budget; the 2026-07-21 header-decrement probe).
  Seeding via ``search.fromDate`` on the tokenless first call is
  wire-proven (DESIGN section 4 carries the docs-falsified record);
  resuming sends ``fromVersion`` and never ``search``.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final

from pydantic import BaseModel, ConfigDict

from fleetpull.config import GeotabConfig
from fleetpull.endpoints.shared import (
    ResumeValue,
    require_date_window,
    require_feed_resume,
)
from fleetpull.incremental import DateWindow, FeedSeed, FeedToken
from fleetpull.network.client import TransportClient
from fleetpull.network.contract import (
    HttpMethod,
    RequestSpec,
    validated_envelope_slice,
)
from fleetpull.timing import to_iso8601
from fleetpull.vocabulary import JsonValue

__all__: list[str] = [
    'GeotabGetFeedSpecBuilder',
    'GeotabGetSpecBuilder',
    'GeotabWindowedGetSpecBuilder',
    'GetCountOfCheck',
    'server_host',
]

# The JSON-RPC ingress path every GeoTab method POSTs to.
_API_PATH: Final[str] = '/apiv1'

# Pre-auth placeholder host for a default-constructed (credential-less)
# config -- mirrors GeotabAuthConfig's server default; the session
# strategy retargets every prepared request, so no request ever leaves
# for this host un-retargeted.
_DEFAULT_SERVER: Final[str] = 'my.geotab.com'

# Wire-protocol tokens: module-private Final constants, colocated with
# the strategies that emit them (the constants-scope precedent;
# deliberately unshared with the decoder module's own copies).
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
_FROM_VERSION_KEY: Final[str] = 'fromVersion'
_GET_METHOD: Final[str] = 'Get'
_GET_COUNT_OF_METHOD: Final[str] = 'GetCountOf'
_GET_FEED_METHOD: Final[str] = 'GetFeed'
_ID_SORT: Final[str] = 'id'
_ASCENDING: Final[str] = 'asc'


def server_host(config: GeotabConfig) -> str:
    """The authentication host the spec URLs are built on.

    Args:
        config: The validated GeoTab configuration.

    Returns:
        ``auth.server`` when a credential is configured; the placeholder
        default otherwise (a credential-less config still builds every
        discovered leaf -- the registry walk requires it -- but can never
        fetch, so the placeholder never reaches the wire; the session
        strategy retargets every prepared request to the resolved host).

    Side Effects:
        None.
    """
    if config.auth is not None:
        return config.auth.server
    return _DEFAULT_SERVER


def _post_spec(server: str, json_body: dict[str, JsonValue]) -> RequestSpec:
    """The JSON-RPC POST envelope every GeoTab method rides.

    Args:
        server: The pre-auth authentication host.
        json_body: The credential-less JSON-RPC body to send.

    Returns:
        A credential-less POST for ``https://{server}/apiv1``.
    """
    return RequestSpec(
        method=HttpMethod.POST,
        url=f'https://{server}{_API_PATH}',
        json_body=json_body,
    )


def _get_json_body(
    type_name: str,
    results_limit: int,
    window: DateWindow | None,
    id_sort: bool,
) -> dict[str, JsonValue]:
    """Assemble a ``Get`` body from the leaf's declared shape.

    ``params`` composes ``typeName`` always; the window as
    ``search.fromDate`` / ``search.toDate`` (UTC ISO-8601 ``Z`` strings)
    when one is given; ``resultsLimit`` always; and the probed ``sort``
    member (``sortBy: id``, ``sortDirection: asc``, EXPLICIT null
    ``offset``) only when the walk is id-sorted.

    Args:
        type_name: The GeoTab entity name (``'Device'``, ``'Trip'``).
        results_limit: The per-request record limit.
        window: The resume window riding ``search``; ``None`` for a
            snapshot (no search member at all).
        id_sort: Whether the probed ``sort`` member is written.

    Returns:
        The credential-less JSON-RPC ``Get`` body.
    """
    params: dict[str, JsonValue] = {_TYPE_NAME_KEY: type_name}
    if window is not None:
        params[_SEARCH_KEY] = {
            _FROM_DATE_KEY: to_iso8601(window.start),
            _TO_DATE_KEY: to_iso8601(window.end),
        }
    params[_RESULTS_LIMIT_KEY] = results_limit
    if id_sort:
        params[_SORT_KEY] = {
            _SORT_BY_KEY: _ID_SORT,
            _SORT_DIRECTION_KEY: _ASCENDING,
            _OFFSET_KEY: None,
        }
    return {_METHOD_KEY: _GET_METHOD, _PARAMS_KEY: params}


@dataclass(frozen=True, slots=True)
class GeotabGetSpecBuilder:
    """Build the snapshot seek walk's first ``Get`` request.

    Sort is intrinsic here, not declared: a seek walk without id-sort is
    meaningless -- termination and completeness both ride the
    id-ascending advance (the probed shape's provenance is the module
    docstring). Every request after this one is the decoder's.

    ``build_spec`` rejects any non-``None`` resume: a snapshot always
    resumes from nothing, so a window reaching this builder is a wiring
    bug that would otherwise silently fetch the entire entity set
    unwindowed.

    Attributes:
        server: The pre-auth authentication host (retargeted by the
            session strategy after Authenticate).
        type_name: The GeoTab entity to walk (``'Device'``, ``'User'``).
        results_limit: The page size; 5000 -- the largest sound page
            under the silent cap.
    """

    server: str
    type_name: str
    results_limit: int

    def build_spec(
        self, resume: ResumeValue, member_values: Mapping[str, str]
    ) -> RequestSpec:
        """Build the walk's first request.

        Args:
            resume: Must be ``None`` -- a snapshot resumes from nothing;
                any other value is a wiring bug.
            member_values: Accepted to satisfy the protocol; unused --
                a single-chain endpoint binds no member.

        Returns:
            A credential-less JSON-RPC POST; ``params.credentials`` and
            the resolved host are the session strategy's injections.

        Raises:
            TypeError: ``resume`` is not ``None``.
        """
        if resume is not None:
            raise TypeError(
                f'{type(self).__name__} requires a None resume -- a snapshot '
                f'resumes from nothing; got {type(resume).__name__}.'
            )
        return _post_spec(
            self.server,
            _get_json_body(
                type_name=self.type_name,
                results_limit=self.results_limit,
                window=None,
                id_sort=True,
            ),
        )


@dataclass(frozen=True, slots=True)
class GeotabWindowedGetSpecBuilder:
    """Build a watermark endpoint's windowed ``Get`` request.

    The resume window rides ``search.fromDate`` / ``search.toDate``;
    ``id_sort`` declares whether the probed ``sort`` member is written
    beside it. Id-sortability is a per-type PROBED provider capability,
    never assumed (module docstring: Device yes 2026-07-09, User yes
    2026-07-16, ExceptionEvent no 2026-07-15). ``id_sort=True`` selects
    the seek-walk pairing (the decoder advances ``sort.offset``, and its
    advance spreads the sent params so ``search`` survives every page --
    live-verified 2026-07-13 on Trip); ``id_sort=False`` also selects
    the single-shot/bisection pairing -- there is no page advance, the
    driver re-invokes this builder per sub-window. The ExceptionEvent
    probes found ``version`` and date fields sortable for that type --
    banked as feed-design input, which is why this field may later widen
    to a seek-key enum.

    Attributes:
        server: The pre-auth authentication host (retargeted by the
            session strategy after Authenticate).
        type_name: The GeoTab entity to fetch (``'Trip'``,
            ``'ExceptionEvent'``).
        results_limit: The per-request record limit; for an unsorted
            leaf it doubles as the bisection overflow threshold.
        id_sort: Whether the probed ``sort`` member is written -- the
            per-type declared capability.
    """

    server: str
    type_name: str
    results_limit: int
    id_sort: bool

    def build_spec(
        self, resume: ResumeValue, member_values: Mapping[str, str]
    ) -> RequestSpec:
        """Build one window's request.

        Args:
            resume: The window to fetch. Must be a ``DateWindow`` -- a
                watermark endpoint always resumes from one; any other
                value is a wiring bug.
            member_values: Accepted to satisfy the protocol; unused --
                a single-chain endpoint binds no member.

        Returns:
            A credential-less JSON-RPC POST carrying the window as
            ``search.fromDate`` / ``search.toDate`` (UTC ISO-8601 ``Z``
            strings; the half-open end passes verbatim -- the runner's
            per-batch window filter owns the boundary), with the probed
            ``sort`` member iff ``id_sort``.

        Raises:
            TypeError: ``resume`` is not a ``DateWindow``.
        """
        window = require_date_window(resume, type(self).__name__)
        return _post_spec(
            self.server,
            _get_json_body(
                type_name=self.type_name,
                results_limit=self.results_limit,
                window=window,
                id_sort=self.id_sort,
            ),
        )


@dataclass(frozen=True, slots=True)
class GeotabGetFeedSpecBuilder:
    """Build a feed endpoint's first ``GetFeed`` request: seed or resume.

    One builder serves every feed leaf; the leaf declares only its
    ``type_name`` and ``results_limit``. The resume value decides the
    shape (the wire-proven pair, DESIGN section 4):

    - ``FeedSeed`` -- the tokenless first run. ``search.fromDate`` carries
      the cold-start anchor, positioning the feed at a version covering all
      entities with date >= that instant; NO ``fromVersion`` is written.
      Wire-proven to the second on LogRecord and StatusData (2026-07-21)
      DESPITE the docs claiming those types' search is ignored.
    - ``FeedToken`` -- every run after. ``fromVersion`` carries the stored
      token; NO ``search`` is written (the decoder's advances strip it the
      same way, so the pair of sites agree).

    Every request after this one is the decoder's
    (``GeotabFeedPageDecoder`` advances by ``toVersion`` and reads
    ``resultsLimit`` from the sent body, so builder-versus-decoder
    divergence is structurally impossible).

    Attributes:
        server: The pre-auth authentication host (retargeted by the
            session strategy after Authenticate).
        type_name: The GeoTab entity to feed (``'LogRecord'``, ``'Trip'``).
        results_limit: The page size; the short-page terminal rule and the
            50,000 protocol maximum both key off it (per-type caps are the
            leaf's declaration concern).
    """

    server: str
    type_name: str
    results_limit: int

    def build_spec(
        self, resume: ResumeValue, member_values: Mapping[str, str]
    ) -> RequestSpec:
        """Build the feed walk's first request.

        Args:
            resume: The seed-or-token a feed run always carries -- a
                ``FeedSeed`` (first run) or ``FeedToken`` (every run
                after); any other value is a wiring bug.
            member_values: Accepted to satisfy the protocol; unused --
                a single-chain endpoint binds no member.

        Returns:
            A credential-less JSON-RPC POST; ``params.credentials`` and
            the resolved host are the session strategy's injections.

        Raises:
            TypeError: ``resume`` is neither a ``FeedSeed`` nor a
                ``FeedToken``.
            ValueError: A seed ``start`` that is naive or non-UTC
                (surfaced from the timing codec).
        """
        feed_resume = require_feed_resume(resume, type(self).__name__)
        params: dict[str, JsonValue] = {_TYPE_NAME_KEY: self.type_name}
        match feed_resume:
            case FeedSeed(start=start):
                params[_SEARCH_KEY] = {_FROM_DATE_KEY: to_iso8601(start)}
            case FeedToken(from_version=from_version):
                params[_FROM_VERSION_KEY] = from_version
        params[_RESULTS_LIMIT_KEY] = self.results_limit
        return _post_spec(
            self.server, {_METHOD_KEY: _GET_FEED_METHOD, _PARAMS_KEY: params}
        )


class _GetCountOfEnvelope(BaseModel):
    """Envelope slice: ``GetCountOf`` returns the count under ``result``.

    strict=True so a stringly count fails loudly instead of coercing
    (and a boolean never passes as an integer); extra='ignore' per the
    house slice-model pattern.
    """

    model_config = ConfigDict(frozen=True, extra='ignore', strict=True)

    result: int


@dataclass(frozen=True, slots=True)
class GetCountOfCheck:
    """The GeoTab completeness check: ``GetCountOf`` as truth instrument.

    Fires one ``GetCountOf`` JSON-RPC request for the harvested entity
    through the same open client the harvest used -- session auth, the
    limiter (one token on the given scope, the token-per-attempt law),
    and the classifier all apply -- and reads the integer count through
    a private envelope slice. Declared on the snapshot definitions so
    the single-fetch driver can prove the capped ``Get`` walk lost
    nothing (probe-settled decision).

    Attributes:
        server: The pre-auth authentication host (retargeted by the
            session strategy, exactly as the data requests are).
        type_name: The GeoTab entity to count (``'Device'``, ``'User'``).
    """

    server: str
    type_name: str

    def expected_count(self, client: TransportClient, quota_scope: str) -> int:
        """Return GeoTab's reported count of the harvested entity.

        Args:
            client: The open transport client the harvest ran on.
            quota_scope: The endpoint's rate-limit scope key
                (``GEOTAB_GET`` -- the count spends from the same
                method-class budget as the data pages).

        Returns:
            The provider-reported entity count.

        Raises:
            ProviderResponseError: The envelope's ``result`` is not an
                integer (via the slice model), or the request failed
                fatally (via the client).
        """
        spec = _post_spec(
            self.server,
            {
                _METHOD_KEY: _GET_COUNT_OF_METHOD,
                _PARAMS_KEY: {_TYPE_NAME_KEY: self.type_name},
            },
        )
        envelope = client.fetch_envelope(spec, quota_scope)
        return validated_envelope_slice(_GetCountOfEnvelope, envelope).result
