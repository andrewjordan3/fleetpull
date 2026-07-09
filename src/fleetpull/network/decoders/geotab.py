# src/fleetpull/network/decoders/geotab.py
"""GeoTab page decoders: GetFeed (toVersion cursor) and seek-paging Get.

Feed pagination over JSON-RPC (scrubbed provider-behavior verification,
June 2026): the request body's ``params`` carry ``typeName``,
``resultsLimit``, and either ``search`` (the historical bootstrap) or
``fromVersion`` (the resume cursor); the ``result`` is
``{"data": [...], "toVersion": str}``. ``toVersion`` is the durable
cursor and surfaces from every page including the terminal one.

Seek paging over plain ``Get`` (captured 2026-07-09): ``Get`` silently
hard-caps at 5,000 records with no continuation signal (``GetCountOf``
5,666 vs the capped 5,000), so a capped entity pages by ``sort`` on
``id`` ascending -- each advance sets ``sort.offset`` to the last
returned record's ``id``; termination is the empty result list (a short
page is NOT terminal, deliberately unlike the feed rule). The ``result``
is a plain record list.

Decoder logic deliberately resembles its siblings without sharing code
across providers.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final

from pydantic import BaseModel, ConfigDict, Field

from fleetpull.exceptions import ProviderResponseError
from fleetpull.network.contract import (
    DecodedPage,
    PageAdvance,
    RequestSpec,
    validated_envelope_slice,
)
from fleetpull.vocabulary import JsonObject, JsonValue

__all__: list[str] = ['GeotabFeedPageDecoder', 'GeotabGetPageDecoder']

# Wire-protocol tokens: Final constants, not an enum. Deliberately unshared
# with other providers' decoder modules.
_PARAMS_KEY: Final[str] = 'params'
_RESULTS_LIMIT_KEY: Final[str] = 'resultsLimit'
_SEARCH_KEY: Final[str] = 'search'
_FROM_VERSION_KEY: Final[str] = 'fromVersion'
_SORT_KEY: Final[str] = 'sort'
_OFFSET_KEY: Final[str] = 'offset'
_ID_KEY: Final[str] = 'id'


class _GeotabFeedResult(BaseModel):
    """GetFeed's result: the records and the durable cursor.

    ``data`` is typed as a list of JSON objects so the decoder's
    ``list[JsonObject]`` records return is honored at validation time;
    strict=True / extra='ignore' rationale: see motive.py's _MotivePageEcho.
    """

    model_config = ConfigDict(frozen=True, extra='ignore', strict=True)

    data: list[JsonObject]
    to_version: str = Field(alias='toVersion')


class _GeotabFeedEnvelope(BaseModel):
    """Envelope slice: locates the feed result."""

    model_config = ConfigDict(frozen=True, extra='ignore', strict=True)

    result: _GeotabFeedResult


def _params_from_body(
    sent_body: Mapping[str, JsonValue], method_label: str
) -> tuple[Mapping[str, JsonValue], int]:
    """Locate the sent body's JSON-RPC ``params`` and ``resultsLimit``.

    The sent body is fleetpull's own construction, not provider data, so
    malformation here is a caller bug -- stdlib ``ValueError``, no slice
    model. Shared by both GeoTab decoders (same module, same provider).

    Args:
        sent_body: The JSON-RPC body of the spec under inspection.
        method_label: The method-class word for the error message
            (``'feed'`` / ``'Get'``).

    Returns:
        The ``params`` mapping and the integer ``resultsLimit``.

    Raises:
        ValueError: When ``params`` or ``resultsLimit`` is missing or
            mistyped.
    """
    params_value: JsonValue = sent_body.get(_PARAMS_KEY)
    if not isinstance(params_value, Mapping):
        raise ValueError(
            f'GeoTab {method_label} request body must carry a {_PARAMS_KEY!r} mapping'
        )
    results_limit: JsonValue = params_value.get(_RESULTS_LIMIT_KEY)
    # bool is a subclass of int; a True resultsLimit is a bug, not 1.
    if not isinstance(results_limit, int) or isinstance(results_limit, bool):
        raise ValueError(
            f'GeoTab {method_label} request params must carry an integer '
            f'{_RESULTS_LIMIT_KEY!r}'
        )
    return params_value, results_limit


@dataclass(frozen=True, slots=True)
class GeotabFeedPageDecoder:
    """Decode GeoTab GetFeed pages and advance by ``toVersion``.

    No configuration fields: ``resultsLimit`` is read from the sent
    body, so decoder-versus-endpoint divergence is structurally
    impossible.
    """

    def first_request(self, spec: RequestSpec) -> RequestSpec:
        """Return the spec unchanged.

        The base spec already carries the bootstrap ``search.fromDate``
        or resume ``fromVersion`` -- state-layer input, not the
        decoder's concern.
        """
        return spec

    def decode_page(self, sent: RequestSpec, envelope: JsonValue) -> DecodedPage:
        """Extract the feed records and compute the version verdict.

        Args:
            sent: The spec that produced this page; its body supplies
                ``resultsLimit`` and seeds the next page's body.
            envelope: The parsed response body.

        Returns:
            The feed records and the verdict. ``durable_progress``
            carries ``toVersion`` on EVERY page, terminal included.

        Raises:
            ProviderResponseError: When the feed result is structurally
                violating.
            ValueError: When ``sent`` is malformed for this decoder -- a
                caller bug, deliberately stdlib.
        """
        if sent.json_body is None:
            raise ValueError('GeoTab feed requests require a JSON-RPC body')
        sent_body: Mapping[str, JsonValue] = sent.json_body
        sent_params, results_limit = _params_from_body(sent_body, 'feed')
        feed = validated_envelope_slice(_GeotabFeedEnvelope, envelope).result
        if len(feed.data) < results_limit:
            # The terminal page still carries the resume point.
            return DecodedPage(
                records=feed.data,
                advance=PageAdvance(next_spec=None, durable_progress=feed.to_version),
            )
        # Advance by cursor: fromVersion replaces search.
        next_params: dict[str, JsonValue] = {
            param_name: param_value
            for param_name, param_value in sent_params.items()
            if param_name != _SEARCH_KEY
        }
        next_params[_FROM_VERSION_KEY] = feed.to_version
        next_body: dict[str, JsonValue] = {**sent_body, _PARAMS_KEY: next_params}
        return DecodedPage(
            records=feed.data,
            advance=PageAdvance(
                next_spec=sent.with_json_body(next_body),
                durable_progress=feed.to_version,
            ),
        )


class _GeotabGetEnvelope(BaseModel):
    """Envelope slice: ``Get`` returns records as a plain top-level list.

    strict=True / extra='ignore' rationale: see motive.py's
    _MotivePageEcho.
    """

    model_config = ConfigDict(frozen=True, extra='ignore', strict=True)

    result: list[JsonObject]


def _sort_from_params(sent_params: Mapping[str, JsonValue]) -> Mapping[str, JsonValue]:
    """Locate the Get request's ``sort`` mapping; its absence is a caller bug.

    Args:
        sent_params: The sent body's ``params`` mapping.

    Returns:
        The ``sort`` mapping the advance rewrites.

    Raises:
        ValueError: When ``sort`` is missing or not a mapping.
    """
    sort_value: JsonValue = sent_params.get(_SORT_KEY)
    if not isinstance(sort_value, Mapping):
        raise ValueError(
            f'GeoTab Get request params must carry a {_SORT_KEY!r} mapping'
        )
    return sort_value


def _last_record_id(records: list[JsonObject]) -> str:
    """The last returned record's ``id`` -- the next page's seek offset.

    Args:
        records: The page's records, non-empty.

    Returns:
        The trailing record's string ``id``.

    Raises:
        ProviderResponseError: When the trailing record lacks a string
            ``id`` -- provider data violating the seek contract, not a
            caller bug.
    """
    last_id: JsonValue = records[-1].get(_ID_KEY)
    if not isinstance(last_id, str):
        raise ProviderResponseError(
            detail=(
                f'GeoTab Get seek paging requires a string {_ID_KEY!r} on '
                f'every record; the last record of the page carries '
                f'{type(last_id).__name__}'
            )
        )
    return last_id


@dataclass(frozen=True, slots=True)
class GeotabGetPageDecoder:
    """Decode plain ``Get`` pages and advance by id-sorted seek offset.

    Plain ``Get`` silently hard-caps at 5,000 records with no
    continuation signal (captured 2026-07-09), so the walk seeks: the
    spec builder's first request sorts by ``id`` ascending with an
    explicit null ``offset``, and each advance sets ``sort.offset`` to
    the last returned record's ``id``. Termination is the EMPTY result
    list only -- a short page still advances (unlike the feed rule),
    because the cap makes page fullness meaningless as a completion
    signal. ``lastId`` is never written, by construction: the probe
    bodies carried ``"lastId": null`` (tolerated), but the settled
    decision (DESIGN section 8, decision 1) is to never send the key --
    the docs name it an ``ArgumentException`` beside id-sort.

    No configuration fields: ``resultsLimit`` lives only in the sent
    body, so decoder-versus-endpoint divergence is structurally
    impossible (the feed precedent).
    """

    def first_request(self, spec: RequestSpec) -> RequestSpec:
        """Return the spec unchanged.

        The base spec already carries ``resultsLimit`` and the initial
        ``sort`` (``sortBy: id``, explicit null ``offset``) -- the spec
        builder's concern, not the decoder's.
        """
        return spec

    def decode_page(self, sent: RequestSpec, envelope: JsonValue) -> DecodedPage:
        """Extract the page's records and compute the seek verdict.

        Args:
            sent: The spec that produced this page; its body's ``sort``
                seeds the next page's offset.
            envelope: The parsed response body.

        Returns:
            The page's records and the verdict: an empty page
            terminates; any records advance with ``sort.offset`` set to
            the last record's ``id``. ``durable_progress`` is always
            ``None`` -- the seek offset is fetch-private, never a resume
            cursor.

        Raises:
            ProviderResponseError: When the envelope is structurally
                violating, or a record lacks the string ``id`` the seek
                contract stands on.
            ValueError: When ``sent`` is malformed for this decoder -- a
                caller bug, deliberately stdlib.
        """
        if sent.json_body is None:
            raise ValueError('GeoTab Get requests require a JSON-RPC body')
        sent_body: Mapping[str, JsonValue] = sent.json_body
        sent_params, _results_limit = _params_from_body(sent_body, 'Get')
        sent_sort = _sort_from_params(sent_params)
        records = validated_envelope_slice(_GeotabGetEnvelope, envelope).result
        if not records:
            return DecodedPage(
                records=[],
                advance=PageAdvance(next_spec=None, durable_progress=None),
            )
        next_sort: dict[str, JsonValue] = {
            **sent_sort,
            _OFFSET_KEY: _last_record_id(records),
        }
        next_params: dict[str, JsonValue] = {**sent_params, _SORT_KEY: next_sort}
        next_body: dict[str, JsonValue] = {**sent_body, _PARAMS_KEY: next_params}
        return DecodedPage(
            records=records,
            advance=PageAdvance(
                next_spec=sent.with_json_body(next_body),
                durable_progress=None,
            ),
        )
