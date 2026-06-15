# src/fleetpull/network/paginators/geotab.py
"""GeoTab GetFeed pagination strategy (sources: scrubbed
provider-behavior verification, June 2026).

Feed pagination over JSON-RPC: the request body's ``params`` carry
``typeName``, ``resultsLimit``, and either ``search`` (the historical
bootstrap) or ``fromVersion`` (the resume cursor); the ``result`` is
``{"data": [...], "toVersion": str}``. ``toVersion`` is the FeedToken
durable cursor and surfaces from every page including the terminal one.
Branch logic deliberately resembles sibling paginators without sharing
code: provider paginators evolve independently (blast-radius over DRY).
"""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final

from pydantic import BaseModel, ConfigDict, Field

from fleetpull.network.contract import (
    JsonValue,
    PageAdvance,
    RequestSpec,
    validated_envelope_slice,
)

__all__: list[str] = ['GeotabFeedPagination']

# Wire-protocol tokens: Final constants, not an enum — nothing
# dispatches over these. Per-provider and deliberately unshared.
_PARAMS_KEY: Final[str] = 'params'
_RESULTS_LIMIT_KEY: Final[str] = 'resultsLimit'
_SEARCH_KEY: Final[str] = 'search'
_FROM_VERSION_KEY: Final[str] = 'fromVersion'


class _GeotabFeedResult(BaseModel):
    """GetFeed's result: the records and the durable cursor."""

    # strict=True / extra='ignore' rationale: see motive.py's _MotivePageEcho.
    model_config = ConfigDict(frozen=True, extra='ignore', strict=True)

    data: list[JsonValue]
    to_version: str = Field(alias='toVersion')


class _GeotabFeedEnvelope(BaseModel):
    """Envelope slice: locates the feed result."""

    model_config = ConfigDict(frozen=True, extra='ignore', strict=True)

    result: _GeotabFeedResult


def _feed_params_from_body(
    sent_body: Mapping[str, JsonValue],
) -> tuple[Mapping[str, JsonValue], int]:
    """
    Locate the sent body's JSON-RPC ``params`` and its ``resultsLimit``.

    The sent body is fleetpull's own construction, not provider data,
    so malformation here is a caller bug — stdlib ``ValueError``, no
    slice model.

    Args:
        sent_body: The JSON-RPC body of the spec that produced the
            page under inspection.

    Returns:
        The ``params`` mapping and the integer ``resultsLimit``.

    Raises:
        ValueError: When ``params`` or ``resultsLimit`` is missing or
            mistyped.
    """
    params_value: JsonValue = sent_body.get(_PARAMS_KEY)
    if not isinstance(params_value, Mapping):
        raise ValueError(
            f'GeoTab feed request body must carry a {_PARAMS_KEY!r} mapping'
        )
    results_limit: JsonValue = params_value.get(_RESULTS_LIMIT_KEY)
    # bool is a subclass of int; a True resultsLimit is a bug, not 1.
    if not isinstance(results_limit, int) or isinstance(results_limit, bool):
        raise ValueError(
            f'GeoTab feed request params must carry an integer {_RESULTS_LIMIT_KEY!r}'
        )
    return params_value, results_limit


@dataclass(frozen=True, slots=True)
class GeotabFeedPagination:
    """
    GetFeed pagination: advance by ``toVersion`` until a short page.

    No configuration fields: ``resultsLimit`` is read from the sent
    body — the single source of truth is the body the short-page
    comparison runs against, so strategy-versus-endpoint divergence is
    structurally impossible.
    """

    def first_request(self, spec: RequestSpec) -> RequestSpec:
        """
        Return the spec unchanged.

        The endpoint's base spec already carries the bootstrap
        ``search.fromDate`` or resume ``fromVersion`` — state-layer
        input, not pagination's concern.
        """
        return spec

    def advance(self, sent: RequestSpec, envelope: JsonValue) -> PageAdvance:
        """
        Compute the verdict from the feed result's length and cursor.

        Args:
            sent: The spec that produced this page; its body supplies
                ``resultsLimit`` and seeds the next page's body.
            envelope: The parsed response body.

        Returns:
            The verdict. ``durable_progress`` carries ``toVersion`` on
            EVERY page, terminal included — per-page durable progress
            is what makes a crash mid-feed resumable.

        Raises:
            ProviderResponseError: When the feed result is structurally
                violating.
            ValueError: When ``sent`` is malformed for this strategy —
                a caller bug, deliberately stdlib.
        """
        if sent.json_body is None:
            raise ValueError('GeoTab feed requests require a JSON-RPC body')
        sent_body: Mapping[str, JsonValue] = sent.json_body
        sent_params, results_limit = _feed_params_from_body(sent_body)
        feed = validated_envelope_slice(_GeotabFeedEnvelope, envelope).result
        if len(feed.data) < results_limit:
            # The terminal page still carries the resume point.
            return PageAdvance(next_spec=None, durable_progress=feed.to_version)
        # Advance by cursor: fromVersion replaces search. Verified: the
        # API accepts fromVersion alone (and tolerates both being sent),
        # but the strategy always strips search.
        next_params: dict[str, JsonValue] = {
            param_name: param_value
            for param_name, param_value in sent_params.items()
            if param_name != _SEARCH_KEY
        }
        next_params[_FROM_VERSION_KEY] = feed.to_version
        next_body: dict[str, JsonValue] = {**sent_body, _PARAMS_KEY: next_params}
        return PageAdvance(
            next_spec=sent.with_json_body(next_body),
            durable_progress=feed.to_version,
        )
