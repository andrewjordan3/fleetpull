# src/fleetpull/network/contract/paginators/motive.py
"""Motive pagination strategy (sources: scrubbed provider-behavior
verification, June 2026; page-size cap from provider documentation).

Page-numbered pagination: ``page_no``/``per_page`` query params, with
every paginated response echoing a top-level ``pagination`` block
beside the records key. Branch logic deliberately resembles sibling
paginators without sharing code: provider paginators evolve
independently (blast-radius over DRY).
"""

from dataclasses import dataclass
from typing import Final

from pydantic import BaseModel, ConfigDict

from fleetpull.network.contract.pagination import (
    PageAdvance,
    validate_pagination_envelope,
)
from fleetpull.network.contract.request import JsonValue, RequestSpec

__all__: list[str] = ['MotivePagination']

# Wire-protocol tokens: Final constants, not an enum — nothing
# dispatches over these. Per-provider and deliberately unshared.
_PAGE_NO_PARAM: Final[str] = 'page_no'
_PER_PAGE_PARAM: Final[str] = 'per_page'


class _MotivePageEcho(BaseModel):
    """The pagination block Motive echoes beside every page's records."""

    model_config = ConfigDict(frozen=True, extra='ignore')

    page_no: int
    per_page: int
    total: int


class _MotiveEnvelope(BaseModel):
    """Envelope slice: locates the echo; the records key (and anything
    provider-additive) is deliberately ignored."""

    model_config = ConfigDict(frozen=True, extra='ignore')

    pagination: _MotivePageEcho


@dataclass(frozen=True, slots=True)
class MotivePagination:
    """
    Page-numbered pagination over Motive's echoed ``pagination`` block.

    Attributes:
        per_page: The endpoint's page size. Cap enforcement belongs to
            the endpoint definition, not here.
    """

    per_page: int

    def first_request(self, spec: RequestSpec) -> RequestSpec:
        """Merge the first page's number and the page size into the spec."""
        return spec.with_merged_params(
            {_PAGE_NO_PARAM: '1', _PER_PAGE_PARAM: str(self.per_page)}
        )

    def advance(self, sent: RequestSpec, envelope: JsonValue) -> PageAdvance:
        """
        Compute the verdict from the page's freshly echoed pagination block.

        Args:
            sent: The spec that produced this page (unused; Motive's
                state lives entirely in the echo).
            envelope: The parsed response body.

        Returns:
            The verdict; ``durable_progress`` is always None — Motive
            cursors are fetch-private.

        Raises:
            ProviderResponseError: When the echo is structurally
                violating.
        """
        echo = validate_pagination_envelope(_MotiveEnvelope, envelope).pagination
        # page_no * per_page >= total is the ceil comparison
        # page_no >= ceil(total / per_page) without the division.
        if echo.page_no * echo.per_page >= echo.total:
            return PageAdvance(next_spec=None, durable_progress=None)
        # Echo the RESPONSE's values, not this strategy's field: each
        # page's echo is the fresh truth, so mid-pagination drift in
        # total (or a server-clamped per_page) self-corrects on the
        # next comparison — which is also why no empty-page guard
        # exists here.
        return PageAdvance(
            next_spec=sent.with_merged_params(
                {
                    _PAGE_NO_PARAM: str(echo.page_no + 1),
                    _PER_PAGE_PARAM: str(echo.per_page),
                }
            ),
            durable_progress=None,
        )
