# src/fleetpull/network/contract/paginators/samsara.py
"""Samsara pagination strategy (sources: scrubbed provider-behavior
verification, June 2026; cursor contract from provider documentation).

Cursor pagination: the first page sends no ``after`` param; every
response carries a ``pagination`` block with ``endCursor`` and
``hasNextPage``; subsequent pages send ``after=<endCursor>``. Branch
logic deliberately resembles sibling paginators without sharing code:
provider paginators evolve independently (blast-radius over DRY).
"""

from dataclasses import dataclass
from typing import Final

from pydantic import BaseModel, ConfigDict, Field

from fleetpull.exceptions import ProviderResponseError
from fleetpull.network.contract.pagination import (
    PageAdvance,
    validate_pagination_envelope,
)
from fleetpull.network.contract.request import JsonValue, RequestSpec

__all__: list[str] = ['SamsaraPagination']

# Wire-protocol token: Final constant, not an enum — nothing
# dispatches over it. Per-provider and deliberately unshared.
_AFTER_PARAM: Final[str] = 'after'


class _SamsaraPageEcho(BaseModel):
    """The pagination block Samsara returns on every page."""

    model_config = ConfigDict(frozen=True, extra='ignore')

    has_next_page: bool = Field(alias='hasNextPage')
    end_cursor: str | None = Field(default=None, alias='endCursor')


class _SamsaraEnvelope(BaseModel):
    """Envelope slice: locates the echo; records key ignored."""

    model_config = ConfigDict(frozen=True, extra='ignore')

    pagination: _SamsaraPageEcho


@dataclass(frozen=True, slots=True)
class SamsaraPagination:
    """Cursor pagination over Samsara's ``endCursor``/``hasNextPage``."""

    def first_request(self, spec: RequestSpec) -> RequestSpec:
        """Return the spec unchanged; the first page must NOT carry ``after``."""
        return spec

    def advance(self, sent: RequestSpec, envelope: JsonValue) -> PageAdvance:
        """
        Compute the verdict from the page's cursor block.

        Args:
            sent: The spec that produced this page.
            envelope: The parsed response body.

        Returns:
            The verdict; ``durable_progress`` is always None — Samsara
            cursors are fetch-private.

        Raises:
            ProviderResponseError: When the cursor block is
                structurally violating, including continuation promised
                without a cursor.
        """
        echo = validate_pagination_envelope(_SamsaraEnvelope, envelope).pagination
        if not echo.has_next_page:
            return PageAdvance(next_spec=None, durable_progress=None)
        if echo.end_cursor is None or echo.end_cursor == '':
            # Continuation promised without a cursor: silently
            # finishing here would truncate data — the one failure
            # mode a fetch library must never have.
            raise ProviderResponseError(
                detail='hasNextPage is true but endCursor is missing or empty'
            )
        return PageAdvance(
            next_spec=sent.with_merged_params({_AFTER_PARAM: echo.end_cursor}),
            durable_progress=None,
        )
