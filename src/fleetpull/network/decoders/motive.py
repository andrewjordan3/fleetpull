# src/fleetpull/network/decoders/motive.py
"""Motive page decoders: wrapped-list records, paginated and single-page
(sources: normalized provider-behavior verification, June 2026).

Records arrive as a list of single-key wrappers under a per-endpoint
top-level key -- ``{"vehicles": [{"vehicle": {...}}, ...]}`` -- and the
``pagination`` block carries ``page_no``/``per_page``/``total``. Decoder
logic deliberately resembles its siblings without sharing code:
provider decoders evolve independently (blast-radius over DRY).
"""

from dataclasses import dataclass
from typing import Final

from pydantic import BaseModel, ConfigDict

from fleetpull.network.contract import (
    DecodedPage,
    PageAdvance,
    RequestSpec,
    require_record_list,
    unwrap_record_objects,
    validated_envelope_slice,
)
from fleetpull.vocabulary import JsonObject, JsonValue

__all__: list[str] = [
    'MotiveWrappedListPageDecoder',
    'MotiveWrappedSinglePageDecoder',
]

# Wire-protocol tokens: Final constants, not an enum -- nothing
# dispatches over these. Per-provider and deliberately unshared.
_PAGE_NO_PARAM: Final[str] = 'page_no'
_PER_PAGE_PARAM: Final[str] = 'per_page'


class _MotivePageEcho(BaseModel):
    """The pagination block Motive returns on every page.

    strict=True refuses type drift on the integers the advance acts on
    (Motive elsewhere stringifies numerics); extra='ignore' tolerates
    the fields this slice does not name.
    """

    model_config = ConfigDict(frozen=True, extra='ignore', strict=True)

    page_no: int
    per_page: int
    total: int


class _MotiveEnvelope(BaseModel):
    """Envelope slice: locates the echo; the record key is ignored."""

    model_config = ConfigDict(frozen=True, extra='ignore', strict=True)

    pagination: _MotivePageEcho


@dataclass(frozen=True, slots=True)
class MotiveWrappedListPageDecoder:
    """Decode Motive's wrapped-list pages and page-numbered cursor.

    Attributes:
        list_key: The top-level key holding the wrapper list.
        item_key: The key inside each wrapper holding the record.
        per_page: The page size sent on the first request.
    """

    list_key: str
    item_key: str
    per_page: int

    def first_request(self, spec: RequestSpec) -> RequestSpec:
        """Send page one with ``page_no=1`` and the configured size."""
        return spec.with_merged_params(
            {_PAGE_NO_PARAM: '1', _PER_PAGE_PARAM: str(self.per_page)}
        )

    def decode_page(self, sent: RequestSpec, envelope: JsonValue) -> DecodedPage:
        """Extract the wrapped records and compute the page verdict.

        Args:
            sent: The spec that produced this page.
            envelope: The parsed response body.

        Returns:
            The unwrapped records and the pagination verdict.

        Raises:
            ProviderResponseError: When the record-bearing shape or the
                pagination block is structurally violating.
        """
        wrappers: list[JsonObject] = require_record_list(envelope, self.list_key)
        records: list[JsonObject] = unwrap_record_objects(wrappers, self.item_key)
        echo: _MotivePageEcho = validated_envelope_slice(
            _MotiveEnvelope, envelope
        ).pagination
        if echo.page_no * echo.per_page >= echo.total:
            return DecodedPage(
                records=records,
                advance=PageAdvance(next_spec=None, durable_progress=None),
            )
        next_spec: RequestSpec = sent.with_merged_params(
            {
                _PAGE_NO_PARAM: str(echo.page_no + 1),
                _PER_PAGE_PARAM: str(echo.per_page),
            }
        )
        return DecodedPage(
            records=records,
            advance=PageAdvance(next_spec=next_spec, durable_progress=None),
        )


@dataclass(frozen=True, slots=True)
class MotiveWrappedSinglePageDecoder:
    """Decode a single unpaginated page of Motive's wrapped-list records.

    The non-paginated sibling of ``MotiveWrappedListPageDecoder``: the same
    wrapped-list shape (a list of single-key wrappers under ``list_key``, each
    holding the record under ``item_key``), but the endpoint returns one page and
    no ``pagination`` block, so the first request is the base spec unchanged and
    every page is terminal. ``SinglePageDecoder`` does not fit -- it reads a
    top-level record list and never strips the per-item wrapper.

    Attributes:
        list_key: The top-level key holding the wrapper list.
        item_key: The key inside each wrapper holding the record.
    """

    list_key: str
    item_key: str

    def first_request(self, spec: RequestSpec) -> RequestSpec:
        """Return the base spec unchanged -- the endpoint is unpaginated."""
        return spec

    def decode_page(self, sent: RequestSpec, envelope: JsonValue) -> DecodedPage:
        """Unwrap the wrapped records; the page is always terminal.

        Args:
            sent: The spec that produced this page (unused; no continuation).
            envelope: The parsed response body.

        Returns:
            The unwrapped records and a terminal verdict.

        Raises:
            ProviderResponseError: When the record-bearing shape is structurally
                violating (missing list key, non-list value, missing item key, or
                a non-object record).

        Side Effects:
            None.
        """
        wrappers: list[JsonObject] = require_record_list(envelope, self.list_key)
        records: list[JsonObject] = unwrap_record_objects(wrappers, self.item_key)
        return DecodedPage(
            records=records,
            advance=PageAdvance(next_spec=None, durable_progress=None),
        )
