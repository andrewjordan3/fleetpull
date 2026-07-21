# src/fleetpull/network/decoders/motive.py
"""Motive page decoders: wrapped-list records, paginated and single-page
(sources: scrubbed provider-behavior verification, June 2026).

Records arrive as a list of single-key wrappers under a per-endpoint
top-level key -- ``{"vehicles": [{"vehicle": {...}}, ...]}`` -- and the
``pagination`` block carries ``page_no``/``per_page``/``total``. Decoder
logic deliberately resembles its siblings without sharing code:
provider decoders evolve independently (blast-radius over DRY). WITHIN
this module the wrapped-list extraction and the offset-page verdict are
each written once (``_unwrap_wrapped_list`` / ``_offset_page_advance``)
and shared by every decoder that speaks them.

The window-stamping report composition for the utilization rollup
surfaces (``MotiveWindowReportPageDecoder``) lives in
``motive_reports.py``, delegating to this module's wrapped-list decoder.
"""

from dataclasses import dataclass
from typing import Final

from fleetpull.network.contract import (
    DecodedPage,
    PageAdvance,
    RequestSpec,
    StrictEnvelopeSlice,
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


class _MotivePageEcho(StrictEnvelopeSlice):
    """The pagination block Motive returns on every page."""

    page_no: int
    per_page: int
    total: int


class _MotiveEnvelope(StrictEnvelopeSlice):
    """Envelope slice: locates the echo; the record key is ignored."""

    pagination: _MotivePageEcho


def _unwrap_wrapped_list(
    envelope: JsonValue, list_key: str, item_key: str
) -> list[JsonObject]:
    """Extract and unwrap one page's wrapped-list records.

    The one Motive record-bearing shape, written once for the decoders
    in this module (a same-file extraction, not a cross-provider
    abstraction): the wrapper list under ``list_key``, each wrapper's
    record under ``item_key``.

    Args:
        envelope: The parsed response body.
        list_key: The top-level key holding the wrapper list.
        item_key: The key inside each wrapper holding the record.

    Returns:
        The unwrapped record objects.

    Raises:
        ProviderResponseError: The record-bearing shape is structurally
            violating (missing list key, non-list value, missing item
            key, or a non-object record).
    """
    wrappers = require_record_list(envelope, list_key)
    return unwrap_record_objects(wrappers, item_key)


def _offset_page_advance(sent: RequestSpec, envelope: JsonValue) -> PageAdvance:
    """Compute one page's offset verdict from its ``pagination`` echo.

    The one page-numbered cursor contract every paginated Motive walk
    shares, written once for the decoders in this module: terminal when
    ``page_no * per_page >= total``; otherwise ``page_no + 1`` at the
    echoed size merges onto the SENT spec, so every first-request
    parameter (a window, a fixed selector) persists across the whole
    walk. ``durable_progress`` is always ``None`` -- Motive offsets are
    fetch-private.

    Args:
        sent: The spec that produced this page.
        envelope: The parsed response body.

    Returns:
        The page's pagination verdict.

    Raises:
        ProviderResponseError: The ``pagination`` block is structurally
            violating.
    """
    echo = validated_envelope_slice(_MotiveEnvelope, envelope).pagination
    if echo.page_no * echo.per_page >= echo.total:
        return PageAdvance(next_spec=None, durable_progress=None)
    next_spec = sent.with_merged_params(
        {
            _PAGE_NO_PARAM: str(echo.page_no + 1),
            _PER_PAGE_PARAM: str(echo.per_page),
        }
    )
    return PageAdvance(next_spec=next_spec, durable_progress=None)


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
        records = _unwrap_wrapped_list(envelope, self.list_key, self.item_key)
        return DecodedPage(
            records=records, advance=_offset_page_advance(sent, envelope)
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
        records = _unwrap_wrapped_list(envelope, self.list_key, self.item_key)
        return DecodedPage(
            records=records,
            advance=PageAdvance(next_spec=None, durable_progress=None),
        )
