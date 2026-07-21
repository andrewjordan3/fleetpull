# src/fleetpull/network/decoders/motive.py
"""Motive page decoders: wrapped-list records -- paginated, single-page,
and the window-stamping report composition
(sources: scrubbed provider-behavior verification, June 2026; the
utilization report surfaces probed 2026-07-21, DESIGN section 8).

Records arrive as a list of single-key wrappers under a per-endpoint
top-level key -- ``{"vehicles": [{"vehicle": {...}}, ...]}`` -- and the
``pagination`` block carries ``page_no``/``per_page``/``total``. Decoder
logic deliberately resembles its siblings without sharing code:
provider decoders evolve independently (blast-radius over DRY). WITHIN
this module the wrapped-list extraction and the offset-page verdict are
each written once (``_unwrap_wrapped_list`` / ``_offset_page_advance``)
and shared by every decoder that speaks them. The one deliberate
cross-module share is the window stamp (``_window_stamp.py``): the
synthesized keys are our own provider-uniform vocabulary, not envelope
logic.

``MotiveWindowReportPageDecoder`` decodes the utilization rollup
surfaces (``/v2/vehicle_utilization``, ``/v2/driver_utilization``),
whose rows carry NO event-time key of any kind -- each row is the
provider's rollup over exactly the requested window, so the decoder
stamps every unwrapped record with the window the SENT spec asked for
(probe-settled 2026-07-21, DESIGN section 8).
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
from fleetpull.network.decoders._window_stamp import window_stamp_from_sent_spec
from fleetpull.vocabulary import JsonObject, JsonValue

__all__: list[str] = [
    'MotiveWindowReportPageDecoder',
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


# The utilization report surfaces' window wire params (2026-07-21
# capture): the day-granular date-label pair every windowed Motive
# surface takes, inclusive on both ends and interpreted on COMPANY-LOCAL
# day boundaries on these surfaces. The shared window-stamp helper
# (`_window_stamp.py`) reads them back off the SENT spec to stamp each
# rollup row with the provider-uniform synthesized keys.
_WINDOW_START_PARAM: Final[str] = 'start_date'
_WINDOW_END_PARAM: Final[str] = 'end_date'


@dataclass(frozen=True, slots=True)
class MotiveWindowReportPageDecoder:
    """Decode window-grain rollup pages into window-stamped records.

    The decoder for ``GET /v2/vehicle_utilization`` and
    ``GET /v2/driver_utilization`` (probe-settled 2026-07-21, DESIGN
    section 8): the standard Motive wrapped-list envelope and
    page-numbered cursor -- extraction and verdict shared with
    ``MotiveWrappedListPageDecoder`` via the same-file helpers -- plus
    the window-grain difference the Samsara fuel-energy pair
    established:

    **The rollup grain is the request window.** Rows carry NO date or
    time identity of any kind; each row is the provider's aggregate over
    exactly the requested inclusive ``start_date``/``end_date`` label
    pair (a 1-day and a 6-day request each returned one rollup row per
    entity). So the decoder stamps every unwrapped record with the
    synthesized ``windowStartDate``/``windowEndDate`` keys, copied
    verbatim from the SENT spec's own ``start_date``/``end_date`` params
    -- the shared window-stamp vocabulary (``_window_stamp.py``),
    sourced from the sent spec rather than the record. The stamp wins
    any (census-impossible) key collision: it is the row's REQUIRED time
    identity, and a colliding future wire key must never silently
    supplant what was actually asked of the provider. A sent spec
    lacking either param raises loudly -- a wiring bug surfaced, never
    silently unstamped rows.

    Attributes:
        list_key: The top-level key holding the wrapper list
            (``'vehicle_utilizations'`` / ``'driver_idle_rollups'``).
        item_key: The key inside each wrapper holding the record
            (``'vehicle_utilization'`` / ``'driver_idle_rollup'``).
        per_page: The page size sent on the first request.
    """

    list_key: str
    item_key: str
    per_page: int

    def first_request(self, spec: RequestSpec) -> RequestSpec:
        """Send page one with ``page_no=1`` and the configured size.

        The offset advance merges onto the sent spec, so the builder's
        ``start_date``/``end_date`` window persists across every page --
        and with it, the stamp.
        """
        return spec.with_merged_params(
            {_PAGE_NO_PARAM: '1', _PER_PAGE_PARAM: str(self.per_page)}
        )

    def decode_page(self, sent: RequestSpec, envelope: JsonValue) -> DecodedPage:
        """Unwrap the records, stamp each with the sent window.

        Args:
            sent: The spec that produced this page.
            envelope: The parsed response body.

        Returns:
            One record per rollup row, each carrying the synthesized
            ``windowStartDate``/``windowEndDate`` keys (class
            docstring); the pagination verdict is the shared offset
            contract's.

        Raises:
            ProviderResponseError: The sent spec lacks a window param
                (a wiring bug -- never silently unstamped rows), the
                record-bearing shape is structurally violating, or the
                pagination block is.
        """
        window_stamp = window_stamp_from_sent_spec(
            sent, start_param=_WINDOW_START_PARAM, end_param=_WINDOW_END_PARAM
        )
        records = _unwrap_wrapped_list(envelope, self.list_key, self.item_key)
        stamped = [{**record, **window_stamp} for record in records]
        return DecodedPage(
            records=stamped, advance=_offset_page_advance(sent, envelope)
        )
