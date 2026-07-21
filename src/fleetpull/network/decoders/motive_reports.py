# src/fleetpull/network/decoders/motive_reports.py
"""The Motive window-report decoder family: the utilization rollup surfaces.

``MotiveWindowReportPageDecoder`` decodes the utilization rollup surfaces
(``/v2/vehicle_utilization``, ``/v2/driver_utilization``; probe-settled
2026-07-21, DESIGN section 8), whose rows carry NO event-time key of any kind
-- each row is the provider's rollup over exactly the requested window, so the
decoder stamps every unwrapped record with the window the SENT spec asked for.
Envelope extraction and pagination are the sibling wrapped-list decoder's
(``motive.py``), composed by delegation; the window stamp is the shared
provider-uniform vocabulary (``_window_stamp.py``).
"""

from dataclasses import dataclass
from typing import Final

from fleetpull.network.contract import DecodedPage, RequestSpec
from fleetpull.network.decoders._window_stamp import window_stamp_from_sent_spec
from fleetpull.network.decoders.motive import MotiveWrappedListPageDecoder
from fleetpull.vocabulary import JsonValue

__all__: list[str] = ['MotiveWindowReportPageDecoder']

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
    page-numbered cursor -- composed by DELEGATION to an inner
    ``MotiveWrappedListPageDecoder`` (the Samsara series-decoder
    pattern), which handles ``first_request`` and the whole
    extraction-and-verdict pass verbatim -- plus
    the window-grain difference the Samsara fuel-energy pair
    established, applied to the inner page's records:

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

    def _list_decoder(self) -> MotiveWrappedListPageDecoder:
        """The inner wrapped-list decoder extraction and pagination delegate to."""
        return MotiveWrappedListPageDecoder(
            list_key=self.list_key, item_key=self.item_key, per_page=self.per_page
        )

    def first_request(self, spec: RequestSpec) -> RequestSpec:
        """Delegate page one verbatim to the inner wrapped-list decoder.

        The offset advance merges onto the sent spec, so the builder's
        ``start_date``/``end_date`` window persists across every page --
        and with it, the stamp.
        """
        return self._list_decoder().first_request(spec)

    def decode_page(self, sent: RequestSpec, envelope: JsonValue) -> DecodedPage:
        """Stamp the inner page's unwrapped records with the sent window.

        Args:
            sent: The spec that produced this page.
            envelope: The parsed response body.

        Returns:
            One record per rollup row, each carrying the synthesized
            ``windowStartDate``/``windowEndDate`` keys (class
            docstring); the pagination advance passes through the inner
            decoder untouched.

        Raises:
            ProviderResponseError: The sent spec lacks a window param
                (a wiring bug -- never silently unstamped rows), the
                record-bearing shape is structurally violating, or the
                pagination block is.
        """
        window_stamp = window_stamp_from_sent_spec(
            sent, start_param=_WINDOW_START_PARAM, end_param=_WINDOW_END_PARAM
        )
        inner_page = self._list_decoder().decode_page(sent, envelope)
        stamped = [{**record, **window_stamp} for record in inner_page.records]
        return DecodedPage(records=stamped, advance=inner_page.advance)
