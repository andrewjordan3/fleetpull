# src/fleetpull/network/decoders/samsara_reports.py
"""The Samsara window-report decoder family: the fuel-energy report surfaces.

``SamsaraWindowReportPageDecoder`` decodes the fuel-energy report surfaces
(``/fleet/reports/{vehicles,drivers}/fuel-energy``; probe-settled 2026-07-21,
DESIGN section 8), whose record list nests one level deeper (``data`` is an
OBJECT holding the list under ``vehicleReports``/``driverReports``) and whose
rows carry NO event-time key of any kind -- each row is the provider's rollup
over exactly the requested window, so the decoder stamps every report with the
window the SENT spec asked for. Pagination and the first-page shape are the
sibling cursor module's (``samsara.py``: ``cursor_page_advance`` /
``first_page_spec``); the window stamp is the shared provider-uniform
vocabulary (``_window_stamp.py``).
"""

from dataclasses import dataclass
from typing import Final

from fleetpull.network.contract import (
    DecodedPage,
    RequestSpec,
    require_child_object,
    require_record_list,
)
from fleetpull.network.decoders._window_stamp import window_stamp_from_sent_spec
from fleetpull.network.decoders.samsara import cursor_page_advance, first_page_spec
from fleetpull.vocabulary import JsonValue

__all__: list[str] = ['SamsaraWindowReportPageDecoder']

# The fuel-energy report surfaces' window wire params (2026-07-21
# capture): these surfaces take startDate/endDate NAMES -- unlike every
# other probed Samsara vertical's startTime/endTime -- while accepting
# full RFC3339 datetimes despite the names. The shared window-stamp
# helper (`_window_stamp.py`) reads them back off the SENT spec to stamp
# each report row with the provider-uniform synthesized keys.
_WINDOW_START_PARAM: Final[str] = 'startDate'
_WINDOW_END_PARAM: Final[str] = 'endDate'


@dataclass(frozen=True, slots=True)
class SamsaraWindowReportPageDecoder:
    """Decode fuel-energy report pages into window-stamped records.

    The decoder for ``GET /fleet/reports/{vehicles,drivers}/fuel-energy``
    (probe-settled 2026-07-21, DESIGN section 8), whose envelope differs
    from the flat cursor surfaces twice over:

    - **The record list is NESTED.** ``data`` is an OBJECT whose only
      key is the per-surface report key (``vehicleReports`` /
      ``driverReports``), each a list of report objects -- extracted
      with the same structural-violation loudness ``require_record_list``
      gives flat lists.
    - **The rollup grain is the request window.** Report rows carry NO
      event-time key of any kind; each row is the provider's aggregate
      over exactly the requested window (widening the window GREW
      per-entity metrics, and day rollups are NOT additive into wider
      windows -- 89/267 mismatched). So the decoder stamps every report
      with the synthesized keys ``windowStartDate``/``windowEndDate``,
      copied verbatim from the SENT spec's own ``startDate``/``endDate``
      params -- the stats triple's synthesized-identity-keys precedent,
      sourced from the sent spec rather than the record. The stamp wins
      any (census-impossible) key collision: it is the row's REQUIRED
      time identity, and a colliding future wire key must never silently
      supplant what was actually asked of the provider -- the inverse of
      the series decoder's reading-keys-win order, where the synthesized
      keys are auxiliary attribution.

    Pagination is the standard cursor contract, shared via the sibling
    module's ``cursor_page_advance`` (real at scale: a 2-day
    vehicle window walked 3 pages/267 reports); ``first_request``
    injects ``limit`` exactly as the cursor decoder does.

    Attributes:
        records_key: The top-level key holding the report container
            object (``'data'``).
        report_key: The container key holding this surface's report
            list (``'vehicleReports'`` / ``'driverReports'``).
        results_limit: The per-page record count requested via the
            ``limit`` query parameter (pagination parameters are the
            decoder's, per the ``StaticGetSpecBuilder`` seam).
    """

    records_key: str
    report_key: str
    results_limit: int

    def first_request(self, spec: RequestSpec) -> RequestSpec:
        """Send page one via the shared first-page shape (``first_page_spec``)."""
        return first_page_spec(spec, self.results_limit)

    def decode_page(self, sent: RequestSpec, envelope: JsonValue) -> DecodedPage:
        """Extract the nested reports, stamp each with the sent window.

        Args:
            sent: The spec that produced this page.
            envelope: The parsed response body.

        Returns:
            One record per report, each carrying the synthesized
            ``windowStartDate``/``windowEndDate`` keys (class
            docstring); the pagination verdict is the shared cursor
            contract's.

        Raises:
            ProviderResponseError: The sent spec lacks a window param
                (a wiring bug -- never silently unstamped rows), the
                nested record-bearing shape is structurally violating,
                or the cursor block is (including continuation promised
                without a cursor).
        """
        window_stamp = window_stamp_from_sent_spec(
            sent, start_param=_WINDOW_START_PARAM, end_param=_WINDOW_END_PARAM
        )
        reports = require_record_list(
            require_child_object(envelope, self.records_key), self.report_key
        )
        stamped = [{**report, **window_stamp} for report in reports]
        return DecodedPage(records=stamped, advance=cursor_page_advance(sent, envelope))
