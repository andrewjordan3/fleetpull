# src/fleetpull/network/decoders/__init__.py
"""Per-provider page decoders: envelope interpreters that yield records
and a pagination verdict in one pass.

Implementations of the ``PageDecoder`` protocol (in
``network/contract/page_decoder.py``); peers of the contract surface,
imported by bindings through this face. Each decoder is independent --
provider envelopes evolve separately (blast-radius over DRY). The one
shared package-internal piece is the window stamp (``_window_stamp.py``):
the synthesized window-identity keys are our own provider-uniform
vocabulary, not envelope logic.
"""

from fleetpull.network.decoders.geotab import (
    GeotabFeedPageDecoder,
    GeotabGetPageDecoder,
)
from fleetpull.network.decoders.motive import (
    MotiveWrappedListPageDecoder,
    MotiveWrappedSinglePageDecoder,
)
from fleetpull.network.decoders.motive_reports import MotiveWindowReportPageDecoder
from fleetpull.network.decoders.samsara import (
    SamsaraCursorPageDecoder,
    SamsaraVehicleSeriesPageDecoder,
)
from fleetpull.network.decoders.samsara_reports import SamsaraWindowReportPageDecoder
from fleetpull.network.decoders.single_page import SinglePageDecoder

__all__: list[str] = [
    'GeotabFeedPageDecoder',
    'GeotabGetPageDecoder',
    'MotiveWindowReportPageDecoder',
    'MotiveWrappedListPageDecoder',
    'MotiveWrappedSinglePageDecoder',
    'SamsaraCursorPageDecoder',
    'SamsaraVehicleSeriesPageDecoder',
    'SamsaraWindowReportPageDecoder',
    'SinglePageDecoder',
]
