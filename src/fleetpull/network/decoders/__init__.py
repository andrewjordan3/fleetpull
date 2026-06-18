# src/fleetpull/network/decoders/__init__.py
"""Per-provider page decoders: envelope interpreters that yield records
and a pagination verdict in one pass.

Implementations of the ``PageDecoder`` protocol (in
``network/contract/page_decoder.py``); peers of the contract surface,
imported by bindings through this face. Each decoder is independent --
provider envelopes evolve separately (blast-radius over DRY).
"""

from fleetpull.network.decoders.geotab import GeotabFeedPageDecoder
from fleetpull.network.decoders.motive import MotiveWrappedListPageDecoder
from fleetpull.network.decoders.samsara import SamsaraCursorPageDecoder
from fleetpull.network.decoders.single_page import SinglePageDecoder

__all__: list[str] = [
    'GeotabFeedPageDecoder',
    'MotiveWrappedListPageDecoder',
    'SamsaraCursorPageDecoder',
    'SinglePageDecoder',
]
