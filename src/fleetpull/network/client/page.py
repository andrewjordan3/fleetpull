# src/fleetpull/network/client/page.py
"""The client's emit type: one page of provider data plus resume progress."""

from dataclasses import dataclass

from fleetpull.network.contract import JsonValue

__all__: list[str] = ['FetchedPage']


@dataclass(frozen=True, slots=True)
class FetchedPage:
    """
    One page emitted by the transport client.

    Deliberately minimal: the raw envelope and the opaque resume cursor,
    nothing else. Records, attempt counts, and timings are not here until a
    consumer demands them — the client is records-blind and state-blind.

    Attributes:
        envelope: The parsed response body, exactly as the provider returned
            it. The client never extracts or reshapes records.
        durable_progress: Opaque resume cursor that must outlive the fetch
            (GeoTab ``toVersion``), or None for fetch-private cursors
            (Motive, Samsara). The orchestrator owns its interpretation.
    """

    envelope: JsonValue
    durable_progress: str | None
