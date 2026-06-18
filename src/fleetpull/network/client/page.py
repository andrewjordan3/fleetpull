# src/fleetpull/network/client/page.py
"""The client's emit type: one page of records plus resume progress."""

from dataclasses import dataclass

from fleetpull.network.contract import JsonObject

__all__: list[str] = ['FetchedPage']


@dataclass(frozen=True, slots=True)
class FetchedPage:
    """
    One page emitted by the transport client: its records and the opaque
    resume cursor.

    The records are the page decoder's wire-shape extraction — a list of JSON
    objects, validated as record-bearing but not yet validated into the
    per-record response model (that is the records layer's job). Attempt counts
    and timings are not here until a consumer demands them — the client is
    state-blind.

    Attributes:
        records: The page's records, each a JSON object, as extracted by the
            endpoint's page decoder. Per-record model validation is downstream.
        durable_progress: Opaque resume cursor that must outlive the fetch
            (GeoTab ``toVersion``), or None for fetch-private cursors
            (Motive, Samsara). The orchestrator owns its interpretation.
    """

    records: list[JsonObject]
    durable_progress: str | None
