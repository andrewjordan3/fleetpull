# src/fleetpull/orchestrator/roster_harvest.py
"""Roster harvest: a feeder's complete current membership as a set of strings.

``harvest_roster_members`` drives a feeder endpoint to a full listing and returns
the distinct values of its roster column -- the complete current membership the
refresh hands to ``reconcile``. It drives ``stream_processed_batches`` over the
feeder (validate-and-frame only, no window filter, no write) and unions
``extract_roster_members`` across the streamed batches.

The harvest is always a full listing: ``reconcile`` diffs the complete membership
against the stored roster (anything absent from the listing increments toward
eviction), so a partial listing would not under-update the roster -- it would evict
live members. ``resume`` and ``context`` are therefore fixed at ``None`` (the
validate-and-frame-only path that preserves every row); they are not parameters,
because a windowed harvest is not a harvest. The feeder must be a full-listing
(snapshot) endpoint for the same reason; the coordinator that resolves the feeder
binding guards that, so this stays ``sync_mode``-blind, driving whatever binding it
is handed.

This owns no state and resolves no client or binding: the coordinator resolves the
roster's ``source_endpoint`` to its ``EndpointDefinition``, builds the
``SingleRequestDriver``, opens the provider's client, and calls this.
"""

from fleetpull.endpoints.shared import EndpointDefinition
from fleetpull.model_contract import ResponseModel
from fleetpull.network.client import TransportClient
from fleetpull.orchestrator.drivers import RequestDriver
from fleetpull.orchestrator.streaming import stream_processed_batches
from fleetpull.records import extract_roster_members

__all__: list[str] = ['harvest_roster_members']


def harvest_roster_members(
    definition: EndpointDefinition[ResponseModel],
    driver: RequestDriver,
    client: TransportClient,
    column: str,
) -> set[str]:
    """Harvest a feeder's complete current membership as a set of strings.

    Drives the feeder to a full listing and returns the distinct stringified values
    of ``column`` across every batch -- the complete membership ``reconcile``
    consumes. The stream runs validate-and-frame only (``resume`` and ``context``
    ``None``), so no row is filtered out and the listing stays complete.

    Args:
        definition: The resolved feeder binding to list (its response model, spec
            builder, and page decoder). Must be a full-listing endpoint; the
            coordinator guards that before calling.
        driver: The request driver the feeder is listed with -- a
            ``SingleRequestDriver`` for the full single-chain listing.
        client: The feeder provider's open transport client.
        column: The feeder-frame column whose distinct values are the members (the
            roster's ``source_column``).

    Returns:
        The complete current membership -- the distinct values of ``column`` as
        strings, unioned across batches; empty when the feeder lists nothing.

    Raises:
        ValueError: ``column`` is absent from a feeder frame (from
            ``extract_roster_members``) -- surfaced to the coordinator, whose
            best-effort refresh keeps the existing roster on failure. Null and
            empty-string values do not raise; the extractor filters them loudly.
        FleetpullError: A fetch, validation, or framing failure from the stream.

    Side Effects:
        Issues the feeder's request chain through ``client`` (network I/O); writes
        nothing.
    """
    members: set[str] = set()
    for batch in stream_processed_batches(definition, driver, client, None, None):
        members |= extract_roster_members(batch.frame, column)
    return members
