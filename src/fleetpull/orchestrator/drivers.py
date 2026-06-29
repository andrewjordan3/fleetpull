# src/fleetpull/orchestrator/drivers.py
"""Request drivers: the run executor's request-cardinality seam.

A ``RequestDriver`` owns how many request chains one endpoint run issues, and yields
the run's fetched pages as a stream of batches. ``SingleRequestDriver`` issues
exactly one request chain (``path_values={}``) and yields its pages one at a time;
``FanOutRequestDriver`` issues one chain per supplied member
(``path_values={path_placeholder: member}``) and yields each member's pages -- the
member list is the caller's, one member for a single backfill work unit, the whole
roster for an incremental run. ``path_values`` live only here -- the run executor
never builds them and the coordinator never supplies them; only the driver does. A
driver touches just the endpoint's ``SpecBuilder`` and the transport client, and
yields whole ``FetchedPage`` objects (records and durable progress); validation,
framing, and writing are the run executor's. The batch granularity is each driver's
own choice; the runner consumes batches uniformly.
"""

import logging
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

from fleetpull.endpoints.shared import EndpointDefinition, ResumeValue
from fleetpull.model_contract import ResponseModel
from fleetpull.network.client import FetchedPage, TransportClient

__all__: list[str] = ['FanOutRequestDriver', 'RequestDriver', 'SingleRequestDriver']

logger = logging.getLogger(__name__)


class RequestDriver(Protocol):
    """The request-cardinality seam: yield the run's fetched pages as batches.

    The run executor drives the returned iterator, consuming one batch per
    iteration: it reads ``page.records`` to validate -> frame -> write and
    ``page.durable_progress`` to advance a feed cursor. Both concrete drivers
    yield one fetched page per batch. A plain Protocol -- the run executor
    receives a concrete driver and calls it, never verifies it.
    """

    def record_batches(
        self,
        definition: EndpointDefinition[ResponseModel],
        client: TransportClient,
        resume: ResumeValue,
    ) -> Iterator[FetchedPage]:
        """Yield the run's fetched pages, one batch at a time.

        Each yielded ``FetchedPage`` carries the page's records and its durable
        progress; the run executor reads ``page.records`` to validate/frame/write
        and ``page.durable_progress`` to advance a feed cursor.

        Args:
            definition: The endpoint being run (read for its ``spec_builder``,
                ``page_decoder``, and ``quota_scope``).
            client: The transport client for this endpoint's provider.
            resume: The resume value injected into the first request -- ``None``
                for a snapshot, the resolved window for a watermark endpoint.

        Yields:
            One ``FetchedPage`` per batch, in order.
        """
        ...


def _stream_chain_pages(
    definition: EndpointDefinition[ResponseModel],
    client: TransportClient,
    resume: ResumeValue,
    path_values: Mapping[str, str],
) -> Iterator[FetchedPage]:
    """Issue one request chain and yield its fetched pages in order.

    The chain primitive both drivers compose: build the first request for
    ``path_values`` and stream every page. Yields each ``FetchedPage`` whole
    (records and durable progress), so the feed cursor token survives to the run
    executor; holds nothing across pages, so memory stays bounded by one page
    regardless of how wide the window or how many rows a member has.

    Args:
        definition: The endpoint being run (its ``spec_builder``, ``page_decoder``,
            and ``quota_scope``).
        client: The transport client for this endpoint's provider.
        resume: The resume value injected into the first request.
        path_values: The path-template substitutions -- empty for a lone chain,
            ``{path_placeholder: member}`` for one fan-out member.

    Yields:
        Each fetched page in order. ``fetch_pages`` always drives at least one
        page, so at least one (possibly empty) page yields.
    """
    spec = definition.spec_builder.build_spec(resume=resume, path_values=path_values)
    yield from client.fetch_pages(
        spec, definition.page_decoder, definition.quota_scope.value
    )


class SingleRequestDriver:
    """Issue exactly one request chain and stream its pages one at a time.

    The driver for every endpoint that fetches once (snapshots, and any non-fan-out
    endpoint). Builds the first request with ``path_values={}`` and yields each
    page as its own batch -- no per-chain collection, so the run executor writes
    (and the partitioned writer stages to disk) one page at a time.
    """

    def record_batches(
        self,
        definition: EndpointDefinition[ResponseModel],
        client: TransportClient,
        resume: ResumeValue,
    ) -> Iterator[FetchedPage]:
        """Yield one batch per page of the single request chain.

        Args:
            definition: The endpoint being run.
            client: The transport client for this endpoint's provider.
            resume: The resume value injected into the first request.

        Yields:
            One ``FetchedPage`` per page, in order. ``fetch_pages`` always drives
            at least one page, so at least one (possibly empty) batch yields.
        """
        yield from _stream_chain_pages(definition, client, resume, {})


@dataclass(frozen=True, slots=True)
class FanOutRequestDriver:
    """Issue one request chain per member, streaming each a page at a time.

    The driver for endpoints that fan a request out over per-entity keys (the
    per-vehicle ``vehicle_locations`` endpoint). For each member it builds the
    request with ``path_values={path_placeholder: member}`` and yields that
    member's pages -- nothing is accumulated per member, so memory stays bounded
    by one provider page no matter how wide the window or how many rows a member
    has. The member list is the caller's: one member for a single backfill work
    unit, the whole roster for an incremental run. ``path_values`` (and so the
    fan-out) live only here; the coordinator supplies the members and the
    placeholder already extracted, never ``path_values`` and never the endpoint's
    ``fan_out``.

    Attributes:
        members: The fan-out keys to issue one chain each for, in order.
        path_placeholder: The URL-path template placeholder each member fills
            (from the endpoint's ``FanOutBinding.path_placeholder``).
    """

    members: Sequence[str]
    path_placeholder: str

    def record_batches(
        self,
        definition: EndpointDefinition[ResponseModel],
        client: TransportClient,
        resume: ResumeValue,
    ) -> Iterator[FetchedPage]:
        """Yield each member's fetched pages, one chain per member, in order.

        Args:
            definition: The endpoint being run.
            client: The transport client for this endpoint's provider.
            resume: The resume value injected into every member's first request
                (the shared window -- one watermark, fanned across members).

        Yields:
            Each fetched page, member by member, in order. Each member drives at
            least one (possibly empty) page.
        """
        for member in self.members:
            yield from _stream_chain_pages(
                definition, client, resume, {self.path_placeholder: member}
            )
