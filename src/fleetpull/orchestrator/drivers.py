# src/fleetpull/orchestrator/drivers.py
"""Request drivers: the run executor's request-cardinality seam.

A ``RequestDriver`` owns how many request chains one endpoint run issues, and yields
the records as a stream of batches. ``SingleRequestDriver`` issues exactly one
request chain (``path_values={}``) and yields its records one page at a time; the
fan-out driver (a later prompt) issues one chain per roster member and yields each
member's records as a batch. ``path_values`` live only here -- the run executor
never builds them and the coordinator never supplies them; only the driver does. A
driver touches just the endpoint's ``SpecBuilder`` and the transport client, and
yields raw records in batches; validation, framing, and writing are the run
executor's. The batch granularity is each driver's own choice; the runner consumes
batches uniformly.
"""

import logging
from collections.abc import Iterator
from typing import Protocol

from fleetpull.endpoints.shared import EndpointDefinition, ResumeValue
from fleetpull.model_contract import ResponseModel
from fleetpull.network.client import TransportClient
from fleetpull.network.contract import JsonObject

__all__: list[str] = ['RequestDriver', 'SingleRequestDriver']

logger = logging.getLogger(__name__)


class RequestDriver(Protocol):
    """The request-cardinality seam: yield the run's records as a stream of batches.

    The run executor drives the returned iterator, consuming one batch per iteration
    (validate -> frame -> write). The batch granularity is the driver's choice -- a
    page for ``SingleRequestDriver``, a roster member for the fan-out driver. A plain
    Protocol -- the run executor receives a concrete driver and calls it, never
    verifies it.
    """

    def record_batches(
        self,
        definition: EndpointDefinition[ResponseModel],
        client: TransportClient,
        resume: ResumeValue,
    ) -> Iterator[list[JsonObject]]:
        """Yield the run's raw records, one batch at a time.

        Args:
            definition: The endpoint being run (read for its ``spec_builder``,
                ``page_decoder``, and ``quota_scope``).
            client: The transport client for this endpoint's provider.
            resume: The resume value injected into the first request -- ``None`` for
                a snapshot, the resolved window for a watermark endpoint.

        Yields:
            One list of raw records per batch, in order.
        """
        ...


class SingleRequestDriver:
    """Issue exactly one request chain and stream its records a page at a time.

    The driver for every endpoint that fetches once (snapshots, and any non-fan-out
    endpoint). Builds the first request with ``path_values={}`` and yields each
    page's records as its own batch -- no per-chain collection, so the run executor
    writes (and the partitioned writer stages to disk) one page at a time.
    """

    def record_batches(
        self,
        definition: EndpointDefinition[ResponseModel],
        client: TransportClient,
        resume: ResumeValue,
    ) -> Iterator[list[JsonObject]]:
        """Yield one batch per page of the single request chain.

        Args:
            definition: The endpoint being run.
            client: The transport client for this endpoint's provider.
            resume: The resume value injected into the first request.

        Yields:
            One list of raw records per page, in order. ``fetch_pages`` always
            drives at least one page, so at least one (possibly empty) batch yields.
        """
        spec = definition.spec_builder.build_spec(resume=resume, path_values={})
        for page in client.fetch_pages(
            spec, definition.page_decoder, definition.quota_scope.value
        ):
            yield page.records
