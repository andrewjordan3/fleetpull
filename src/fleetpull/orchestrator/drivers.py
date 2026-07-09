# src/fleetpull/orchestrator/drivers.py
"""Request drivers: the run executor's request-cardinality seam.

A ``RequestDriver`` owns how many request chains one endpoint run issues, and yields
the run's fetched pages as a stream of batches. ``SingleRequestDriver`` issues
exactly one request chain (``path_values={}``) and yields its pages one at a time;
``FanOutRequestDriver`` issues one chain per supplied member
(``path_values={path_placeholder: member}``), fetching members concurrently on its
injected ``FetchPool`` and yielding each member's pages in member order -- the
member list is the caller's (the whole roster, fanned once per work unit's
window). ``path_values`` live only here -- the run executor
never builds them and the coordinator never supplies them; only the driver does. A
driver touches just the endpoint's ``SpecBuilder`` and the transport client, and
yields whole ``FetchedPage`` objects (records and durable progress); validation,
framing, and writing are the run executor's. The batch granularity is each driver's
own choice; the runner consumes batches uniformly.
"""

import logging
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from functools import partial
from typing import Protocol

from fleetpull.endpoints.shared import (
    CompletenessCheck,
    EndpointDefinition,
    ResumeValue,
)
from fleetpull.exceptions import ProviderResponseError
from fleetpull.model_contract import ResponseModel
from fleetpull.network.client import FetchedPage, TransportClient
from fleetpull.orchestrator.fanout import FetchPool, stream_pieces

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

    The one exception to the no-collection rule is a declared
    ``completeness_check``: that chain is a verified harvest -- buffered whole,
    compared against the provider-reported count, refetched once on mismatch --
    which is sound exactly because the declaration is construction-restricted to
    snapshots, whose results are bounded by entity count (DESIGN section 10's
    boundedness; the streaming law governs fan-out channels, not this).
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
            With a declared ``completeness_check`` the pages are the verified
            harvest's, yielded only after the count check passes.

        Raises:
            ProviderResponseError: A declared completeness check mismatched the
                harvest twice (from the verified harvest).
        """
        check = definition.completeness_check
        if check is None:
            yield from _stream_chain_pages(definition, client, resume, {})
            return
        yield from _verified_chain_pages(definition, client, resume, check)


def _verified_chain_pages(
    definition: EndpointDefinition[ResponseModel],
    client: TransportClient,
    resume: ResumeValue,
    check: CompletenessCheck,
) -> Iterator[FetchedPage]:
    """Drive the chain buffered, prove the count, then yield the pages.

    The verified harvest (probe-settled decision 2): buffer the full page
    sequence, fire the declared count check through the same client and quota
    scope, and compare. One refetch absorbs mid-harvest churn without inventing
    a tolerance number; a second mismatch is a provider-contract failure raised
    with both counts. Each round fires its own check, so the comparison is
    always against a count taken beside that round's harvest.

    Args:
        definition: The endpoint being run (its ``quota_scope`` prices the
            check's request).
        client: The open transport client the harvest and the check share.
        resume: The resume value injected into the first request (always
            ``None`` here -- the declaration is construction-restricted to
            snapshots).
        check: The endpoint's declared completeness check.

    Yields:
        The verified rounds' pages, in order, only after its count matched.

    Raises:
        ProviderResponseError: Expected and harvested counts disagreed on both
            rounds; the detail names both counts of the final round.
    """
    expected_count = 0
    harvested_count = 0
    for round_number in (1, 2):
        pages = list(_stream_chain_pages(definition, client, resume, {}))
        harvested_count = sum(len(page.records) for page in pages)
        expected_count = check.expected_count(client, definition.quota_scope.value)
        if harvested_count == expected_count:
            yield from pages
            return
        logger.warning(
            'completeness mismatch on %s.%s (round %d): provider expects %d '
            'records, harvest returned %d.',
            definition.provider.value,
            definition.name,
            round_number,
            expected_count,
            harvested_count,
        )
    raise ProviderResponseError(
        detail=(
            f'{definition.provider.value}.{definition.name}: completeness '
            f'check failed after one refetch -- provider expects '
            f'{expected_count} records, harvest returned {harvested_count}'
        )
    )


@dataclass(frozen=True, slots=True)
class FanOutRequestDriver:
    """Issue one request chain per member, fetched concurrently, yielded in order.

    The driver for endpoints that fan a request out over per-entity keys (the
    per-vehicle ``vehicle_locations`` endpoint). Each member is one piece: a
    worker on the injected ``fetch_pool`` runs that member's whole chain
    (``path_values={path_placeholder: member}``, tokens and the concurrency
    semaphore acquired per attempt exactly as a serial fetch would), and the
    consuming thread receives the pages through the bounded channel
    (``stream_pieces``) in member order -- so memory holds at most
    ``submission_window + 1`` members' pages at once, a function of the pool
    size, never of the roster. The member list is the caller's: the whole
    roster, fanned once per work unit's window (units carry no member key).
    ``path_values`` (and so the fan-out) live only here; the coordinator
    supplies the members and the placeholder already extracted, never
    ``path_values`` and never the endpoint's ``fan_out``.

    ``completeness_check`` is deliberately not consulted: a fan-out definition
    can never declare one (``EndpointDefinition`` rejects the pairing at
    construction), so this driver never buffers a run to verify it.

    Attributes:
        members: The fan-out keys to issue one chain each for, in order.
        path_placeholder: The URL-path template placeholder each member fills
            (from the endpoint's ``FanOutBinding.path_placeholder``).
        fetch_pool: The provider's fetch workers and channel bound (from the
            composition root's ``FetchPoolRegistry``; tests inject a
            synchronous same-thread executor through this same seam).
    """

    members: Sequence[str]
    path_placeholder: str
    fetch_pool: FetchPool

    def record_batches(
        self,
        definition: EndpointDefinition[ResponseModel],
        client: TransportClient,
        resume: ResumeValue,
    ) -> Iterator[FetchedPage]:
        """Yield each member's fetched pages, one chain per member, in member order.

        Members fetch concurrently on the pool's workers; pages still yield
        member by member, in the given order, so the consumer observes the
        serial loop's stream. The first failing member's exception fails the
        run: members past the channel's window are never requested, in-flight
        members finish and are discarded (a discarded failure is logged, never
        raised over the first).

        Args:
            definition: The endpoint being run.
            client: The transport client for this endpoint's provider (safe to
                share across the pool's workers -- the client is reentrant).
            resume: The resume value injected into every member's first request
                (the shared window -- one watermark, fanned across members).

        Yields:
            Each fetched page, member by member, in order. Each member drives at
            least one (possibly empty) page.
        """
        piece_tasks = (
            partial(self._fetch_member_pages, definition, client, resume, member)
            for member in self.members
        )
        yield from stream_pieces(piece_tasks, self.fetch_pool)

    def _fetch_member_pages(
        self,
        definition: EndpointDefinition[ResponseModel],
        client: TransportClient,
        resume: ResumeValue,
        member: str,
    ) -> list[FetchedPage]:
        """Fetch one member's whole chain -- the piece a pool worker executes.

        Runs on a worker thread: it touches the transport (which owns the
        limiter consultation) and nothing else -- no validation, no framing,
        no writing, per the single-writer invariant.

        Args:
            definition: The endpoint being run.
            client: The provider's reentrant transport client.
            resume: The resume value injected into the member's first request.
            member: The fan-out key this piece fetches.

        Returns:
            The member's pages, in chain order -- at least one (possibly
            empty) page.
        """
        return list(
            _stream_chain_pages(
                definition, client, resume, {self.path_placeholder: member}
            )
        )
