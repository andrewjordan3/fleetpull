# src/fleetpull/orchestrator/drivers.py
"""Request drivers: the run executor's request-cardinality seam.

A ``RequestDriver`` owns how many request chains one endpoint run issues, and yields
the run's fetched pages as a stream of batches. ``SingleRequestDriver`` issues
exactly one request chain (``member_values={}``) and yields its pages one at a
time; ``FanOutRequestDriver`` issues one chain per supplied member
(``member_values={member_key: member}``), fetching members concurrently on its
injected ``FetchPool`` and yielding each member's pages in member order -- the
member list is the caller's (a ``RosterFanOut``'s whole roster, fanned once per
work unit's window, or a ``ParamSweep``'s declared values; the driver is
member-agnostic). ``member_values`` live only here -- the run executor never
builds them and the orchestration entry never supplies them; only the driver
does. A driver touches just the endpoint's ``SpecBuilder`` and the transport
client, and yields whole ``FetchedPage`` objects (records and durable
progress); validation, framing, and writing are the run executor's. The batch
granularity is each driver's own choice; the runner consumes batches uniformly.
"""

import logging
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from functools import partial
from typing import Final, Protocol

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

# The fan-out progress heartbeat's cadence: a narration cadence, not a
# correctness knob. A full-fleet fan-out of ~1,500 members narrates ~15
# progress lines; a small fleet emits at most one.
_MEMBER_PROGRESS_INTERVAL: Final[int] = 100


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
    member_values: Mapping[str, str],
) -> Iterator[FetchedPage]:
    """Issue one request chain and yield its fetched pages in order.

    The chain primitive both drivers compose: build the first request for
    ``member_values`` and stream every page. Yields each ``FetchedPage`` whole
    (records and durable progress), so the feed cursor token survives to the run
    executor; holds nothing across pages, so memory stays bounded by one page
    regardless of how wide the window or how many rows a member has.

    Args:
        definition: The endpoint being run (its ``spec_builder``, ``page_decoder``,
            and ``quota_scope``).
        client: The transport client for this endpoint's provider.
        resume: The resume value injected into the first request.
        member_values: The per-chain member binding -- empty for a lone chain,
            ``{member_key: member}`` for one fan-out or sweep member.

    Yields:
        Each fetched page in order. ``fetch_pages`` always drives at least one
        page, so at least one (possibly empty) page yields.
    """
    spec = definition.spec_builder.build_spec(
        resume=resume, member_values=member_values
    )
    yield from client.fetch_pages(
        spec, definition.page_decoder, definition.quota_scope.value
    )


class SingleRequestDriver:
    """Issue exactly one request chain and stream its pages one at a time.

    The driver for every ``SingleFetch``-shaped endpoint (snapshots, and any
    single-chain windowed endpoint). Builds the first request with
    ``member_values={}`` and yields each page as its own batch -- no per-chain
    collection, so the run executor writes (and the partitioned writer stages
    to disk) one page at a time.

    A declared ``completeness_check`` changes none of that streaming: the pages
    flow exactly as on the undeclared path while a running record count
    accumulates, and after the terminal page the provider-reported expected
    count is fetched once and compared -- a mismatch fails the run loudly (the
    next scheduled run is the retry; probe-settled decision 2 as amended
    2026-07-13).
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
            A declared ``completeness_check`` leaves the stream untouched; the
            count is proven after the terminal page.

        Raises:
            ProviderResponseError: A declared completeness check mismatched the
                harvest (raised after the final yield, before the consumer can
                treat the run as complete).
        """
        check = definition.completeness_check
        if check is None:
            yield from _stream_chain_pages(definition, client, resume, {})
            return
        yield from _stream_then_verify_pages(definition, client, resume, check)


def _stream_then_verify_pages(
    definition: EndpointDefinition[ResponseModel],
    client: TransportClient,
    resume: ResumeValue,
    check: CompletenessCheck,
) -> Iterator[FetchedPage]:
    """Stream the chain unbuffered, then prove the count after the last page.

    The completeness guard (probe-settled decision 2, amended 2026-07-13):
    pages yield exactly as the unguarded chain's do while a running record
    count accumulates; after the terminal page the declared check fires once
    through the same client and quota scope, and a mismatch raises with both
    counts -- exact match, no tolerance number, no refetch. The raise happens
    after the final yield, so it reaches the consuming runner before it can
    treat the run as complete: staging is discarded, the ledger row fails, and
    the prior parquet stands. The next scheduled run is the retry.

    Args:
        definition: The endpoint being run (its ``quota_scope`` prices the
            check's request).
        client: The open transport client the harvest and the check share.
        resume: The resume value injected into the first request (always
            ``None`` here -- the declaration is construction-restricted to
            snapshots).
        check: The endpoint's declared completeness check.

    Yields:
        Every fetched page, in order, unbuffered.

    Raises:
        ProviderResponseError: Expected and harvested counts disagree; the
            detail names both.
    """
    harvested_count = 0
    for page in _stream_chain_pages(definition, client, resume, {}):
        harvested_count += len(page.records)
        yield page
    expected_count = check.expected_count(client, definition.quota_scope.value)
    if harvested_count != expected_count:
        raise ProviderResponseError(
            detail=(
                f'{definition.provider.value}.{definition.name}: completeness '
                f'check failed -- provider expects {expected_count} records, '
                f'harvest returned {harvested_count}'
            )
        )


@dataclass(frozen=True, slots=True)
class FanOutRequestDriver:
    """Issue one request chain per member, fetched concurrently, yielded in order.

    The driver for endpoints that fan a request out over per-member values --
    a ``RosterFanOut``'s roster members (the per-vehicle ``vehicle_locations``
    endpoint) or a ``ParamSweep``'s declared values; the driver is
    member-agnostic and never knows which shape supplied its list. Each member
    is one piece: a worker on the injected ``fetch_pool`` runs that member's
    whole chain (``member_values={member_key: member}``, tokens and the
    concurrency semaphore acquired per attempt exactly as a serial fetch
    would), and the consuming thread receives the pages through the bounded
    channel (``stream_pieces``) in member order -- so memory holds at most
    ``submission_window + 1`` members' pages at once, a function of the pool
    size, never of the member count. The member list is the caller's: the
    whole roster or value set, fanned once per work unit's window (units carry
    no member key). ``member_values`` (and so the fan-out) live only here; the
    shape resolution supplies the members and the member key already
    extracted, never ``member_values`` and never the endpoint's
    ``request_shape``.

    ``completeness_check`` is deliberately not consulted: a fanned definition
    can never declare one (``EndpointDefinition`` rejects the pairing at
    construction) -- a per-member run is not the one complete listing an
    expected-count comparison would be meaningful against.

    Attributes:
        members: The member values to issue one chain each for, in order.
        member_key: The key each member binds under in ``member_values``
            (``RosterFanOut.member_key`` or ``ParamSweep.param``); the spec
            builder owns its interpretation.
        fetch_pool: The provider's fetch workers and channel bound (from the
            composition root's ``FetchPoolRegistry``; tests inject a
            synchronous same-thread executor through this same seam).
    """

    members: Sequence[str]
    member_key: str
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

        Progress narrates on this consuming side of the channel -- the one
        seam that sees every member exactly once, in member order, on the
        serial and threaded paths alike: one DEBUG per member, an INFO
        heartbeat every ``_MEMBER_PROGRESS_INTERVAL`` members, and one INFO
        when the fan-out drains. Never per page or per record -- that is
        flood, not progress.

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
        member_total = len(self.members)
        piece_tasks = (
            partial(self._fetch_member_pages, definition, client, resume, member)
            for member in self.members
        )
        for member_ordinal, (member, member_pages) in enumerate(
            stream_pieces(piece_tasks, self.fetch_pool), start=1
        ):
            logger.debug(
                'fetched member: provider=%s endpoint=%s member=%s (%d/%d)',
                definition.provider.value,
                definition.name,
                member,
                member_ordinal,
                member_total,
            )
            if member_ordinal % _MEMBER_PROGRESS_INTERVAL == 0:
                logger.info(
                    'fan-out progress: provider=%s endpoint=%s members=%d/%d',
                    definition.provider.value,
                    definition.name,
                    member_ordinal,
                    member_total,
                )
            yield from member_pages
        logger.info(
            'fan-out complete: provider=%s endpoint=%s members=%d',
            definition.provider.value,
            definition.name,
            member_total,
        )

    def _fetch_member_pages(
        self,
        definition: EndpointDefinition[ResponseModel],
        client: TransportClient,
        resume: ResumeValue,
        member: str,
    ) -> list[tuple[str, list[FetchedPage]]]:
        """Fetch one member's whole chain -- the piece a pool worker executes.

        Runs on a worker thread: it touches the transport (which owns the
        limiter consultation) and nothing else -- no validation, no framing,
        no logging, no writing, per the single-writer invariant. The
        one-element return keeps the channel yielding each member whole, so
        the consuming thread narrates member completions in member order;
        memory stays bounded by one member's whole page list per in-flight
        piece.

        Args:
            definition: The endpoint being run.
            client: The provider's reentrant transport client.
            resume: The resume value injected into the member's first request.
            member: The fan-out key this piece fetches.

        Returns:
            A one-element sequence pairing the member with its pages, in
            chain order -- at least one (possibly empty) page.
        """
        member_pages = list(
            _stream_chain_pages(definition, client, resume, {self.member_key: member})
        )
        return [(member, member_pages)]
