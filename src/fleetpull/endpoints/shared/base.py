# src/fleetpull/endpoints/shared/base.py
"""The endpoints-layer binding: the ``EndpointDefinition`` and the types it composes.

An ``EndpointDefinition`` is the single source of truth per endpoint — a frozen,
keyword-only dataclass that composes one implementation per behavioral axis (the
``SpecBuilder`` and the ``PageDecoder``) plus the per-endpoint facts the generic
machinery reads (DESIGN §11). It is a thin declarative binding, not a fat base
class: the network layer already owns auth, pagination, classification, and
parsing as separate strategies, so the only work that remains on the endpoint is
its spec-builder.

This module ships the binding, the two Protocols it defines (``SpecBuilder``
and ``CompletenessCheck``; the ``PageDecoder`` it composes is imported from the
contract), and the ``ResumeValue`` alias; the declaration families the binding
composes live in their own modules -- the ``RequestShape`` union in
``request_shape.py``, ``StorageKind`` and the ``SyncMode`` union
(``SnapshotMode`` / ``WatermarkMode`` / ``FeedMode``) in ``sync_mode.py``. The
``event_time_column`` the watermark and the partitioned layouts read
(§3/§5) now ships here on the binding, validated at construction; the records
``schema_overrides`` hatch (§9) is the one contract piece still deferred.
"""

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Protocol, get_args

from fleetpull.endpoints.shared.request_shape import (
    BatchedRosterFanOut,
    BisectedWindowFetch,
    ParamSweep,
    RequestShape,
    RosterFanOut,
    SingleFetch,
)
from fleetpull.endpoints.shared.sync_mode import (
    FeedMode,
    SnapshotMode,
    StorageKind,
    SyncMode,
    WatermarkMode,
)
from fleetpull.incremental import DateWindow, FeedSeed, FeedToken
from fleetpull.model_contract import ResponseModel
from fleetpull.network.contract import EnvelopeFetcher, PageDecoder, RequestSpec
from fleetpull.vocabulary import Provider, QuotaScope

__all__: list[str] = [
    'CompletenessCheck',
    'EndpointDefinition',
    'ResumeValue',
    'SpecBuilder',
]


# The resume value a spec-builder consumes: a DateWindow (watermark, from the
# resume resolver), a FeedToken (feed, the stored token), a FeedSeed (feed, the
# cold-start anchor on the tokenless first run), or None — meaning no resume at
# all, i.e. a snapshot every run. Named here with SpecBuilder because it is the
# spec-builder's input contract; the resolver returns stay narrower per arm
# (DateWindow | None for the watermark resolver, FeedResume for the feed one).
type ResumeValue = DateWindow | FeedToken | FeedSeed | None


class SpecBuilder(Protocol):
    """
    Builds the first request for an endpoint — the one genuine per-endpoint behavior.

    Builds only the first request (URL, base params, and the resume injection);
    the page decoder produces every request after it. This is where the canonical
    half-open ``DateWindow`` (§4) is translated to the provider's own request
    convention. A plain Protocol (not ``@runtime_checkable``): it is only composed
    into and called through the stateless ``EndpointDefinition``, never verified
    dynamically.
    """

    def build_spec(
        self, resume: ResumeValue, member_values: Mapping[str, str]
    ) -> RequestSpec:
        """
        Build the endpoint's first request.

        Args:
            resume: The resume value to inject — a ``DateWindow`` (watermark),
                a ``FeedToken`` or ``FeedSeed`` (feed), or ``None`` (snapshot).
            member_values: The per-chain member binding the request shape
                supplies — an empty mapping for a single-chain run, one
                ``{member_key: member}`` entry per fan-out or sweep chain. The
                spec builder owns the interpretation: a URL-path placeholder
                value for a roster fan-out (e.g. a per-vehicle locations
                endpoint), a query-parameter value for a param sweep.

        Returns:
            The first ``RequestSpec``; the page decoder builds every request after it.
        """
        ...


class CompletenessCheck(Protocol):
    """
    A provider-reported expected count fired beside a snapshot harvest.

    The truth instrument behind the single-fetch driver's verified
    harvest (an endpoint that silently caps its listing needs an
    independent count to prove the walk lost nothing -- GeoTab's
    ``GetCountOf`` beside the capped ``Get`` is the first case). An
    implementation fires its count request through the SAME open client
    the harvest used, so auth, the limiter (token-per-attempt on the
    given scope), and the classifier all apply -- typed against the
    contract's one-method ``EnvelopeFetcher``, which ``TransportClient``
    satisfies structurally, so this binding layer never imports the
    client. A plain Protocol (not
    ``@runtime_checkable``): it is only declared on the stateless
    ``EndpointDefinition`` and called by the driver, never verified
    dynamically.
    """

    def expected_count(self, client: EnvelopeFetcher, quota_scope: str) -> int:
        """
        Return the provider-reported count of the harvested entity.

        Args:
            client: The open transport client the harvest ran on (its
                single-request ``fetch_envelope`` surface).
            quota_scope: The endpoint's rate-limit scope key -- the
                count request spends from the same budget as the data
                pages.

        Returns:
            The provider's expected entity count.
        """
        ...


@dataclass(frozen=True, slots=True, kw_only=True)
class EndpointDefinition[ModelT: ResponseModel]:
    """
    The single source of truth per endpoint: a frozen binding, one strategy per axis.

    A frozen, keyword-only dataclass generic over its per-record response model,
    composing one implementation per behavioral axis (``spec_builder``,
    ``page_decoder`` — swappable implementations) plus the per-endpoint facts. It
    does no work itself except through its ``spec_builder``.

    Never subclassed per provider. Variation lives in the composed strategies, not
    in subclasses — subclassing would re-braid the per-provider variation the
    network layer already pulled apart into separate strategies, recreating the
    predecessor's tangle. ``if endpoint.name == 'vehicle_locations':`` in the client
    is the failure mode; the fix is always a swapped-in strategy, never a branch or
    a subclass.

    Generic over ``ModelT`` (bound ``ResponseModel``). ``response_model`` is
    ``type[ModelT]``, so an ``EndpointDefinition[Vehicle]`` carries the concrete
    model type forward to a typed ``iter_records`` (§10). ``ModelT`` appears only in
    that output position, so a heterogeneous collection of definitions uses the base
    bound, ``EndpointDefinition[ResponseModel]`` (that collection is a later
    prompt's concern). ``response_model`` is the per-record model (e.g. ``Vehicle``),
    not a full-response wrapper — the ``page_decoder`` owns the envelope, so
    fleet-telemetry-hub's wrapper models are not ported, only the per-record ones.

    Keyword-only because positional fields here are an error waiting to happen. The
    ``event_time_column`` the watermark and date-partitioning read (§3/§5) now
    attaches here, validated against the response model at construction; the
    records ``schema_overrides`` / ``coercion_overrides`` hatch (§9) is the one
    excluded concern still deferred.

    Attributes:
        provider: The provider this endpoint belongs to.
        name: The endpoint's name (e.g. ``'vehicles'``).
        spec_builder: Builds the first request (the one per-endpoint behavior).
        page_decoder: Interprets each response envelope — the page's records and
            its pagination verdict, from one validated view.
        response_model: The per-record response model type.
        quota_scope: Which token bucket this endpoint spends from.
        storage_kind: The §3 storage layout (single file vs date-partitioned) —
            layout only; merge semantics follow ``sync_mode``.
        sync_mode: The sync-mode declaration (``SnapshotMode`` / ``WatermarkMode``
            / ``FeedMode``) — drives resume and write semantics.
        event_time_column: The response model's UTC datetime field the watermark
            and the partitioned layouts read (§3/§5) — e.g. ``'located_at'``.
            ``None`` for endpoints with no event-time dimension (every
            snapshot). Required for ``WatermarkMode`` / ``DATE_PARTITIONED`` /
            ``APPEND_LOG`` (the feed cell routes each record into its event
            date's partition by it), forbidden for snapshots; validated against
            ``response_model`` at construction.
        request_shape: The request-cardinality declaration — exactly one
            ``RequestShape`` member, so mutual exclusion between patterns is
            structural. Defaults to ``SingleFetch()`` (one chain), keeping
            single-chain leaves undeclared; ``RosterFanOut`` fans one chain per
            roster member, ``BatchedRosterFanOut`` one per comma-joined
            roster batch, ``ParamSweep`` one per declared query-param value,
            ``BisectedWindowFetch`` fetches each unit window whole and halves
            on overflow. Resolved to a request driver by the orchestrator's
            shape resolution; semantic sync-mode pairings are validated here at
            construction.
        completeness_check: The provider-reported-count truth check the
            single-fetch driver fires after the harvest streams (``None``
            for endpoints with no silent cap to guard against).
            Snapshot-mode, ``SingleFetch``-shaped endpoints only -- an
            expected-count comparison is meaningful only against one complete
            listing, which a windowed (partial) or per-member (fan-out /
            sweep) run never is; any other declaration is a wiring error
            rejected at construction.
    """

    provider: Provider
    name: str
    spec_builder: SpecBuilder
    page_decoder: PageDecoder
    response_model: type[ModelT]
    quota_scope: QuotaScope
    storage_kind: StorageKind
    sync_mode: SyncMode
    event_time_column: str | None = None
    request_shape: RequestShape = field(default_factory=SingleFetch)
    completeness_check: CompletenessCheck | None = None

    @property
    def required_event_time_column(self) -> str:
        """The declared event-time column, narrowed for cells that require one.

        Construction validation already guarantees a ``WatermarkMode``,
        ``DATE_PARTITIONED``, or ``APPEND_LOG`` binding declares the column
        (``_validate_event_time_column``), so a ``None`` here is impossible on
        any constructed binding -- this property states that narrowing once
        for every consumer instead of a per-call-site ``is None`` raise.

        Returns:
            The declared ``event_time_column``.

        Raises:
            RuntimeError: ``event_time_column`` is ``None`` -- construction
                validation was bypassed, a wiring bug surfaced loudly.
        """
        if self.event_time_column is None:
            raise RuntimeError(
                f'{self.provider.value}.{self.name}: event_time_column is None '
                f'on a cell that requires one -- construction validation '
                f'should have rejected this binding'
            )
        return self.event_time_column

    def __post_init__(self) -> None:
        """Validate the binding's storage / sync / event-time / shape coherence.

        Raises:
            ValueError: The storage-kind / sync-mode pairing is invalid; the
                event-time column is required-but-missing or forbidden-but-present
                or names no field on the response model; or the declared request
                shape (or the completeness check against it) fails a semantic
                sync-mode pairing.
            TypeError: ``event_time_column`` names a non-date-like field.

        Side Effects:
            None -- reads fields and may raise.
        """
        self._validate_storage_sync_pairing()
        self._validate_event_time_column()
        self._validate_request_shape()
        self._validate_completeness_check()

    def _validate_request_shape(self) -> None:
        """Reject a request shape (or its check pairing) outside its semantics.

        Mutual exclusion between cardinality patterns is structural (one
        ``request_shape`` field); what remains here are the semantic
        pairings. The roster-backed shapes (``RosterFanOut``,
        ``BatchedRosterFanOut``) require their roster to share the
        endpoint's provider -- ``Sync`` (§7) runs one queue per provider,
        and cross-queue independence rests on rosters never crossing
        providers (within a queue, the feeder barrier and the refresh
        coordinator's per-key single-flight serialize roster writes).
        ``BisectedWindowFetch`` recursively narrows a resume
        window, so it requires a windowed (``WatermarkMode``),
        date-partitioned endpoint. A ``completeness_check`` requires
        ``SnapshotMode`` AND ``SingleFetch`` -- an expected-count comparison
        is meaningful only against one complete listing, and a windowed
        harvest is deliberately partial while a fan-out or sweep run is
        per-member. ``ParamSweep`` composes with ``SnapshotMode`` (the sweep
        union is the full current listing); windowed sweep composition is
        untested against any provider, so non-snapshot sweeps are rejected
        loudly for now.

        Raises:
            ValueError: A pairing above is violated.

        Side Effects:
            None.
        """
        match self.request_shape:
            case RosterFanOut() | BatchedRosterFanOut() if (
                self.request_shape.roster.provider is not self.provider
            ):
                # Sync runs one queue per provider; the cross-queue
                # independence argument (§7) rests on rosters never
                # crossing providers -- a cross-provider roster would let
                # two queues reconcile one roster's rows concurrently,
                # outside the reach of either queue's feeder barrier.
                # Both roster-backed shapes carry the invariant.
                raise ValueError(
                    f'{self.provider.value}.{self.name}: '
                    f'{type(self.request_shape).__name__} roster '
                    f'{self.request_shape.roster.provider.value}/'
                    f'{self.request_shape.roster.name} crosses the provider '
                    f'boundary -- a roster and its consumer share one provider.'
                )
            case BisectedWindowFetch():
                if not isinstance(self.sync_mode, WatermarkMode) or (
                    self.storage_kind is not StorageKind.DATE_PARTITIONED
                ):
                    raise ValueError(
                        f'{self.provider.value}.{self.name}: BisectedWindowFetch '
                        f'requires WatermarkMode and DATE_PARTITIONED storage, '
                        f'got {type(self.sync_mode).__name__} / '
                        f'{self.storage_kind}.'
                    )
            case ParamSweep() if not isinstance(self.sync_mode, SnapshotMode):
                # Reopen note: a windowed sweep (one chain per value per
                # window) is coherent on paper but untested against any
                # provider's window-matching semantics -- widen this pairing
                # only with a probed consumer, not speculatively.
                raise ValueError(
                    f'{self.provider.value}.{self.name}: ParamSweep requires '
                    f'SnapshotMode, got {type(self.sync_mode).__name__} -- '
                    f'windowed sweep composition is unprobed.'
                )
            case _:
                pass

    def _validate_storage_sync_pairing(self) -> None:
        """Reject an invalid storage-kind / sync-mode pairing.

        A snapshot has no event-time dimension to partition on, so
        ``DATE_PARTITIONED`` is structurally unexecutable for it -- there is no
        column for ``split_by_date`` to split on (DESIGN §3). The feed pairing
        is exclusive in BOTH directions: ``FeedMode`` requires ``APPEND_LOG``
        (append-only is the feed stream's one write semantic — any other
        layout would delete or replace what the stored-as-emitted contract
        must keep), and ``APPEND_LOG`` requires ``FeedMode`` (a windowed or
        snapshot semantic run against an accumulate-only layout would either
        never clear its window or grow a snapshot without bound).

        Raises:
            ValueError: The endpoint is a snapshot with a non-``SINGLE``
                layout, a feed with a non-``APPEND_LOG`` layout, or an
                ``APPEND_LOG`` layout on a non-feed mode.

        Side Effects:
            None.
        """
        if (
            isinstance(self.sync_mode, SnapshotMode)
            and self.storage_kind is not StorageKind.SINGLE
        ):
            raise ValueError(
                f'{self.provider.value}.{self.name}: SnapshotMode requires '
                f'storage_kind SINGLE, got {self.storage_kind}.'
            )
        if (
            isinstance(self.sync_mode, FeedMode)
            and self.storage_kind is not StorageKind.APPEND_LOG
        ):
            raise ValueError(
                f'{self.provider.value}.{self.name}: FeedMode requires '
                f'storage_kind APPEND_LOG, got {self.storage_kind}.'
            )
        if self.storage_kind is StorageKind.APPEND_LOG and not isinstance(
            self.sync_mode, FeedMode
        ):
            raise ValueError(
                f'{self.provider.value}.{self.name}: APPEND_LOG requires '
                f'FeedMode, got {type(self.sync_mode).__name__}.'
            )

    def _validate_event_time_column(self) -> None:
        """Validate the event-time column against sync mode, layout, and model.

        Snapshots forbid it (no event-time dimension); ``WatermarkMode``,
        ``DATE_PARTITIONED``, and ``APPEND_LOG`` require it (the watermark
        reads it, and both partitioned layouts route rows into ``date=``
        partitions by it). When present it must name a date-like field on the
        response model, caught here at construction rather than mid-persist,
        where ``split_by_date`` or ``latest_event_time`` would otherwise fail
        on a non-temporal column after the fetch is already spent (DESIGN
        §3/§5).

        Raises:
            ValueError: The column is required-but-missing, forbidden-but-present,
                or names no field on the response model.
            TypeError: The named field is not date-like.

        Side Effects:
            None.
        """
        is_snapshot = isinstance(self.sync_mode, SnapshotMode)
        requires_event_time = isinstance(self.sync_mode, WatermarkMode) or (
            self.storage_kind in (StorageKind.DATE_PARTITIONED, StorageKind.APPEND_LOG)
        )
        if is_snapshot and self.event_time_column is not None:
            raise ValueError(
                f'{self.provider.value}.{self.name}: snapshot endpoints have no '
                f'event-time dimension, so event_time_column must be None.'
            )
        if requires_event_time and self.event_time_column is None:
            raise ValueError(
                f'{self.provider.value}.{self.name}: WatermarkMode, '
                f'DATE_PARTITIONED, or APPEND_LOG requires an '
                f'event_time_column.'
            )
        if self.event_time_column is not None:
            self._require_date_like_field(self.event_time_column)

    def _validate_completeness_check(self) -> None:
        """Reject a completeness check declared outside snapshot single-fetch.

        An expected-count comparison is meaningful only against a complete
        listing fetched as one chain: a snapshot's single chain fetches the
        entity's full current state, so the provider's count and the harvest
        describe the same population. A windowed harvest is deliberately
        partial (one window of an unbounded history) and a fan-out or sweep
        run is per-member -- on either, the comparison would be counting
        different things. Both declarations are wiring bugs, rejected here at
        the declaration seam so the driver layer never needs to reason about
        it.

        Raises:
            ValueError: The check is declared on a non-snapshot or
                non-``SingleFetch`` endpoint.

        Side Effects:
            None.
        """
        if self.completeness_check is None:
            return
        if not isinstance(self.sync_mode, SnapshotMode):
            raise ValueError(
                f'{self.provider.value}.{self.name}: a completeness_check '
                f'requires SnapshotMode -- an expected-count comparison is '
                f'meaningful only against a complete listing, and a windowed '
                f'harvest is deliberately partial.'
            )
        if not isinstance(self.request_shape, SingleFetch):
            raise ValueError(
                f'{self.provider.value}.{self.name}: a completeness_check '
                f'requires the SingleFetch request shape, got '
                f'{type(self.request_shape).__name__} -- a fan-out or sweep '
                f'run is per-member, not the one complete listing an expected '
                f'count describes.'
            )

    def _require_date_like_field(self, column: str) -> None:
        """Require ``column`` to name a date-like field on the response model.

        Validated as a top-level Pydantic field name: the records flatten
        preserves top-level field names as column names, so a top-level
        event-time field's column name equals its field name. A nested event-time
        field is not yet supported and would not resolve here (deferred until an
        endpoint needs one).

        Args:
            column: The event-time column name to check against the model.

        Raises:
            ValueError: ``column`` names no field on ``response_model``.
            TypeError: The field is annotated as neither ``date`` nor ``datetime``
                (nullable forms included).

        Side Effects:
            None.
        """
        model_fields = self.response_model.model_fields
        if column not in model_fields:
            valid_names = ', '.join(sorted(model_fields))
            raise ValueError(
                f'{self.provider.value}.{self.name}: event_time_column '
                f'{column!r} is not a field on {self.response_model.__name__}. '
                f'Fields: {valid_names}.'
            )
        annotation = model_fields[column].annotation
        if not _is_date_like_annotation(annotation):
            raise TypeError(
                f'{self.provider.value}.{self.name}: event_time_column '
                f'{column!r} must be date-like, got annotation {annotation!r}.'
            )


# A field annotation is an arbitrary type form (a class, a PEP 604 union, ...):
# typing-justified: Any is the honest input type for annotation inspection
def _is_date_like_annotation(annotation: Any) -> bool:
    """Whether an annotation resolves to ``date`` or ``datetime``.

    Nullable forms count: a provider's ``datetime | None`` timestamp is still a
    valid event-time field. Other unions and non-temporal types do not.

    Args:
        annotation: The field annotation to inspect.

    Returns:
        ``True`` if the annotation is ``date``, ``datetime``, or a two-arm
        optional of either.

    Side Effects:
        None.
    """
    return _unwrap_optional(annotation) in {date, datetime}


# typing-justified: annotation forms (unions, aliases) in, annotation forms out
def _unwrap_optional(annotation: Any) -> Any:
    """Strip ``None`` from a two-arm optional annotation, else return it as is.

    Args:
        annotation: The annotation to inspect.

    Returns:
        The sole non-``None`` arm of a two-arm ``X | None`` union; otherwise
        ``annotation`` unchanged. A union of more than two arms passes through, so
        a genuinely ambiguous union is not treated as date-like.

    Side Effects:
        None.
    """
    union_args = get_args(annotation)
    if not union_args:
        return annotation
    non_none_args = [arg for arg in union_args if arg is not type(None)]
    if len(non_none_args) == 1 and len(non_none_args) != len(union_args):
        return non_none_args[0]
    return annotation
