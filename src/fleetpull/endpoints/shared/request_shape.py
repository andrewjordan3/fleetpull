# src/fleetpull/endpoints/shared/request_shape.py
"""Request cardinality as one closed axis: the ``RequestShape`` union.

How one endpoint run decomposes into request chains is a single concept
and a single closed choice, exactly like ``SyncMode`` -- so it is one
tagged union, not a field per pattern. ``EndpointDefinition`` declares
exactly one member on ``request_shape``; the orchestrator's shape
resolution (``orchestrator/shape_resolution.py``) matches over the union
to pick the request driver. A future cardinality pattern is a new union
member plus its resolution arm -- the definition's field set never
changes for one again (unified 2026-07-20; DESIGN section 14 carries the
decision record).

Members: ``SingleFetch`` (one chain -- the default), ``RosterFanOut``
(one chain per roster member), ``BisectedWindowFetch`` (the unit window
fetched whole, halved adaptively on the capped-response overflow
signal), and ``ParamSweep`` (one chain per declared query-parameter
value).
"""

from dataclasses import dataclass
from datetime import timedelta

from fleetpull.roster import RosterKey

__all__: list[str] = [
    'BisectedWindowFetch',
    'ParamSweep',
    'RequestShape',
    'RosterFanOut',
    'SingleFetch',
]


@dataclass(frozen=True, slots=True)
class SingleFetch:
    """One request chain: the default request shape, a marker.

    The endpoint's whole run is one chain -- the spec-builder's first
    request plus every page the decoder walks. Declared implicitly:
    ``EndpointDefinition.request_shape`` defaults to this marker, so a
    single-chain leaf declares nothing.
    """


@dataclass(frozen=True, slots=True)
class RosterFanOut:
    """One request chain per roster member.

    The shape for endpoints that fan a request out over per-entity keys
    (the per-vehicle ``vehicle_locations`` endpoint). Names only a
    ``RosterKey``: the consumer knows *that* a roster of its keys
    exists, never where those keys come from. The source endpoint and
    column -- and so the feeder -- live in the ``RosterDefinition`` the
    ``RosterRegistry`` holds, keyed by that ``RosterKey``; the
    orchestration entry reads the members from the ``RosterStore``, also
    keyed by it. That indirection keeps the consumer ignorant of the
    feeder: ``vehicle_locations`` references
    ``RosterKey(MOTIVE, 'vehicle_ids')`` and nothing about ``vehicles``.

    Attributes:
        roster: The roster supplying this endpoint's fan-out members --
            the opaque handle; the source endpoint and column live in
            the registry's ``RosterDefinition``, not here.
        member_key: The key under which each member lands in the
            spec-builder's ``member_values``. The spec builder owns the
            interpretation; for URL-path endpoints it is the path
            template placeholder (e.g. ``'vehicle_id'`` for
            ``'/v3/vehicle_locations/{vehicle_id}'``), which the strict
            renderer enforces at request build.
    """

    roster: RosterKey
    member_key: str


@dataclass(frozen=True, slots=True)
class BisectedWindowFetch:
    """The unit window fetched whole, halved adaptively on overflow.

    The shape for a capped, unsortable Get endpoint (GeoTab
    ExceptionEvent: the silent 5,000 cap plus id-sort rejected outright
    -- DESIGN section 8, captured 2026-07-15): fetch the window whole; a
    response of exactly the declared limit is the overflow signal;
    discard it, halve the window, recurse; a floor-width window still
    coming back full fails loudly. Executed by the orchestrator's
    ``BisectingWindowDriver`` -- this declaration carries the provider
    facts the provider-agnostic driver cannot know.

    Attributes:
        results_limit: The per-request record limit the endpoint's spec
            builder writes; a response of exactly this many records is
            the overflow signal. Sound only where the provider's silent
            cap is Captured at or above this value for the entity type
            (a lower cap would make every page look partial and overflow
            undetectable).
        floor: The minimum window width. A floor-width window still
            returning ``results_limit`` records raises loudly -- the
            data is denser than windowed fetching can enumerate, or more
            than ``results_limit`` records overlap a single instant,
            which no window width resolves.
        event_time_wire_key: The raw wire key carrying each record's
            owning timestamp (e.g. ``'activeFrom'``) -- pre-model, so the
            driver can assign each record to exactly one leaf window
            under overlap-matched retrieval instead of leaning on
            write-time dedup for correctness.
    """

    results_limit: int
    floor: timedelta
    event_time_wire_key: str


@dataclass(frozen=True, slots=True)
class ParamSweep:
    """One request chain per declared query-parameter value.

    The shape for endpoints whose provider partitions the population
    behind a mandatory closed-enum filter with no all-values request
    (first consumer: Samsara drivers, where the default listing is only
    the active set). The union of the sweeps is the endpoint's one
    complete dataset -- the sweep is completeness machinery, never a
    member filter.

    Attributes:
        param: The member key the spec builder merges as a query
            parameter -- each sweep value lands in ``member_values``
            under this key.
        values: The closed, ordered value set, one chain each. Never
            empty and never duplicated -- either is a wiring bug
            rejected at construction.
    """

    param: str
    values: tuple[str, ...]

    def __post_init__(self) -> None:
        """Reject a sweep that could not be a complete dataset.

        Raises:
            ValueError: ``values`` is empty (a sweep over nothing fetches
                nothing and would silently emit an empty dataset) or
                carries a duplicate (the same partition fetched twice is
                a declaration typo, not a wider sweep).

        Side Effects:
            None -- reads fields and may raise.
        """
        if not self.values:
            raise ValueError(
                f'ParamSweep({self.param!r}): values must not be empty -- '
                f'a sweep over nothing is a wiring bug, not an empty dataset.'
            )
        if len(set(self.values)) != len(self.values):
            raise ValueError(
                f'ParamSweep({self.param!r}): values carry a duplicate -- '
                f'each declared value is one partition of the population, '
                f'fetched exactly once.'
            )


# The endpoint's request-cardinality declaration (config): the shape
# resolution matches on it to pick the request driver -- SingleFetch ->
# SingleRequestDriver, RosterFanOut / ParamSweep -> FanOutRequestDriver,
# BisectedWindowFetch -> BisectingWindowDriver. One declared member per
# endpoint; mutual exclusion is structural.
type RequestShape = SingleFetch | RosterFanOut | BisectedWindowFetch | ParamSweep
