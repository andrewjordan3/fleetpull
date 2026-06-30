# src/fleetpull/endpoints/shared/fan_out.py
"""The per-endpoint fan-out declaration: ``FanOutBinding``.

What an endpoint that fans a request out over per-entity keys (the per-vehicle
``vehicle_locations`` endpoint) declares: which roster supplies its keys, and the
URL-path placeholder each key fills. ``FanOutBinding`` hangs on
``EndpointDefinition.fan_out`` (``None`` = fetch once), parallel to the
``spec_builder`` and ``page_decoder`` strategies -- a declared fact, so the
orchestrator never branches on endpoint identity to decide whether to fan out.

The binding names only a ``RosterKey``: the consumer knows *that* a roster of its keys
exists, never where those keys come from. The source endpoint and column -- and so the
feeder -- live in the ``RosterDefinition`` the ``RosterRegistry`` holds, keyed by that
``RosterKey``; the fan-out reads the members from the ``RosterStore``, also keyed by
it. That indirection keeps the consumer ignorant of the feeder: ``vehicle_locations``
references ``RosterKey(MOTIVE, 'vehicle_ids')`` and nothing about ``vehicles``.
"""

from dataclasses import dataclass

from fleetpull.roster import RosterKey

__all__: list[str] = ['FanOutBinding']


@dataclass(frozen=True, slots=True)
class FanOutBinding:
    """How an endpoint fans out: the roster key and the URL-path placeholder.

    Declared on the binding (``EndpointDefinition.fan_out``), parallel to the
    ``spec_builder`` and ``page_decoder`` strategies; ``None`` on the definition means
    the endpoint fetches once. The orchestrator reads this to fan a request per
    member: it resolves ``roster`` to its members (via the registry and store), then
    for each member builds ``path_values={path_placeholder: member}`` and calls the
    spec-builder. ``path_placeholder`` fills a URL-path template placeholder (e.g.
    ``{vehicle_id}``), not a query parameter -- ``path_values`` is path-substitution.

    Attributes:
        roster: The roster supplying this endpoint's fan-out keys -- the opaque
            handle; the source endpoint and column live in the registry's
            ``RosterDefinition``, not here.
        path_placeholder: The URL-path template placeholder each member fills (e.g.
            ``'vehicle_id'`` for ``'/v3/vehicle_locations/{vehicle_id}'``); must match
            the spec-builder's template, which the renderer enforces at request build.
    """

    roster: RosterKey
    path_placeholder: str
