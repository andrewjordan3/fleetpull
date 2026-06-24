# src/fleetpull/endpoints/shared/fan_out.py
"""The per-endpoint fan-out declaration: ``FanOutSource`` and ``FanOutSpec``.

The data an endpoint that fans a request out over per-entity keys (the per-vehicle
``vehicle_locations`` endpoint) declares: where its keys come from, and the URL-path
placeholder each key fills. ``FanOutSpec`` hangs on ``EndpointDefinition.fan_out``
(``None`` = fetch once), parallel to the ``spec_builder`` and ``page_decoder``
strategies -- a declared fact, so the orchestrator never branches on endpoint identity
to decide whether to fan out.

The keys are not read here, and not read from a parquet: the orchestrator lists the
feeder, persists the keys to the SQLite roster (``state/rosters.py``), and fans the
request out from the roster. These types only declare the source and the placeholder.
``FanOutSource`` is the feeder identity the roster is keyed by, which is why the store
takes its three fields as primitives -- ``state`` sits below ``endpoints`` and cannot
import this module.
"""

from dataclasses import dataclass

from fleetpull.vocabulary import Provider

__all__: list[str] = ['FanOutSource', 'FanOutSpec']


@dataclass(frozen=True, slots=True)
class FanOutSource:
    """The feeder endpoint and column that supply an endpoint's fan-out keys.

    Names where a fan-out's keys come from: a feeder endpoint's records, read at
    roster-refresh time, not its output parquet. On a refresh the feeder is listed, its
    records validated to a frame, and ``column``'s distinct values become the roster's
    keys; the fan-out then reads the roster, never the feeder's persisted dataset (that
    is the user's product). ``provider`` always equals the consuming endpoint's own
    provider -- the feeder is same-provider -- but is carried here so the source is
    self-contained and keys the roster.

    Attributes:
        provider: Provider that owns the feeder endpoint.
        endpoint: The feeder endpoint whose listing supplies the keys.
        column: The feeder frame column whose distinct values become the keys -- the
            column name after the records-layer flatten (the model field name, e.g.
            ``'vehicle_id'``), not the wire key.
    """

    provider: Provider
    endpoint: str
    column: str

    @property
    def discriminator(self) -> str:
        """A stable, compact source identifier for logs and diagnostics.

        Not a storage key -- the roster keys on the three fields as separate columns;
        this string is human-readable log and error context only.
        """
        return f'{self.provider.value}.{self.endpoint}.{self.column}'


@dataclass(frozen=True, slots=True)
class FanOutSpec:
    """How an endpoint fans out: the key source and the URL-path placeholder.

    Declared on the binding (``EndpointDefinition.fan_out``), parallel to the
    ``spec_builder`` and ``page_decoder`` strategies; ``None`` on the definition means
    the endpoint fetches once. The orchestrator reads this to fan a request per key: it
    lists the ``source``, persists the keys to the roster, then for each member builds
    ``path_values={path_placeholder: member}`` and calls the spec-builder.
    ``path_placeholder`` fills a URL-path template placeholder (e.g. ``{vehicle_id}``),
    not a query parameter -- ``path_values`` is path-substitution by definition; a
    query-parameter fan-out would be a separate mechanism.

    Attributes:
        source: Where the fan-out keys come from.
        path_placeholder: The URL-path template placeholder each key fills (e.g.
            ``'vehicle_id'`` for ``'/v3/vehicle_locations/{vehicle_id}'``); must match
            the spec-builder's template, which the renderer enforces at request build.
    """

    source: FanOutSource
    path_placeholder: str
