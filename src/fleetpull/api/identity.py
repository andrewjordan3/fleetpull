# src/fleetpull/api/identity.py
"""The public endpoint identities: snapshot vs windowed vs feed at the type level.

An identity is the ``(provider, name)`` pair the endpoint registry keys
on and nothing more -- inert public data, never behavior. The private
``EndpointDefinition`` (spec builder, page decoder, response model) stays
behind the verbs; a consumer holds only these identities, obtained from
the ``Endpoints`` catalog (``api/catalog.py``).

Distinct types, not one type with a mode field, because the split IS the
public exposure gate (DESIGN §10): ``fetch`` accepts only
``SnapshotEndpoint``, so handing it a windowed or feed identity fails
mypy before it can fail at runtime. A snapshot result is bounded by
entity count -- the property ``fetch``'s in-memory contract stands on; a
windowed result is not, and a feed result is an unbounded version stream
with durable cursor state besides, so both belong to the config-driven
sync path.
"""

from dataclasses import dataclass

from fleetpull.vocabulary import Provider

__all__: list[str] = [
    'EndpointIdentity',
    'FeedEndpoint',
    'SnapshotEndpoint',
    'WindowedEndpoint',
]


@dataclass(frozen=True, slots=True)
class SnapshotEndpoint:
    """A snapshot-mode endpoint's public identity.

    The endpoint returns its full current-state dataset every fetch, so
    its result is bounded by entity count. Carries exactly the registry
    key.

    Attributes:
        provider: The provider the endpoint belongs to.
        name: The endpoint's name (e.g. ``'vehicles'``).
    """

    provider: Provider
    name: str


@dataclass(frozen=True, slots=True)
class WindowedEndpoint:
    """A windowed (non-snapshot) endpoint's public identity.

    The endpoint's result grows with window width and fleet activity --
    unbounded by anything a caller controls in memory -- so it is
    addressable only through the config-driven sync path; ``fetch``
    rejects it statically and at runtime. Carries exactly the registry
    key.

    Attributes:
        provider: The provider the endpoint belongs to.
        name: The endpoint's name (e.g. ``'vehicle_locations'``).
    """

    provider: Provider
    name: str


@dataclass(frozen=True, slots=True)
class FeedEndpoint:
    """A feed-mode endpoint's public identity.

    The endpoint drives a provider version-token stream (GeoTab
    ``GetFeed``) whose result is unbounded and whose resume rests on a
    durable stored cursor -- both properties ``fetch``'s stateless
    in-memory contract cannot hold -- so it is addressable only through
    the config-driven sync path; ``fetch`` rejects it statically and at
    runtime. Carries exactly the registry key.

    Attributes:
        provider: The provider the endpoint belongs to.
        name: The endpoint's name (e.g. ``'log_records'``).
    """

    provider: Provider
    name: str


# Every identity the catalog can hold: the union the mode-agnostic
# surfaces (the catalog manifest, the auth ingress) accept.
type EndpointIdentity = SnapshotEndpoint | WindowedEndpoint | FeedEndpoint
