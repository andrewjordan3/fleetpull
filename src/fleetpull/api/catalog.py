# src/fleetpull/api/catalog.py
"""The public ``Endpoints`` catalog: every exposed endpoint identity.

A static, committed module -- never codegen. Provider namespaces are
CapWords class-like containers (``Endpoints.Motive``, PEP 8's convention
for public class-like names); endpoint attributes are lowercase
identities typed snapshot or incremental, so the type checker enforces
``fetch``'s snapshot-only exposure at the call site. The load-bearing
identity everywhere strings live stays the lowercase ``Provider`` value,
so the CapWords surface introduces no string drift (DESIGN §10).

The drift protection is the two-way parity discipline test
(``tests/api/test_catalog.py``) against ``build_endpoint_registry``:
every identity here must resolve in the discovery registry with a
matching mode, and every discovered endpoint must appear here with the
mode-matching identity type.
"""

from fleetpull.api.identity import (
    EndpointIdentity,
    IncrementalEndpoint,
    SnapshotEndpoint,
)
from fleetpull.vocabulary import Provider

__all__: list[str] = ['Endpoints', 'available_endpoints']


class Endpoints:
    """Provider-namespaced catalog of every public endpoint identity.

    The namespaces hold inert identities only -- the verbs stay flat and
    provider-agnostic, so the catalog is organized by provider while the
    behavior is not (DESIGN §10, the orchestrator-boundary principle).
    """

    class Motive:
        """Motive endpoint identities."""

        vehicles: SnapshotEndpoint = SnapshotEndpoint(Provider.MOTIVE, 'vehicles')
        vehicle_locations: IncrementalEndpoint = IncrementalEndpoint(
            Provider.MOTIVE, 'vehicle_locations'
        )

    class Geotab:
        """GeoTab endpoint identities."""

        devices: SnapshotEndpoint = SnapshotEndpoint(Provider.GEOTAB, 'devices')
        trips: IncrementalEndpoint = IncrementalEndpoint(Provider.GEOTAB, 'trips')
        log_records: IncrementalEndpoint = IncrementalEndpoint(
            Provider.GEOTAB, 'log_records'
        )


def available_endpoints() -> tuple[EndpointIdentity, ...]:
    """Enumerate the whole catalog -- its manifest, in declaration order.

    Returns:
        Every identity the ``Endpoints`` catalog exposes.
    """
    return (
        Endpoints.Motive.vehicles,
        Endpoints.Motive.vehicle_locations,
        Endpoints.Geotab.devices,
        Endpoints.Geotab.trips,
        Endpoints.Geotab.log_records,
    )
