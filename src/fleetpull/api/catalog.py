# src/fleetpull/api/catalog.py
"""The public ``Endpoints`` catalog: every exposed endpoint identity.

A static, committed module -- never codegen. Provider namespaces are
CapWords class-like containers (``Endpoints.Motive``, PEP 8's convention
for public class-like names); endpoint attributes are lowercase
identities typed snapshot, windowed, or feed, so the type checker
enforces ``fetch``'s snapshot-only exposure at the call site. The load-bearing
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
    FeedEndpoint,
    SnapshotEndpoint,
    WindowedEndpoint,
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
        vehicle_locations: WindowedEndpoint = WindowedEndpoint(
            Provider.MOTIVE, 'vehicle_locations'
        )
        driving_periods: WindowedEndpoint = WindowedEndpoint(
            Provider.MOTIVE, 'driving_periods'
        )
        idle_events: WindowedEndpoint = WindowedEndpoint(Provider.MOTIVE, 'idle_events')
        groups: SnapshotEndpoint = SnapshotEndpoint(Provider.MOTIVE, 'groups')
        users: SnapshotEndpoint = SnapshotEndpoint(Provider.MOTIVE, 'users')
        vehicle_utilizations: WindowedEndpoint = WindowedEndpoint(
            Provider.MOTIVE, 'vehicle_utilizations'
        )
        driver_idle_rollups: WindowedEndpoint = WindowedEndpoint(
            Provider.MOTIVE, 'driver_idle_rollups'
        )

    class Samsara:
        """Samsara endpoint identities."""

        vehicles: SnapshotEndpoint = SnapshotEndpoint(Provider.SAMSARA, 'vehicles')
        drivers: SnapshotEndpoint = SnapshotEndpoint(Provider.SAMSARA, 'drivers')
        trips: WindowedEndpoint = WindowedEndpoint(Provider.SAMSARA, 'trips')
        idling_events: WindowedEndpoint = WindowedEndpoint(
            Provider.SAMSARA, 'idling_events'
        )
        addresses: SnapshotEndpoint = SnapshotEndpoint(Provider.SAMSARA, 'addresses')
        engine_states: WindowedEndpoint = WindowedEndpoint(
            Provider.SAMSARA, 'engine_states'
        )
        gps_readings: WindowedEndpoint = WindowedEndpoint(
            Provider.SAMSARA, 'gps_readings'
        )
        odometer_readings: WindowedEndpoint = WindowedEndpoint(
            Provider.SAMSARA, 'odometer_readings'
        )
        asset_locations: WindowedEndpoint = WindowedEndpoint(
            Provider.SAMSARA, 'asset_locations'
        )
        driver_vehicle_assignments: WindowedEndpoint = WindowedEndpoint(
            Provider.SAMSARA, 'driver_vehicle_assignments'
        )
        vehicle_fuel_energy_reports: WindowedEndpoint = WindowedEndpoint(
            Provider.SAMSARA, 'vehicle_fuel_energy_reports'
        )
        driver_fuel_energy_reports: WindowedEndpoint = WindowedEndpoint(
            Provider.SAMSARA, 'driver_fuel_energy_reports'
        )

    class Geotab:
        """GeoTab endpoint identities."""

        devices: SnapshotEndpoint = SnapshotEndpoint(Provider.GEOTAB, 'devices')
        users: SnapshotEndpoint = SnapshotEndpoint(Provider.GEOTAB, 'users')
        trips: WindowedEndpoint = WindowedEndpoint(Provider.GEOTAB, 'trips')
        exception_events: WindowedEndpoint = WindowedEndpoint(
            Provider.GEOTAB, 'exception_events'
        )
        log_records: FeedEndpoint = FeedEndpoint(Provider.GEOTAB, 'log_records')
        status_data: FeedEndpoint = FeedEndpoint(Provider.GEOTAB, 'status_data')
        fill_ups: FeedEndpoint = FeedEndpoint(Provider.GEOTAB, 'fill_ups')
        fuel_and_energy_used: FeedEndpoint = FeedEndpoint(
            Provider.GEOTAB, 'fuel_and_energy_used'
        )
        fuel_tax_details: FeedEndpoint = FeedEndpoint(
            Provider.GEOTAB, 'fuel_tax_details'
        )
        fault_data: FeedEndpoint = FeedEndpoint(Provider.GEOTAB, 'fault_data')
        duty_status_logs: FeedEndpoint = FeedEndpoint(
            Provider.GEOTAB, 'duty_status_logs'
        )
        driver_changes: FeedEndpoint = FeedEndpoint(Provider.GEOTAB, 'driver_changes')
        dvir_logs: FeedEndpoint = FeedEndpoint(Provider.GEOTAB, 'dvir_logs')
        annotation_logs: FeedEndpoint = FeedEndpoint(Provider.GEOTAB, 'annotation_logs')
        shipment_logs: FeedEndpoint = FeedEndpoint(Provider.GEOTAB, 'shipment_logs')
        audits: FeedEndpoint = FeedEndpoint(Provider.GEOTAB, 'audits')
        text_messages: FeedEndpoint = FeedEndpoint(Provider.GEOTAB, 'text_messages')
        media_files: FeedEndpoint = FeedEndpoint(Provider.GEOTAB, 'media_files')


def available_endpoints() -> tuple[EndpointIdentity, ...]:
    """Enumerate the whole catalog -- its manifest, in declaration order.

    Derived from the provider namespaces themselves: each namespace's
    class ``vars()`` preserves declaration order, so the manifest is the
    catalog read back, never a hand-kept repeat that could drift from it
    (the parity test still proves both directions against discovery).

    Returns:
        Every identity the ``Endpoints`` catalog exposes -- Motive, then
        Samsara, then GeoTab, each in declaration order.
    """
    provider_namespaces = (Endpoints.Motive, Endpoints.Samsara, Endpoints.Geotab)
    return tuple(
        attribute
        for namespace in provider_namespaces
        for attribute in vars(namespace).values()
        if isinstance(attribute, (SnapshotEndpoint, WindowedEndpoint, FeedEndpoint))
    )
