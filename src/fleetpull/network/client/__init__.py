# src/fleetpull/network/client/__init__.py
"""The transport client face: the assembled HTTP fetch loop and its inputs.

``TransportClient`` runs the per-attempt pipeline and page loop against a
per-provider ``ProviderProfile`` (auth + classifier) and a process-global
``ClientRuntime`` (configs, limiter registry, jitter, sleeper), emitting
``FetchedPage`` objects. ``ProviderClientRegistry`` owns one open client per
provider, keyed by ``Provider``, and hands the right one to the run executor.
External callers import these names here."""

from fleetpull.network.client.page import FetchedPage
from fleetpull.network.client.profile import ProviderProfile
from fleetpull.network.client.registry import ProviderClientRegistry
from fleetpull.network.client.registry_base import ProviderResourceRegistry
from fleetpull.network.client.runtime import ClientRuntime
from fleetpull.network.client.transport import TransportClient

__all__: list[str] = [
    'ClientRuntime',
    'FetchedPage',
    'ProviderClientRegistry',
    'ProviderProfile',
    'ProviderResourceRegistry',
    'TransportClient',
]
