"""The transport client face: the assembled HTTP fetch loop and its inputs.

``TransportClient`` runs the per-attempt pipeline and page loop against a
per-provider ``ProviderProfile`` (auth + classifier) and a process-global
``ClientRuntime`` (configs, limiter registry, jitter, sleeper), emitting
``FetchedPage`` objects. External callers import these four names here."""

from fleetpull.network.client.page import FetchedPage
from fleetpull.network.client.profile import ProviderProfile
from fleetpull.network.client.runtime import ClientRuntime
from fleetpull.network.client.transport import TransportClient

__all__: list[str] = [
    'ClientRuntime',
    'FetchedPage',
    'ProviderProfile',
    'TransportClient',
]
