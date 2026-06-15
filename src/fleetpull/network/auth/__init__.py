"""GeoTab session lifecycle: single-flight authentication state."""

from fleetpull.network.auth.authenticate import build_geotab_authenticator
from fleetpull.network.auth.manager import GeotabSessionManager
from fleetpull.network.auth.models import AuthenticationResult, GeotabSession
from fleetpull.network.auth.strategies import GeotabSessionAuth, StaticHeaderAuth

__all__: list[str] = [
    'AuthenticationResult',
    'GeotabSession',
    'GeotabSessionAuth',
    'GeotabSessionManager',
    'StaticHeaderAuth',
    'build_geotab_authenticator',
]
