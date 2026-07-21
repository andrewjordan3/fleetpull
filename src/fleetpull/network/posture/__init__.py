# src/fleetpull/network/posture/__init__.py
"""Transport posture: the one ``HttpConfig`` -> httpx client construction."""

from fleetpull.network.posture.client_options import new_http_client

__all__: list[str] = ['new_http_client']
