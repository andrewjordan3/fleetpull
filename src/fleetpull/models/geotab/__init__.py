# src/fleetpull/models/geotab/__init__.py
"""GeoTab response models; the face re-exports each endpoint module's models."""

from fleetpull.models.geotab.device import Device

__all__: list[str] = ['Device']
