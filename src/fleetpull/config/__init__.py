"""Pydantic models for user-provided YAML configuration, one module per section."""

from fleetpull.config.geotab import GeotabAuthConfig
from fleetpull.config.logger import LoggerConfig

__all__: list[str] = ['GeotabAuthConfig', 'LoggerConfig']
