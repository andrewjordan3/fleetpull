# src/fleetpull/config/root.py
"""The whole-document configuration root: ``FleetpullConfig``.

Composes the section models into the one shape a YAML config file must
take (DESIGN section 10 -- the schema IS sync's API). ``sync``,
``storage``, and ``providers`` are required; every other section is
optional wholesale, defaulting to its model's own defaults.

Two composition facts live in the loader, not here: ``sync.dataset_root``
is populated from the ``storage`` section (a user-supplied
``sync.dataset_root`` key is rejected), and the cross-section defaults
(state database path, log file path) resolve against
``storage.dataset_root`` at load time. This model only states the shape.
"""

from pydantic import BaseModel, ConfigDict, Field

from fleetpull.config.http import HttpConfig
from fleetpull.config.logger import LoggerConfig
from fleetpull.config.providers import ProvidersConfig
from fleetpull.config.retry import RetryConfig
from fleetpull.config.state import StateConfig
from fleetpull.config.storage import StorageConfig
from fleetpull.config.sync import SyncConfig

__all__: list[str] = ['FleetpullConfig']


class FleetpullConfig(BaseModel):
    """
    The validated whole-document configuration, one instance per run.

    Attributes:
        sync: Sync-wide settings (the cold-start anchor, the package-wide
            window knobs, and -- fed from ``storage`` by the loader --
            the dataset root).
        storage: Where the parquet dataset lives.
        state: Where operational SQLite state lives; the loader defaults
            its path from ``storage.dataset_root``.
        logging: Console and file logging levels and the log file path;
            the loader defaults the file path from
            ``storage.dataset_root`` when only ``file_level`` is given.
        http: Transport timeouts and TLS posture.
        retry: Attempt budgets and backoff shape.
        providers: The per-provider sections; enablement (credential
            AND endpoints) is checked by the loader.
    """

    model_config = ConfigDict(frozen=True, extra='forbid', validate_default=True)

    sync: SyncConfig
    storage: StorageConfig
    state: StateConfig = Field(default_factory=StateConfig)
    logging: LoggerConfig = Field(default_factory=LoggerConfig)
    http: HttpConfig = Field(default_factory=HttpConfig)
    retry: RetryConfig = Field(default_factory=RetryConfig)
    providers: ProvidersConfig
