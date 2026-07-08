# src/fleetpull/config/root.py
"""The whole-document configuration root: ``FleetpullConfig`` and ``from_yaml``.

The one model family that IS the schema (DESIGN section 10 -- the YAML
schema is sync's API): the root composes the section models exactly as
the YAML reads, and cross-section resolution runs as ``mode='before'``
validation on the root, where key presence is still visible. Each
validator body is a thin wrapper over a single-concern pure function in
``config/resolution.py``; the loading steps behind ``from_yaml`` live in
``config/loading.py``. No masks, no injections, no post-validation
rewriting -- the sections and the schema agree, so nothing needs
compensating for.

The invariant, precisely: any ``FleetpullConfig`` validated from a raw
document is fully resolved -- every path field normalized through
``paths.resolve_path``, ``state.database_path`` and the log-file default
composed against ``storage.dataset_root``, and every provider's window
knobs settled by precedence (provider key > ``sync`` key > provider
default). Direct construction from already-built section models skips
raw-document resolution but never the enablement invariant: endpoints
listed with no credential raise ``ConfigurationError`` at validation
either way.
"""

from collections.abc import Mapping
from pathlib import Path
from typing import Self

from pydantic import Field, ValidationError, model_validator

from fleetpull.config.base import ConfigModel
from fleetpull.config.http import HttpConfig
from fleetpull.config.loading import (
    read_yaml_document,
    validation_detail,
    warn_disabled_providers,
    with_environment_credentials,
)
from fleetpull.config.logger import LoggerConfig
from fleetpull.config.providers import ProvidersConfig, require_provider_credentials
from fleetpull.config.resolution import (
    with_log_path_defaulted,
    with_provider_knobs_applied,
    with_state_path_defaulted,
)
from fleetpull.config.retry import RetryConfig
from fleetpull.config.sections import StateConfig, StorageConfig, SyncConfig
from fleetpull.exceptions import ConfigurationError

__all__: list[str] = ['FleetpullConfig']


class FleetpullConfig(ConfigModel):
    """
    The validated whole-document configuration, one instance per run.

    Attributes:
        sync: Sync-wide settings: the cold-start anchor and the optional
            package-wide window knobs.
        storage: Where the parquet dataset lives (``dataset_root``'s one
            and only home).
        state: Where operational SQLite state lives; defaulted under
            ``dataset_root`` by raw-document resolution.
        logging: Console and file logging; the file-path default composes
            under ``dataset_root`` when only ``file_level`` is given.
        http: Transport timeouts and TLS posture.
        retry: Attempt budgets and backoff shape.
        providers: The per-provider sections; a provider listing
            endpoints without a credential fails validation.
    """

    sync: SyncConfig
    storage: StorageConfig
    state: StateConfig = Field(default_factory=StateConfig)
    logging: LoggerConfig = Field(default_factory=LoggerConfig)
    http: HttpConfig = Field(default_factory=HttpConfig)
    retry: RetryConfig = Field(default_factory=RetryConfig)
    providers: ProvidersConfig

    @model_validator(mode='before')
    @classmethod
    # typing-justified: mode='before' input is the arbitrary raw document
    def _apply_provider_knob_precedence(cls, document: object) -> object:
        """Fan declared ``sync`` knobs into providers lacking their own key."""
        if not isinstance(document, Mapping):
            return document
        return with_provider_knobs_applied(document)

    @model_validator(mode='before')
    @classmethod
    # typing-justified: mode='before' input is the arbitrary raw document
    def _apply_state_path_default(cls, document: object) -> object:
        """Default ``state.database_path`` under ``dataset_root``."""
        if not isinstance(document, Mapping):
            return document
        return with_state_path_defaulted(document)

    @model_validator(mode='before')
    @classmethod
    # typing-justified: mode='before' input is the arbitrary raw document
    def _apply_log_path_default(cls, document: object) -> object:
        """Inject the default log path when ``file_level`` is set alone."""
        if not isinstance(document, Mapping):
            return document
        return with_log_path_defaulted(document)

    @model_validator(mode='after')
    def _require_credentialed_providers(self) -> Self:
        """Enforce enablement's credential half; see the providers family.

        Raises:
            ConfigurationError: A provider lists endpoints with no
                credential. Raised (not a ``ValueError``) so it reaches
                the caller as itself -- Pydantic wraps only
                ``ValueError``/``AssertionError`` into validation errors.
        """
        require_provider_credentials(self.providers)
        return self

    @classmethod
    def from_yaml(cls, path: Path | str) -> Self:
        """Load, validate, and resolve one fleetpull YAML configuration file.

        The loading API: read the file, merge conventional credential
        environment variables (a YAML literal wins; empty counts as
        unset), validate -- which resolves every cross-section default --
        and warn about credentialed providers with no endpoints.

        Args:
            path: The configuration file to read; a string coerces.

        Returns:
            The fully resolved configuration (the class docstring's
            invariant).

        Raises:
            ConfigurationError: The file is missing (naming the path), is
                not valid YAML (naming the line), violates the schema
                (naming each offending key path, never a raw value), or
                lists endpoints for a provider with no resolvable
                credential (naming the YAML field and the environment
                variable).

        Side Effects:
            Reads the file and the process environment; logs one WARNING
            per credentialed provider whose endpoint list is empty.
        """
        document = read_yaml_document(Path(path))
        merged = with_environment_credentials(document)
        try:
            config = cls.model_validate(merged)
        except ValidationError as validation_error:
            raise ConfigurationError(
                'invalid configuration', detail=validation_detail(validation_error)
            ) from None
        warn_disabled_providers(config.providers)
        return config
