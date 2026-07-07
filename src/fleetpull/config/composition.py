# src/fleetpull/config/composition.py
"""Cross-section composition: the defaults no single section model can know.

The section models validate shape; this module resolves the settled
load-time semantics that span sections (DESIGN section 10): the state
database path and log file path default against ``storage.dataset_root``,
provider credentials fall back to the conventional environment variable,
the package-wide window knobs fan into every enabled provider's config,
and the enablement rules fire (endpoints without a credential raise; a
credential without endpoints warns and the provider stays disabled).
Consumed only by ``config/loader.py``, inside ``load_config``.
"""

import logging
import os
from collections.abc import Mapping
from pathlib import Path

from pydantic import SecretStr

from fleetpull.config.logger import LoggerConfig
from fleetpull.config.motive import MotiveConfig
from fleetpull.config.providers import ProvidersConfig
from fleetpull.config.root import FleetpullConfig
from fleetpull.config.state import StateConfig
from fleetpull.config.sync import SyncConfig
from fleetpull.exceptions import ConfigurationError

__all__: list[str] = ['composed_config']

logger = logging.getLogger(__name__)

# The conventional credential fallback -- the same name the snapshot
# script has always read.
_MOTIVE_API_KEY_ENV: str = 'MOTIVE_API_KEY'

# The internal directory convention under dataset_root (DESIGN section 5).
_INTERNAL_SUBDIRECTORY: str = '.fleetpull'
_STATE_FILENAME: str = 'state.sqlite3'
_LOG_FILENAME: str = 'fleetpull.log'


def composed_config(
    parsed: FleetpullConfig,
    # typing-justified: raw YAML values are arbitrary until validated
    raw_logging_section: Mapping[str, object] | None,
) -> FleetpullConfig:
    """Resolve every cross-section default into the returned config.

    Args:
        parsed: The shape-validated root config.
        raw_logging_section: The raw ``logging:`` mapping (or ``None``),
            read for key *presence* -- a validated ``file_level`` is
            indistinguishable from the model default, so enablement of
            file logging must be decided against the raw section.

    Returns:
        The effective config: state and logging defaults resolved against
        ``storage.dataset_root``, provider credentials resolved, window
        knobs fanned in.

    Raises:
        ConfigurationError: A provider lists endpoints but no credential
            resolves.

    Side Effects:
        Reads the process environment; logs one WARNING per provider
        whose credential resolves while its endpoint list is empty.
    """
    internal_directory = parsed.storage.dataset_root / _INTERNAL_SUBDIRECTORY
    state = StateConfig(
        database_path=parsed.state.database_path or internal_directory / _STATE_FILENAME
    )
    return FleetpullConfig(
        sync=parsed.sync,
        storage=parsed.storage,
        state=state,
        logging=_composed_logging(
            parsed.logging, raw_logging_section, internal_directory
        ),
        http=parsed.http,
        retry=parsed.retry,
        providers=_composed_providers(parsed.providers, parsed.sync),
    )


def _composed_logging(
    validated: LoggerConfig,
    # typing-justified: raw YAML values are arbitrary until validated
    raw_logging_section: Mapping[str, object] | None,
    internal_directory: Path,
) -> LoggerConfig:
    """Default the missing file-logging partner when either file key is set.

    Either file key present enables file logging; the missing partner is
    defaulted -- level to the model's DEBUG, path to
    ``<dataset_root>/.fleetpull/fleetpull.log``. Neither present leaves
    ``file_path`` ``None`` and file logging disabled.
    """
    if raw_logging_section is None:
        return validated
    if 'file_path' not in raw_logging_section and (
        'file_level' not in raw_logging_section
    ):
        return validated
    return LoggerConfig(
        console_level=validated.console_level,
        file_path=validated.file_path or internal_directory / _LOG_FILENAME,
        file_level=validated.file_level,
    )


def _composed_providers(
    providers: ProvidersConfig, sync: SyncConfig
) -> ProvidersConfig:
    """Compose every present provider section; absent providers pass through."""
    if providers.motive is None:
        return providers
    return ProvidersConfig(motive=_composed_motive(providers.motive, sync))


def _composed_motive(motive: MotiveConfig, sync: SyncConfig) -> MotiveConfig:
    """Resolve the credential, enforce enablement, fan in the window knobs.

    Enablement: enabled iff a credential resolves (YAML literal winning
    over the environment fallback) AND ``endpoints`` is non-empty.

    Raises:
        ConfigurationError: Endpoints are listed but no credential
            resolves, naming both the YAML field and the environment
            variable -- never the credential value.

    Side Effects:
        Logs the credential-without-endpoints WARNING.
    """
    api_key = motive.api_key or _environment_api_key()
    if motive.endpoints and api_key is None:
        raise ConfigurationError(
            'provider credential missing',
            provider='motive',
            detail=(
                'endpoints are configured but no credential resolves; set '
                f"'providers.motive.api_key' in the YAML or the "
                f'{_MOTIVE_API_KEY_ENV} environment variable'
            ),
        )
    if api_key is not None and not motive.endpoints:
        logger.warning(
            "providers.motive: a credential resolves but 'endpoints' is empty; "
            'the provider is disabled for this run.'
        )
    enabled = api_key is not None and bool(motive.endpoints)
    updates: dict[str, SecretStr | int | None] = {'api_key': api_key}
    if enabled and sync.lookback_days is not None:
        updates['lookback_days'] = sync.lookback_days
    if enabled and sync.cutoff_days is not None:
        updates['cutoff_days'] = sync.cutoff_days
    # model_copy skips validation; every value here was validated upstream
    # (the SecretStr by the model or wrapped fresh, the ints ge=0 on SyncConfig).
    return motive.model_copy(update=updates)


def _environment_api_key() -> SecretStr | None:
    """The ``MOTIVE_API_KEY`` environment fallback; empty counts as unset."""
    environment_value = os.environ.get(_MOTIVE_API_KEY_ENV)
    return SecretStr(environment_value) if environment_value else None
