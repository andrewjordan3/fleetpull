# src/fleetpull/config/resolution.py
"""Pure cross-section resolution over raw YAML documents.

The single-concern functions behind ``FleetpullConfig``'s
``mode='before'`` validators (house style: validator bodies delegate to
functions). Each takes a raw document mapping and returns one -- no
environment access, no I/O, no logging -- and each acts only where the
relevant sections are raw mappings with the relevant keys present:
already-constructed section models and malformed sections pass through
unchanged, so shape errors surface from validation with their real key
paths.

The three settled rules (DESIGN section 10):
    - Window-knob precedence per provider: a provider's own
      ``lookback_days`` / ``cutoff_days`` key stands; else a declared
      ``sync`` value fans in; else the provider model's default
      validates in naturally.
    - ``state.database_path`` defaults to
      ``<dataset_root>/.fleetpull/state.sqlite3`` when absent.
    - Either logging file key enables file logging: ``file_level``
      without ``file_path`` injects the default path
      (``<dataset_root>/.fleetpull/fleetpull.log``); ``file_path`` alone
      already works via the model's DEBUG default.
"""

from collections.abc import Mapping
from pathlib import Path

__all__: list[str] = [
    'with_log_path_defaulted',
    'with_provider_knobs_applied',
    'with_state_path_defaulted',
]

# The internal directory convention under dataset_root (DESIGN section 5).
_INTERNAL_SUBDIRECTORY: str = '.fleetpull'
_STATE_FILENAME: str = 'state.sqlite3'
_LOG_FILENAME: str = 'fleetpull.log'

# The knobs the sync section can declare package-wide.
_WINDOW_KNOB_KEYS: tuple[str, ...] = ('lookback_days', 'cutoff_days')


# typing-justified: raw YAML documents are arbitrary until validated
def with_provider_knobs_applied(document: Mapping[str, object]) -> dict[str, object]:
    """Fan declared ``sync`` window knobs into providers lacking their own key.

    Args:
        document: The raw document mapping.

    Returns:
        The document with each provider mapping gaining any ``sync``-declared
        knob it does not set itself; everything else unchanged. A provider key
        always wins over the ``sync`` value (proven by the precedence tests).
    """
    sync_section = document.get('sync')
    providers_section = document.get('providers')
    if not isinstance(sync_section, Mapping) or not isinstance(
        providers_section, Mapping
    ):
        return dict(document)
    declared = {
        key: sync_section[key] for key in _WINDOW_KNOB_KEYS if key in sync_section
    }
    if not declared:
        return dict(document)
    # typing-justified: raw provider entries, arbitrary until validated
    fanned_providers: dict[object, object] = {}
    for provider_name, provider_section in providers_section.items():
        # typing-justified: a raw provider section, arbitrary until validated
        fanned_section: object = provider_section
        if isinstance(provider_section, Mapping):
            missing = {
                key: value
                for key, value in declared.items()
                if key not in provider_section
            }
            if missing:
                fanned_section = {**provider_section, **missing}
        fanned_providers[provider_name] = fanned_section
    return {**document, 'providers': fanned_providers}


# typing-justified: raw YAML documents are arbitrary until validated
def with_state_path_defaulted(document: Mapping[str, object]) -> dict[str, object]:
    """Default ``state.database_path`` under ``dataset_root`` when absent.

    Args:
        document: The raw document mapping.

    Returns:
        The document with ``state.database_path`` set to the DESIGN
        section 5 convention when the key is absent and ``dataset_root``
        is readable; otherwise unchanged.
    """
    dataset_root = _raw_dataset_root(document)
    if dataset_root is None:
        return dict(document)
    state_section = document.get('state', {})
    if not isinstance(state_section, Mapping) or 'database_path' in state_section:
        return dict(document)
    defaulted = {
        **state_section,
        'database_path': dataset_root / _INTERNAL_SUBDIRECTORY / _STATE_FILENAME,
    }
    return {**document, 'state': defaulted}


# typing-justified: raw YAML documents are arbitrary until validated
def with_log_path_defaulted(document: Mapping[str, object]) -> dict[str, object]:
    """Inject the default log path when ``file_level`` is set without a path.

    Args:
        document: The raw document mapping.

    Returns:
        The document with ``logging.file_path`` defaulted when
        ``file_level`` is present without it and ``dataset_root`` is
        readable; otherwise unchanged. Neither file key present means
        file logging stays off; ``file_path`` alone needs no help (the
        model's DEBUG default covers the level).
    """
    logging_section = document.get('logging')
    if not isinstance(logging_section, Mapping):
        return dict(document)
    if 'file_level' not in logging_section or 'file_path' in logging_section:
        return dict(document)
    dataset_root = _raw_dataset_root(document)
    if dataset_root is None:
        return dict(document)
    defaulted = {
        **logging_section,
        'file_path': dataset_root / _INTERNAL_SUBDIRECTORY / _LOG_FILENAME,
    }
    return {**document, 'logging': defaulted}


# typing-justified: reads a raw YAML value, arbitrary until validated
def _raw_dataset_root(document: Mapping[str, object]) -> Path | None:
    """The raw ``storage.dataset_root`` as a Path, or None when unreadable.

    Unreadable (missing section, missing key, non-path type) returns
    ``None`` so the caller passes the document through and validation
    names the real problem at its real key path. The value may still be
    unnormalized here; the derived defaults normalize with everything
    else at field validation (``resolve_path``).
    """
    storage_section = document.get('storage')
    if not isinstance(storage_section, Mapping):
        return None
    dataset_root = storage_section.get('dataset_root')
    if isinstance(dataset_root, str) and dataset_root.strip():
        return Path(dataset_root)
    if isinstance(dataset_root, Path):
        return dataset_root
    return None
