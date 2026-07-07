# src/fleetpull/config/loader.py
"""The YAML config loader: one file in, one composed ``FleetpullConfig`` out.

Validates structure only -- shapes, required keys, the YAML surface --
and hands the validated document to ``config/composition.py`` for the
cross-section defaults, the credential fallback, and the enablement
rules. Endpoint names stay unvalidated strings here by design: the
catalog lives in the ``api`` tier above ``config``, so name validation
happens at ``Sync`` construction, never in this package.

Every failure is a ``ConfigurationError`` a user can act on: a missing
file names the path, a parse failure names the line the parser reports,
a validation failure names the offending key path, and a masked
runtime-only key names where the setting actually lives.
"""

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

import yaml
from pydantic import ValidationError

from fleetpull.config.composition import composed_config
from fleetpull.config.root import FleetpullConfig
from fleetpull.exceptions import ConfigurationError

__all__: list[str] = ['load_config']

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class _MaskedKeySet:
    """Schema-excluded keys of one section, with the user-facing redirect.

    These fields exist on the section models for composition's sake, so
    ``extra='forbid'`` cannot reject them; the schema (DESIGN section 10)
    still does not admit them as YAML keys.

    Attributes:
        section_path: The section the mask applies to, as nested keys.
        masked_keys: The keys rejected inside that section.
        hint: Where the setting actually lives, for the error message.
    """

    section_path: tuple[str, ...]
    masked_keys: frozenset[str]
    hint: str


_MASKED_KEY_SETS: tuple[_MaskedKeySet, ...] = (
    _MaskedKeySet(
        section_path=('sync',),
        masked_keys=frozenset({'dataset_root'}),
        hint="dataset_root is authored under 'storage:'",
    ),
    _MaskedKeySet(
        section_path=('providers', 'motive'),
        masked_keys=frozenset({'lookback_days', 'cutoff_days'}),
        hint=(
            'window knobs are package-wide in this cut: use '
            "'sync.lookback_days' / 'sync.cutoff_days'"
        ),
    ),
)


def load_config(path: Path | str) -> FleetpullConfig:
    """Load, validate, and compose one fleetpull YAML configuration file.

    Args:
        path: The configuration file to read; a string coerces to a path.

    Returns:
        The validated root config with every cross-section default
        resolved: the state database path and log file path against
        ``storage.dataset_root``, provider credentials against the
        environment fallback, and the package-wide window knobs fanned
        into every enabled provider's config.

    Raises:
        ConfigurationError: The file is missing (naming the path), is not
            valid YAML (naming the line), violates the schema (naming the
            offending key path), places a runtime-only key in the YAML,
            or lists endpoints for a provider with no resolvable
            credential (naming the YAML field and environment variable).

    Side Effects:
        Reads the file and the process environment; logs one WARNING for
        a provider whose credential resolves but whose endpoint list is
        empty.
    """
    config_path = Path(path)
    raw_document = _read_yaml_document(config_path)
    _reject_masked_keys(raw_document)
    parsed = _validated_root(_with_dataset_root_injected(raw_document))
    return composed_config(parsed, _section_at(raw_document, ('logging',)))


# typing-justified: YAML values are arbitrary user input until validated
def _read_yaml_document(config_path: Path) -> dict[str, object]:
    """Read and parse the file into the raw top-level mapping.

    Args:
        config_path: The file to read.

    Returns:
        The parsed top-level mapping; an empty file is an empty mapping
        (root validation then names the missing required sections).

    Raises:
        ConfigurationError: The file is missing, unparseable (naming the
            line the parser reports), or not a mapping at the top level.
    """
    if not config_path.is_file():
        raise ConfigurationError('config file not found', detail=str(config_path))
    try:
        loaded = yaml.safe_load(config_path.read_text(encoding='utf-8'))
    except yaml.YAMLError as parse_error:
        raise ConfigurationError(
            'config file is not valid YAML',
            detail=_parse_error_detail(config_path, parse_error),
        ) from None
    if loaded is None:
        return {}
    if not isinstance(loaded, dict):
        raise ConfigurationError(
            'config file must be a YAML mapping at the top level',
            detail=f'{config_path}: got {type(loaded).__name__}',
        )
    return loaded


def _parse_error_detail(config_path: Path, parse_error: yaml.YAMLError) -> str:
    """Locate a parse failure at the line the parser reports.

    Composes from the parser's ``problem`` description, never the marked
    source snippet -- a snippet could echo a malformed credential line.
    """
    problem: str = getattr(parse_error, 'problem', None) or type(parse_error).__name__
    mark = getattr(parse_error, 'problem_mark', None)
    if isinstance(mark, yaml.Mark):
        return f'{config_path}, line {mark.line + 1}: {problem}'
    return f'{config_path}: {problem}'


def _section_at(
    # typing-justified: raw YAML sections are arbitrary until validated
    raw_document: Mapping[str, object],
    section_path: tuple[str, ...],
) -> Mapping[str, object] | None:  # typing-justified: a raw subsection, or None
    """The raw mapping at a nested path, or None when absent or not a mapping."""
    current: object = raw_document  # typing-justified: narrowed at each step
    for key in section_path:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current if isinstance(current, Mapping) else None


# typing-justified: operates on the raw document before validation
def _reject_masked_keys(raw_document: Mapping[str, object]) -> None:
    """Reject schema-excluded keys that ``extra='forbid'`` cannot catch.

    Raises:
        ConfigurationError: A masked key appears, naming it and where the
            setting actually lives.
    """
    for masked in _MASKED_KEY_SETS:
        section = _section_at(raw_document, masked.section_path)
        if section is None:
            continue
        for key in sorted(masked.masked_keys & section.keys()):
            dotted_key = '.'.join((*masked.section_path, key))
            raise ConfigurationError(
                'unknown configuration key',
                detail=f'{dotted_key!r} is not a YAML key: {masked.hint}',
            )


# typing-justified: rewrites the raw document before validation
def _with_dataset_root_injected(raw_document: dict[str, object]) -> dict[str, object]:
    """Feed ``storage.dataset_root`` into the ``sync`` section.

    ``SyncConfig`` carries ``dataset_root`` at runtime (the runner reads
    it there) while its YAML home is the ``storage`` section; the loader
    bridges the two. A user-supplied ``sync.dataset_root`` was already
    rejected by ``_reject_masked_keys``. When either section is missing
    or malformed, the document passes through unchanged and root
    validation names the real problem.
    """
    sync_section = raw_document.get('sync')
    storage_section = raw_document.get('storage')
    if not (isinstance(sync_section, Mapping) and isinstance(storage_section, Mapping)):
        return raw_document
    if 'dataset_root' not in storage_section:
        return raw_document
    injected_sync = {**sync_section, 'dataset_root': storage_section['dataset_root']}
    return {**raw_document, 'sync': injected_sync}


# typing-justified: validates the raw document; arbitrary until this call
def _validated_root(document: dict[str, object]) -> FleetpullConfig:
    """Validate the whole document against the root model.

    Raises:
        ConfigurationError: Any schema violation, with one
            ``key.path: message`` entry per failure and no raw input
            values (a value could be a credential).
    """
    try:
        return FleetpullConfig.model_validate(document)
    except ValidationError as validation_error:
        raise ConfigurationError(
            'invalid configuration', detail=_validation_detail(validation_error)
        ) from None


def _validation_detail(validation_error: ValidationError) -> str:
    """Summarize validation errors as ``key.path: message`` entries.

    The ``sync.dataset_root`` location is loader-injected, never a user
    key, so its absence is always ``storage.dataset_root``'s own (already
    reported) failure; it is dropped rather than shown as a key the user
    never wrote.
    """
    located_messages = [
        ('.'.join(str(item) for item in entry['loc']), entry['msg'])
        for entry in validation_error.errors()
    ]
    filtered = [
        (location, message)
        for location, message in located_messages
        if location != 'sync.dataset_root'
    ]
    return '; '.join(
        f'{location}: {message}' for location, message in (filtered or located_messages)
    )
