# src/fleetpull/config/loading.py
"""The single-concern loading steps behind ``FleetpullConfig.from_yaml``.

Reading and parsing the file, merging conventional credential
environment variables into the raw document, shaping validation failures
into actionable messages, and the post-validation disabled-provider
warning. Environment access lives here and nowhere else in the config
layer; none of it runs inside validators.

Every failure surfaces as a ``ConfigurationError`` a user can act on: a
missing file names the path; a parse failure names the line the parser
reports, composed from the parser's ``problem`` text and never the
marked source snippet (a snippet could echo a malformed credential
line); a validation failure carries one ``key.path: message`` entry per
error and no raw input values.
"""

import logging
import os
from pathlib import Path

import yaml
from pydantic import SecretStr, ValidationError

from fleetpull.config.providers import PROVIDER_CREDENTIAL_ENV_VARS, ProvidersConfig
from fleetpull.exceptions import ConfigurationError

__all__: list[str] = [
    'read_yaml_document',
    'validation_detail',
    'warn_disabled_providers',
    'with_environment_credentials',
]

logger = logging.getLogger(__name__)


# typing-justified: YAML values are arbitrary user input until validated
def read_yaml_document(config_path: Path) -> dict[str, object]:
    """Read and parse the file into the raw top-level mapping.

    Args:
        config_path: The file to read.

    Returns:
        The parsed top-level mapping; an empty file is an empty mapping
        (validation then names the missing required sections).

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


# typing-justified: rewrites the raw document before validation
def with_environment_credentials(document: dict[str, object]) -> dict[str, object]:
    """Merge conventional credential environment variables into the document.

    Applied per provider from ``PROVIDER_CREDENTIAL_ENV_VARS``, only when
    the credential is absent from the YAML (a YAML literal wins) and the
    variable carries a non-empty value (empty counts as unset). The shape
    is per-provider (the mapping's documented asymmetry): Motive's and
    Samsara's variables each supply the whole credential (``api_key``);
    GeoTab's fills only the ``password`` field of a YAML-present ``auth``
    section --
    username, database, and server are not secrets and always come from
    the YAML, so an absent ``auth`` section is left for the enablement
    guard to reject. The value is wrapped in ``SecretStr`` here, so the
    raw string never travels in the document.

    Args:
        document: The raw document mapping.

    Returns:
        The document with resolvable credentials merged; otherwise
        unchanged.

    Side Effects:
        Reads the process environment -- the only place in the config
        layer that does.
    """
    providers_section = document.get('providers')
    if not isinstance(providers_section, dict):
        return document
    merged_providers = dict(providers_section)
    motive_section = merged_providers.get('motive')
    if isinstance(motive_section, dict) and 'api_key' not in motive_section:
        motive_value = os.environ.get(PROVIDER_CREDENTIAL_ENV_VARS['motive'])
        if motive_value:
            merged_providers['motive'] = {
                **motive_section,
                'api_key': SecretStr(motive_value),
            }
    geotab_section = merged_providers.get('geotab')
    if isinstance(geotab_section, dict):
        auth_section = geotab_section.get('auth')
        if isinstance(auth_section, dict) and 'password' not in auth_section:
            geotab_value = os.environ.get(PROVIDER_CREDENTIAL_ENV_VARS['geotab'])
            if geotab_value:
                merged_providers['geotab'] = {
                    **geotab_section,
                    'auth': {**auth_section, 'password': SecretStr(geotab_value)},
                }
    samsara_section = merged_providers.get('samsara')
    if isinstance(samsara_section, dict) and 'api_key' not in samsara_section:
        samsara_value = os.environ.get(PROVIDER_CREDENTIAL_ENV_VARS['samsara'])
        if samsara_value:
            merged_providers['samsara'] = {
                **samsara_section,
                'api_key': SecretStr(samsara_value),
            }
    return {**document, 'providers': merged_providers}


def validation_detail(validation_error: ValidationError) -> str:
    """Summarize validation errors as ``key.path: message`` entries.

    Uses each error's location and message only -- never the offending
    input value, which could be a credential.
    """
    return '; '.join(
        f'{".".join(str(item) for item in entry["loc"])}: {entry["msg"]}'
        for entry in validation_error.errors()
    )


def warn_disabled_providers(providers: ProvidersConfig) -> None:
    """Log one WARNING per provider with a credential but no endpoints.

    The provider is merely disabled, not misconfigured, so this is a
    post-validation side effect of loading -- never a validator's job.

    Args:
        providers: The validated providers container.

    Side Effects:
        Logs through this module's logger.
    """
    motive = providers.motive
    if motive is not None and motive.api_key is not None and not motive.endpoints:
        logger.warning(
            "providers.motive: a credential resolves but 'endpoints' is empty; "
            'the provider is disabled for this run.'
        )
    geotab = providers.geotab
    if geotab is not None and geotab.auth is not None and not geotab.endpoints:
        logger.warning(
            "providers.geotab: a credential resolves but 'endpoints' is empty; "
            'the provider is disabled for this run.'
        )
