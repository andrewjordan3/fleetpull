# src/fleetpull/config/logger.py
"""Logging configuration model.

LoggerConfig is one section of fleetpull's user-provided YAML
configuration. Like every model in fleetpull.config, it validates user
input at the boundary; consuming code (fleetpull.logger.setup) receives
only validated, normalized values.

Level fields accept standard level names case-insensitively ('debug',
'INFO') or the standard integer levels (10, 20, 30, 40, 50) and are
normalized to integers at validation time. Booleans, nonstandard
integers, deprecated aliases (WARN, FATAL), and NOTSET are rejected.
``file_path`` normalizes through ``paths.resolve_path`` at validation
time, like every path field in the config layer.
"""

import logging
from pathlib import Path
from typing import Any

from pydantic import field_validator

from fleetpull.config.base import ConfigModel
from fleetpull.paths import resolve_path

__all__: list[str] = ['LoggerConfig']

# Canonical mapping from level name to Python logging integer. The name
# set is fixed to the five standard levels on purpose: the deprecated
# aliases (WARN, FATAL) and NOTSET (level 0, "inherit from parent") are
# rejected so a YAML file reads the same as the logging documentation.
# The integer values are pulled from the stdlib rather than hardcoded.
_LEVEL_NAME_TO_INT: dict[str, int] = {
    level_name: getattr(logging, level_name)
    for level_name in ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
}

# Used to validate integer input and to render error messages that show
# both accepted forms.
_ALLOWED_LEVEL_INTS: frozenset[int] = frozenset(_LEVEL_NAME_TO_INT.values())


# typing-justified: mode='before' validator input is contractually arbitrary
def _coerce_log_level(raw_value: Any) -> int:
    # ``Any``: receives the raw pre-validation value from a Pydantic
    # ``mode='before'`` validator, which is contractually arbitrary —
    # any YAML scalar the user might write. The isinstance chain below
    # enumerates every accepted shape and rejects the rest.
    """
    Convert a user-supplied log level (name or int) to a logging integer.

    Accepts:
        - A ``str`` naming one of DEBUG / INFO / WARNING / ERROR /
          CRITICAL (case-insensitive, surrounding whitespace stripped).
        - An ``int`` equal to one of the standard levels (10, 20, 30,
          40, 50).

    Args:
        raw_value: The value supplied by the user.

    Returns:
        The Python logging integer corresponding to ``raw_value``.

    Raises:
        ValueError: If ``raw_value`` is a bool, a nonstandard integer,
            an unrecognized name, or any other type. ``ValueError``
            (rather than ``TypeError``) is used throughout so Pydantic
            wraps the failure into a ``ValidationError`` attributed to
            the offending field.
    """
    if isinstance(raw_value, bool):
        # bool is a subclass of int; reject explicitly so True does not
        # quietly become log level 1.
        raise ValueError('log level must be a level name or integer, got bool')

    if isinstance(raw_value, int):
        if raw_value not in _ALLOWED_LEVEL_INTS:
            allowed_pairs: str = ', '.join(
                f'{name}={value}' for name, value in _LEVEL_NAME_TO_INT.items()
            )
            raise ValueError(
                f'integer {raw_value} is not a standard log level; '
                f'allowed: {allowed_pairs}'
            )
        return raw_value

    if isinstance(raw_value, str):
        normalized_name: str = raw_value.strip().upper()
        if normalized_name not in _LEVEL_NAME_TO_INT:
            allowed_names: str = ', '.join(_LEVEL_NAME_TO_INT)
            raise ValueError(
                f'{raw_value!r} is not a recognized log level; '
                f'allowed: {allowed_names} (case-insensitive)'
            )
        return _LEVEL_NAME_TO_INT[normalized_name]

    raise ValueError(
        f'log level must be a level name or integer, got {type(raw_value).__name__}'
    )


class LoggerConfig(ConfigModel):
    """
    User-facing logging configuration.

    Attributes:
        console_level: Minimum level for stderr console output. Accepts
            a standard level name or integer; normalized to int.
            Defaults to INFO.
        file_path: Path to a log file, normalized through
            ``paths.resolve_path`` at validation. When None (the
            default), file logging is disabled and ``file_level`` is
            inert.
        file_level: Minimum level for file output. Required with a
            default of DEBUG so there is never a None to narrow at the
            use site; it simply has no effect while ``file_path`` is
            None.
    """

    console_level: int = logging.INFO
    file_path: Path | None = None
    file_level: int = logging.DEBUG

    @field_validator('console_level', 'file_level', mode='before')
    @classmethod
    # typing-justified: mode='before' validator input is contractually arbitrary
    def _coerce_level(cls, raw_value: Any) -> int:
        # ``Any``: mode='before' validators receive arbitrary
        # pre-validation input; _coerce_log_level enumerates the
        # accepted shapes. No field label is threaded through —
        # Pydantic's ValidationError attributes the failure to the
        # correct field via its loc.
        return _coerce_log_level(raw_value)

    @field_validator('file_path')
    @classmethod
    def _resolve(cls, value: Path | None) -> Path | None:
        """Normalize the path lexically when present; ``None`` passes through."""
        return None if value is None else resolve_path(value)
