# src/fleetpull/config/logger.py
"""
Logging configuration model.

LoggerConfig is one section of fleetpull's user-provided YAML
configuration. Like every model in fleetpull.config, it validates user
input at the boundary; consuming code (fleetpull.logger.setup) receives
only validated, normalized values.

Level fields accept either stdlib integer levels (e.g. ``logging.INFO``)
or standard level names case-insensitively (e.g. ``'info'``, ``'DEBUG'``)
— YAML authors write names, programmatic callers may pass either. Values
are normalized to integers at validation time.
"""

import logging
from pathlib import Path

from pydantic import BaseModel, ConfigDict, field_validator

__all__: list[str] = ['LoggerConfig']


class LoggerConfig(BaseModel):
    """
    User-facing logging configuration.

    Attributes:
        console_level: Minimum level for stderr console output.
            Defaults to INFO.
        file_path: Path to a log file. When None (the default), file
            logging is disabled and ``file_level`` is inert.
        file_level: Minimum level for file output. Required with a
            default of DEBUG so there is never a None to narrow at the
            use site; it simply has no effect while ``file_path`` is
            None.
    """

    model_config = ConfigDict(
        frozen=True,
        extra='forbid',
        validate_default=True,
    )

    console_level: int = logging.INFO
    file_path: Path | None = None
    file_level: int = logging.DEBUG

    @field_validator('console_level', 'file_level', mode='before')
    @classmethod
    def _coerce_level_name(cls, value: object) -> object:
        """
        Accept standard level names (case-insensitively) as well as ints.

        Args:
            value: The raw field value — an int passes through
                untouched; a string is looked up in the stdlib level
                name mapping.

        Returns:
            The normalized value (int for recognized names; non-string
            values unchanged for downstream type validation).

        Raises:
            ValueError: If a string is not a recognized stdlib level
                name.
        """
        if isinstance(value, str):
            level_names: dict[str, int] = logging.getLevelNamesMapping()
            normalized_name: str = value.strip().upper()
            if normalized_name not in level_names:
                raise ValueError(f'unknown log level name: {value!r}')
            return level_names[normalized_name]
        return value
