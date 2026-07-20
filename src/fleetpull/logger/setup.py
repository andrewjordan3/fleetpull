# src/fleetpull/logger/setup.py
"""Logging configuration for the fleetpull package.

Provides centralized logging setup so every module that calls
``logging.getLogger(__name__)`` inherits the same level, format, and
handler configuration. Users drive the setup from a validated
``LoggerConfig`` — no ad-hoc level parsing or path expansion happens
here; that is the configuration layer's job.

Library convention:
    fleetpull attaches no handlers at import time and registers no
    ``NullHandler``. When a consuming application never configures
    logging, Python's ``lastResort`` handler surfaces WARNING-and-above
    records on stderr — for a data-fetching library, warnings (rate
    limit penalties, session invalidations) should be visible by
    default, not swallowed. Applications that want full control
    configure the ``'fleetpull'`` logger with stdlib tools or call
    :func:`setup_logger`.

Timestamps:
    All log timestamps are UTC with a ``Z`` suffix. fleetpull's data
    semantics are UTC end to end; log time matches data time so
    incident correlation never crosses a timezone boundary.
"""

import logging
import sys
import time
from typing import Final, TextIO

from fleetpull.config import LoggerConfig

__all__: list[str] = ['setup_logger']

# Name of the package's root logger; every fleetpull module logger
# (logging.getLogger(__name__)) is a descendant and inherits this
# configuration.
_PACKAGE_LOGGER_NAME: Final[str] = 'fleetpull'

# Shared format and date-format for every handler this module attaches.
# Module-level constants so tests can assert against them by identity
# rather than re-typing the literals.
_LOG_FORMAT: Final[str] = (
    '%(asctime)s - %(levelname)-8s - [%(threadName)s] - [%(name)s] - %(message)s'
)
_DATE_FORMAT: Final[str] = '%Y-%m-%dT%H:%M:%SZ'


def setup_logger(config: LoggerConfig) -> None:
    """
    Configure the fleetpull package logger from a ``LoggerConfig``.

    Clears any handlers already attached to the ``fleetpull`` logger,
    installs a console handler (always) and an optional file handler,
    and sets the package logger's level so that neither handler is
    starved by an ancestor filter. All module loggers created with
    ``logging.getLogger(__name__)`` inside fleetpull inherit the result
    automatically.

    The function is idempotent: calling it twice produces the same
    handler count as one call. The second call's ``LoggerConfig`` fully
    supersedes the first.

    Args:
        config: Validated ``LoggerConfig``. ``console_level`` drives
            stderr output; ``file_path``, when set, enables file
            logging at ``file_level``.

    Returns:
        None. Matches the stdlib convention (``logging.basicConfig``
        and friends all return None); module loggers inherit the
        configuration automatically, so no handle is useful.

    Side Effects:
        - Closes and replaces the ``fleetpull`` logger's existing
          handlers (closing releases the previous configuration's file
          descriptors).
        - Sets the ``fleetpull`` logger's level (the minimum of the
          active handler levels, so no record is filtered at the
          logger before reaching a handler).
        - Sets ``propagate = False`` on the ``fleetpull`` logger so a
          hosting application that configures the root logger does not
          see duplicate records.
        - Creates the parent directory of ``file_path`` if file
          logging is enabled and the directory does not exist.
        - Emits a single INFO-level record confirming the
          configuration, through the just-installed handlers.
    """
    package_logger: logging.Logger = logging.getLogger(_PACKAGE_LOGGER_NAME)

    # Close, then clear, existing handlers: closing releases the file
    # descriptors (and Windows file locks) a previous configuration
    # holds; clearing makes the call idempotent so a caller can
    # reconfigure at runtime without accumulating duplicates. Closing
    # a StreamHandler never closes its underlying stream, so stderr is
    # safe.
    for previous_handler in package_logger.handlers:
        previous_handler.close()
    package_logger.handlers.clear()

    # A hosting application may configure the root logger; disabling
    # propagation prevents every fleetpull record from being emitted
    # twice.
    package_logger.propagate = False

    log_formatter: logging.Formatter = logging.Formatter(
        fmt=_LOG_FORMAT,
        datefmt=_DATE_FORMAT,
    )
    # UTC timestamps: see the module docstring. The converter applies
    # to asctime only; record.created remains an epoch float.
    log_formatter.converter = time.gmtime

    console_handler: logging.StreamHandler[TextIO] = logging.StreamHandler(sys.stderr)
    console_handler.setFormatter(log_formatter)
    console_handler.setLevel(config.console_level)
    package_logger.addHandler(console_handler)

    if config.file_path is not None:
        config.file_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler: logging.FileHandler = logging.FileHandler(
            filename=config.file_path,
            mode='a',
            encoding='utf-8',
        )
        file_handler.setFormatter(log_formatter)
        file_handler.setLevel(config.file_level)
        package_logger.addHandler(file_handler)

    # The logger's own level must not filter records before they reach
    # any handler. file_level participates only when a file handler is
    # actually attached; otherwise it is inert by design.
    effective_level: int = (
        min(config.console_level, config.file_level)
        if config.file_path is not None
        else config.console_level
    )
    package_logger.setLevel(effective_level)

    package_logger.info(
        'Logging configured: console_level=%s%s',
        logging.getLevelName(config.console_level),
        f', file={config.file_path}' if config.file_path is not None else '',
    )
