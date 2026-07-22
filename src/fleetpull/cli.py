# src/fleetpull/cli.py
"""The fleetpull command line: config-driven sync and config scaffolding.

Two subcommands. ``fleetpull sync <config>`` is the shell form of
``Sync(config).run()`` (DESIGN §10): parse the arguments, run the sync, and
translate the operational-failure family (``FleetpullError``) into a stderr
line and exit code 1. No logging setup happens here — ``Sync.run`` applies
the config's logging section itself — and an unexpected exception propagates
with its traceback: a bug report, not an operational outcome. ``fleetpull
init-config [path]`` writes the packaged annotated example configuration to
disk, the onboarding path for a pip-installed user who has no repository to
copy it from.
"""

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

from fleetpull.api import Sync
from fleetpull.config import EXAMPLE_CONFIG_FILENAME, write_example_config
from fleetpull.exceptions import FleetpullError

__all__: list[str] = ['main']


def _build_parser() -> argparse.ArgumentParser:
    """Build the ``fleetpull`` argument parser.

    Two required subcommands: ``sync`` (run a config-driven sync) and
    ``init-config`` (write the example configuration).

    Returns:
        The configured parser.
    """
    parser = argparse.ArgumentParser(
        prog='fleetpull',
        description=(
            'Pull fleet telematics data from provider APIs into typed parquet.'
        ),
    )
    subcommands = parser.add_subparsers(dest='command', required=True)

    sync_parser = subcommands.add_parser(
        'sync', help='run a config-driven sync: fleetpull sync <config.yaml>'
    )
    sync_parser.add_argument(
        'config', type=Path, help='path to the fleetpull YAML configuration file'
    )

    init_parser = subcommands.add_parser(
        'init-config',
        help=(
            'write the annotated example configuration to a path '
            f'(default ./{EXAMPLE_CONFIG_FILENAME})'
        ),
    )
    init_parser.add_argument(
        'path',
        type=Path,
        nargs='?',
        default=Path(EXAMPLE_CONFIG_FILENAME),
        help=(
            'destination file, or an existing directory to write '
            f'{EXAMPLE_CONFIG_FILENAME} into '
            f'(default ./{EXAMPLE_CONFIG_FILENAME})'
        ),
    )
    init_parser.add_argument(
        '--force',
        action='store_true',
        help='overwrite the destination if it already exists',
    )
    return parser


def _run_sync(config: Path) -> int:
    """Run a config-driven sync, mapping operational failures to exit code 1.

    Args:
        config: The YAML configuration path.

    Returns:
        ``0`` on success; ``1`` on a ``FleetpullError``-family failure
        (whose message lands on stderr).

    Side Effects:
        Runs the sync (network, parquet, state, logging per the config);
        writes the failure message to ``sys.stderr`` on an operational
        error.
    """
    try:
        Sync(config).run()
    except FleetpullError as error:
        sys.stderr.write(f'{error}\n')
        return 1
    return 0


def _run_init_config(path: Path, *, force: bool) -> int:
    """Write the example configuration, mapping a refusal to exit code 1.

    Args:
        path: The destination file, or an existing directory.
        force: Overwrite an existing destination when ``True``.

    Returns:
        ``0`` after writing; ``1`` when the destination exists (and
        ``force`` is unset) or the write fails, the reason on stderr.

    Side Effects:
        Writes the example config to disk; prints the written path to
        stdout on success, the failure to ``sys.stderr`` otherwise.
    """
    try:
        written = write_example_config(path, force=force)
    except OSError as error:
        sys.stderr.write(f'{error}\n')
        return 1
    sys.stdout.write(f'wrote example configuration to {written}\n')
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Run the fleetpull CLI and return its exit code.

    Args:
        argv: The argument vector to parse, or ``None`` for ``sys.argv[1:]``
            (the console-script entry point).

    Returns:
        ``0`` on success; ``1`` on an operational failure (a
        ``FleetpullError``-family sync error, or a refused/failed config
        write), whose message lands on stderr.

    Raises:
        SystemExit: Argparse's exit on a missing or unknown argument
            (exit code 2).

    Side Effects:
        Per subcommand: runs the sync, or writes the example config.
    """
    arguments = _build_parser().parse_args(argv)
    if arguments.command == 'init-config':
        return _run_init_config(arguments.path, force=arguments.force)
    return _run_sync(arguments.config)
