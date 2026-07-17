# src/fleetpull/cli.py
"""The fleetpull command line: the yaml-run entry over ``Sync``.

``fleetpull sync <config>`` is the shell form of ``Sync(config).run()``
(DESIGN §10): parse the arguments, run the sync, and translate the
operational-failure family (``FleetpullError``) into a stderr line and exit
code 1. No logging setup happens here — ``Sync.run`` applies the config's
logging section itself — and an unexpected exception propagates with its
traceback: a bug report, not an operational outcome.
"""

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

from fleetpull.api import Sync
from fleetpull.exceptions import FleetpullError

__all__: list[str] = ['main']


def _build_parser() -> argparse.ArgumentParser:
    """Build the ``fleetpull`` argument parser.

    One required subcommand, ``sync``, taking the YAML configuration path
    as its positional argument.

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
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the fleetpull CLI and return its exit code.

    Args:
        argv: The argument vector to parse, or ``None`` for ``sys.argv[1:]``
            (the console-script entry point).

    Returns:
        ``0`` on success; ``1`` when the run failed with an operational
        (``FleetpullError``-family) error, whose message lands on stderr.

    Raises:
        SystemExit: Argparse's exit on a missing or unknown argument
            (exit code 2).

    Side Effects:
        Runs the sync — network fetches, parquet and state writes, and
        logging setup per the config — and writes the failure message to
        ``sys.stderr`` on an operational error.
    """
    arguments = _build_parser().parse_args(argv)
    try:
        Sync(arguments.config).run()
    except FleetpullError as error:
        sys.stderr.write(f'{error}\n')
        return 1
    return 0
