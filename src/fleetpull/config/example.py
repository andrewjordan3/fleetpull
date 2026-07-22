# src/fleetpull/config/example.py
"""The packaged example configuration: read it, or materialize it to disk.

``config.example.yaml`` ships inside the wheel (``fleetpull.resources``),
so a pip-installed user has no repository to copy it from. ``fleetpull
init-config`` writes it to a path of their choosing through
``write_example_config``; ``read_example_config`` returns its text for any
programmatic caller. Read through ``importlib.resources`` so the file
resolves identically in a built wheel and an editable checkout.
"""

from importlib import resources
from pathlib import Path

from fleetpull.paths import PathInput, resolve_path

__all__: list[str] = [
    'EXAMPLE_CONFIG_FILENAME',
    'read_example_config',
    'write_example_config',
]

# The resource's package and name; the file lives in ``fleetpull/resources``.
_RESOURCE_PACKAGE = 'fleetpull.resources'
_RESOURCE_NAME = 'config.example.yaml'

# The default filename ``init-config`` writes when given only a directory
# (or nothing) -- not the resource's own name, so the materialized file
# reads as the user's config rather than "the example".
EXAMPLE_CONFIG_FILENAME = 'fleetpull_config.yaml'


def read_example_config() -> str:
    """Return the packaged example configuration's text.

    Returns:
        The full ``config.example.yaml`` document, verbatim UTF-8.

    Side Effects:
        Reads the packaged resource.
    """
    return (
        resources.files(_RESOURCE_PACKAGE)
        .joinpath(_RESOURCE_NAME)
        .read_text(encoding='utf-8')
    )


def _resolve_destination(destination: PathInput) -> Path:
    """Resolve the write target, defaulting a directory to the config filename.

    Args:
        destination: The user-supplied path -- a file path, or a directory
            (existing) into which the default filename is written.

    Returns:
        The concrete file path to write.
    """
    resolved = resolve_path(destination)
    if resolved.is_dir():
        return resolved / EXAMPLE_CONFIG_FILENAME
    return resolved


def write_example_config(destination: PathInput, *, force: bool = False) -> Path:
    """Write the packaged example configuration to ``destination``.

    Args:
        destination: Where to write -- a file path, or an existing
            directory (the default filename ``fleetpull_config.yaml`` is
            appended). The parent directory must already exist.
        force: Overwrite an existing target when ``True``; otherwise a
            pre-existing target is a loud refusal (never clobber a config a
            user may have edited).

    Returns:
        The path actually written.

    Raises:
        FileExistsError: The target exists and ``force`` is ``False``.
        OSError: The parent directory is missing or the write fails.

    Side Effects:
        Writes a file to disk.
    """
    target = _resolve_destination(destination)
    if target.exists() and not force:
        raise FileExistsError(f'{target} already exists; pass force to overwrite it')
    target.write_text(read_example_config(), encoding='utf-8')
    return target
