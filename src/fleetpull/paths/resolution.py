# src/fleetpull/paths/resolution.py
"""Path expansion and lexical absolute-path normalization.

The resolution leaf of the ``paths`` package — a dependency-free utility used by
config, state, storage, and anywhere else that needs to turn a user-provided path
into an absolute, normalized ``Path``. It performs only lexical resolution —
expanding ``~`` and anchoring relative paths to the current working directory —
and never touches the filesystem: no existence check, no directory creation, no
symlink dereference. Domain meaning (is this a file or a directory? must it exist?
is it on local disk?) belongs to the layer that knows what the path is for.
"""

import os
from pathlib import Path

__all__: list[str] = ['PathInput', 'resolve_path']

type PathInput = str | Path


def resolve_path(value: PathInput) -> Path:
    """
    Expand and lexically normalize a user-provided filesystem path.

    Resolution order:
        1. Reject unsupported types and empty or whitespace-only strings.
        2. Expand ``~`` and ``~user`` to the home directory via
           :meth:`Path.expanduser`.
        3. Anchor a still-relative path to the current working directory.
        4. Lexically normalize — collapse ``.``, ``..``, and redundant
           separators — via :func:`os.path.normpath`, without touching the
           filesystem.

    Purely lexical: no filesystem access, no symlink dereference, no existence
    check, and no directory creation. An intentional symlink in the path is
    preserved rather than canonicalized, and a path to something that does not
    exist resolves fine — the caller decides what existence and on-disk
    suitability mean.

    Args:
        value: User-provided path. ``str`` and :class:`Path` are accepted; empty
            or whitespace-only strings are rejected rather than silently resolving
            to the current directory.

    Returns:
        An absolute, expanded, lexically normalized path.

    Raises:
        TypeError: ``value`` is not a ``str`` or :class:`Path`.
        ValueError: ``value`` is empty or whitespace-only, or ``~`` cannot be
            expanded (no resolvable home directory).

    Side Effects:
        Reads the current user's home directory and the current working
        directory. Does not touch the target path on the filesystem.
    """
    raw_path: str = _coerce_nonempty_str(value)

    try:
        expanded_path: Path = Path(raw_path).expanduser()
    except RuntimeError as exc:
        raise ValueError(f'path {raw_path!r} could not expand user home') from exc

    anchored_path: Path
    if expanded_path.is_absolute():
        anchored_path = expanded_path
    else:
        anchored_path = Path.cwd() / expanded_path

    normalized_path: str = os.path.normpath(anchored_path)
    return Path(normalized_path)


def _coerce_nonempty_str(value: PathInput) -> str:
    """
    Convert a supported path value to a non-empty string.

    Args:
        value: Path ``str`` or :class:`Path` value.

    Returns:
        Non-empty path string.

    Raises:
        TypeError: ``value`` is not a ``str`` or :class:`Path`.
        ValueError: ``value`` is an empty or whitespace-only string.

    Side Effects:
        None.
    """
    if not isinstance(value, str | Path):
        raise TypeError(f'path value must be str or Path, got {type(value).__name__}')

    raw_path: str = str(value)
    if raw_path.strip() == '':
        raise ValueError('path value must not be empty')

    return raw_path
