# src/fleetpull/paths/datasets.py
"""Dataset-layout path construction: locate an endpoint's directory under a
dataset root.

The shared, filesystem-neutral half of storage path-building. The parquet
writers and the metadata projection's caller (the runner, which resolves the
directory and hands it to ``storage.write_metadata_json``) locate an
endpoint's directory through this one function, so the construction lives in
``paths`` -- pure and shared. Like the rest of ``paths``, it never touches
the filesystem; directory creation is the writing layer's concern.
"""

from pathlib import Path

from fleetpull.paths.resolution import PathInput, resolve_path

__all__: list[str] = ['endpoint_directory']


def endpoint_directory(dataset_root: PathInput, provider: str, endpoint: str) -> Path:
    """Build the directory holding one endpoint's output files.

    The dataset is laid out one directory per ``(provider, endpoint)`` under the
    root (DESIGN §3): ``{root}/{provider}/{endpoint}/``. The provider and endpoint
    are passed as their directory-name strings (e.g. ``definition.provider.value``
    and ``definition.name`` at the call site), so ``paths`` need not import the
    vocabulary or endpoints layers.

    Args:
        dataset_root: The dataset root path, normalized via ``resolve_path``.
        provider: The provider directory name (e.g. ``'motive'``).
        endpoint: The endpoint directory name (e.g. ``'vehicles'``).

    Returns:
        The absolute, normalized endpoint directory path. Not created -- the
        writing layer creates it.
    """
    return resolve_path(dataset_root) / provider / endpoint
