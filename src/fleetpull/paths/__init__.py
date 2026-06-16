# src/fleetpull/paths/__init__.py
"""Filesystem path expansion and normalization utilities."""

from fleetpull.paths.resolution import PathInput, resolve_path

__all__: list[str] = ['PathInput', 'resolve_path']
