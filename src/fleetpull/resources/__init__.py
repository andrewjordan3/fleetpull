# src/fleetpull/resources/__init__.py
"""Packaged data resources shipped inside the wheel.

Holds ``config.example.yaml`` -- the annotated starter configuration a
user materializes with ``fleetpull init-config`` (read through
``importlib.resources`` so it resolves in both a wheel and an editable
install). This package carries data, not code; it exports nothing.
"""

__all__: list[str] = []
