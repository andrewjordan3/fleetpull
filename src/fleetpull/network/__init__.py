"""Transport layer: an organizational namespace.

Carries no aggregated surface of its own — the transport machinery lives
in its subpackages (``auth/``, ``contract/``, ``limits/``, ``retry/``,
``tls/``), each with its own face. Callers import from those subpackages
directly.
"""

__all__: list[str] = []
