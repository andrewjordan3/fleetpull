# src/fleetpull/polars_typing/__init__.py
"""The single sanctioned boundary for Polars type aliases that lack a public
equivalent.

Polars exposes some annotation types only under the private ``polars._typing``
module (``ParquetCompression`` and others to come). A strict-typing project needs
the exact types -- a hand-mirrored ``Literal`` can silently drift from what Polars
actually accepts -- but importing a private module across the codebase scatters
the risk. Quarantining those imports here makes a Polars relocation a one-file
fix, the same blast-radius isolation the package applies elsewhere; the locked
Polars version keeps any such upgrade deliberate. Add aliases here as needed.

A one-file subpackage (not a root module) so an internal compat shim stays out of
the user-facing package root, and so importers reach it through a package face
rather than an underscore-prefixed module name (CLAUDE.md).
"""

from polars._typing import ParquetCompression

__all__: list[str] = ['ParquetCompression']
