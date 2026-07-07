# src/fleetpull/config/state.py
"""State-section configuration: where operational SQLite state lives.

The ``state:`` YAML section -- AUD-13's landing. The path is separable
from ``storage.dataset_root`` for the DESIGN section 5 reason: SQLite
must stay on a real local disk even when the parquet dataset sits on a
network filesystem. One module per config section (house rule).
"""

import logging
from pathlib import Path

from pydantic import BaseModel, ConfigDict

__all__: list[str] = ['StateConfig']

logger = logging.getLogger(__name__)


class StateConfig(BaseModel):
    """
    User-facing operational-state settings, one instance per run.

    Attributes:
        database_path: Where the SQLite state database lives. Optional
            in YAML; ``None`` survives only inside the raw section --
            ``load_config`` always resolves it, defaulting to
            ``<dataset_root>/.fleetpull/state.sqlite3`` (DESIGN
            section 5), so a loaded config always carries a real path.
    """

    model_config = ConfigDict(frozen=True, extra='forbid', validate_default=True)

    database_path: Path | None = None
