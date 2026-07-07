# src/fleetpull/config/storage.py
"""Storage-section configuration: where the parquet dataset lives.

The ``storage:`` YAML section. ``dataset_root`` is authored here and fed
into the runtime ``SyncConfig`` by the loader (``config/loader.py``) --
the runner consumes it off ``SyncConfig``, but its YAML home is this
section, so ``sync.dataset_root`` is never a YAML key. One module per
config section (house rule).
"""

import logging
from pathlib import Path

from pydantic import BaseModel, ConfigDict

__all__: list[str] = ['StorageConfig']

logger = logging.getLogger(__name__)


class StorageConfig(BaseModel):
    """
    User-facing storage settings, one instance per run.

    Attributes:
        dataset_root: The root directory the dataset is written under
            (``{root}/{provider}/{endpoint}/``, DESIGN section 3).
            Required: there is no safe default for where output lands.
            Use a real local path -- never a cloud-synced folder
            (OneDrive and kin), whose sync clients fight the writer's
            atomic renames. A string coerces to ``Path``; the storage
            layer normalizes it via ``resolve_path``.
    """

    model_config = ConfigDict(frozen=True, extra='forbid', validate_default=True)

    dataset_root: Path
