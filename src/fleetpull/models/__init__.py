"""Pydantic response models: the shared config-policy base, one module per provider.

``base.py`` holds ``ResponseModel`` (the config-policy base); per-provider model
modules are added as providers are implemented.
"""

from fleetpull.models.base import ResponseModel

__all__: list[str] = ['ResponseModel']
