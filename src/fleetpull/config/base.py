# src/fleetpull/config/base.py
"""The shared configuration-model base: the policy, stated exactly once.

Every config model inherits ``ConfigModel``: frozen so a loaded config
cannot mutate mid-run, ``extra='forbid'`` so a misspelled YAML key is
rejected rather than silently dropped, and ``validate_default=True`` so
defaulted values pass the same validators as supplied ones.
"""

from pydantic import BaseModel, ConfigDict

__all__: list[str] = ['ConfigModel']


class ConfigModel(BaseModel):
    """Base for every fleetpull configuration model; carries the policy only."""

    model_config = ConfigDict(frozen=True, extra='forbid', validate_default=True)
