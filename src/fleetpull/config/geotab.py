# src/fleetpull/config/geotab.py
"""
GeoTab authentication configuration model.

GeotabAuthConfig is the GeoTab authentication section of fleetpull's
user-provided YAML configuration. The password is a ``SecretStr``: its
value is extracted with ``.get_secret_value()`` only at the moment of
use (inside the real authenticate function), and is never logged or
included in ``repr()``/``str()`` output.
"""

from pydantic import BaseModel, ConfigDict, Field, SecretStr

__all__: list[str] = ['GeotabAuthConfig']


class GeotabAuthConfig(BaseModel):
    """
    GeoTab authentication credentials and target database.

    Attributes:
        username: GeoTab account username (non-empty).
        password: GeoTab account password; masked in all output.
        database: GeoTab database name (non-empty).
        server: The authentication host. ``Authenticate`` may redirect
            subsequent calls to a different resolved host — that is
            session state, not configuration, so it never lives here.
    """

    model_config = ConfigDict(
        frozen=True,
        extra='forbid',
        validate_default=True,
    )

    username: str = Field(min_length=1)
    password: SecretStr
    database: str = Field(min_length=1)
    server: str = 'my.geotab.com'
