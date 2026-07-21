# src/fleetpull/config/geotab.py
"""GeoTab authentication configuration model.

GeotabAuthConfig is the GeoTab authentication section of fleetpull's
user-provided YAML configuration (nested under ``providers.geotab.auth``;
the provider section itself lives with its family in
``config/providers.py``). The password is a ``SecretStr``: its value is
extracted with ``.get_secret_value()`` only at the moment of use (inside
the real authenticate function), and is never logged or included in
``repr()``/``str()`` output.
"""

from pydantic import Field, SecretStr, field_validator

from fleetpull.config.base import ConfigModel

__all__: list[str] = ['DEFAULT_GEOTAB_SERVER', 'GeotabAuthConfig']

# The documented default authentication host. Hoisted as a named constant
# (and exported through the config face) because it is a shared provider
# fact: the auth section defaults to it, and the GeoTab spec builders use
# it as the pre-auth placeholder host for a credential-less config.
DEFAULT_GEOTAB_SERVER: str = 'my.geotab.com'


class GeotabAuthConfig(ConfigModel):
    """
    GeoTab authentication credentials and target database.

    Attributes:
        username: GeoTab account username (non-empty).
        password: GeoTab account password; masked in all output.
        database: GeoTab database name (non-empty).
        server: The authentication host — a bare hostname like
            ``my.geotab.com`` (no scheme, path, or whitespace). The
            authenticator builds ``https://{server}/apiv1`` from it.
            ``Authenticate`` may redirect subsequent calls to a
            different resolved host — that is session state, not
            configuration, so it never lives here.
    """

    username: str = Field(min_length=1)
    password: SecretStr
    database: str = Field(min_length=1)
    server: str = Field(default=DEFAULT_GEOTAB_SERVER, min_length=1)

    @field_validator('server')
    @classmethod
    def _server_is_bare_hostname(cls, server: str) -> str:
        """
        Reject anything but a bare hostname.

        The authenticator builds ``https://{server}/apiv1``; a scheme,
        path, or stray whitespace would corrupt that URL. Caught here at
        the config boundary with a message that says exactly what to
        write.

        Args:
            server: The configured server value.

        Returns:
            The value unchanged when it is a bare hostname.

        Raises:
            ValueError: When the value carries a scheme, a path, a
                slash, or whitespace.
        """
        if '/' in server:
            raise ValueError(
                f'server must be a bare hostname like "my.geotab.com" — no '
                f'scheme or path; got {server!r} (write "https://my.geotab.com" '
                f'as "my.geotab.com")'
            )
        if any(character.isspace() for character in server):
            raise ValueError(
                f'server must be a bare hostname with no whitespace; got {server!r}'
            )
        return server
