"""Connection: a resource for holding connection/credential values with env-loading support."""

from __future__ import annotations

import os
from typing import Any, ClassVar

from pydantic import model_validator
from pydantic_settings import BaseSettings

from interloper.oauth import OAuthConfig
from interloper.resource import InputField, Resource, SecretField


def validate_oauth_fields(cls: type[Connection]) -> None:
    """Check that an ``oauth`` config maps onto declared model fields.

    Every model field name in ``OAuthConfig.fields.values()`` must exist on
    the class, otherwise the frontend would silently fail to fill the form
    after sign-in.

    Args:
        cls: The connection class to validate.

    Raises:
        TypeError: If the mapping targets fields the class does not declare.
    """
    oauth = getattr(cls, "oauth", None)
    if not isinstance(oauth, OAuthConfig):
        return
    missing = sorted(set(oauth.fields.values()) - set(cls.model_fields))
    if missing:
        raise TypeError(
            f"{cls.__name__}: OAuthConfig.fields maps token response keys to "
            f"undeclared model fields: {missing}"
        )


class Connection(BaseSettings, Resource):
    """A resource for database/service connection credentials.

    Extends both ``Resource`` and ``BaseSettings``, so connection values
    can be loaded from environment variables, .env files, or passed
    directly::

        class MyConnection(Connection):
            host: str = "localhost"
            port: int = 5432
            username: str
            password: str

        # Loads USERNAME, PASSWORD from environment if not passed explicitly
        conn = MyConnection()

    Connections that support OAuth declare it via the decorator::

        @connection(oauth=OAuthConfig("amazon", scope="..."))
        class AmazonAdsConnection(OAuthConnection):
            ...

    (An ``oauth: ClassVar[OAuthConfig]`` in the class body is equivalent.)
    """

    kind: ClassVar[str] = "connection"
    tags: ClassVar[list[str]] = []
    oauth: ClassVar[OAuthConfig | None] = None

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        """Validate the oauth fields mapping as soon as the model is built."""
        super().__pydantic_init_subclass__(**kwargs)
        validate_oauth_fields(cls)


class OAuthConnection(Connection):
    """A connection authenticated via the standard OAuth2 refresh-token flow.

    Declares the credential trio that ``OAuthConfig``'s default ``fields``
    mapping targets, so OAuth-enabled connections only add their own
    fields::

        @connection(oauth=OAuthConfig("linkedin", scope="r_ads"))
        class LinkedinAdsConnection(OAuthConnection):
            account_id: str

    Connections with a non-standard token response shape (e.g. Facebook's
    app_id/app_secret/access_token) declare their own fields on a plain
    ``Connection`` and pass a custom ``fields=`` mapping to ``OAuthConfig``.

    ``client_id`` / ``client_secret`` are the *app* credentials. They are
    optional and default to the in-house per-provider credentials resolved
    from the ``<PROVIDER>_CLIENT_ID`` / ``<PROVIDER>_CLIENT_SECRET``
    environment (the same vars the API's token-exchange endpoint reads), so
    the in-house secret is never sent to the browser or stored per
    connection. Setting them explicitly overrides the in-house app — e.g. to
    use your own OAuth client.
    """

    client_id: str = InputField("")
    client_secret: str = SecretField("")
    refresh_token: str = SecretField()

    @model_validator(mode="after")
    def _resolve_app_credentials(self) -> OAuthConnection:
        """Fall back to the in-house per-provider app credentials from env.

        Only fills blanks: an explicitly set ``client_id`` / ``client_secret``
        (a per-connection override) always wins. Keyed on the provider from
        ``oauth`` — not the connection's settings ``env_prefix`` — so it reads
        the same ``<PROVIDER>_CLIENT_ID`` / ``_CLIENT_SECRET`` vars the token
        exchange uses.

        Returns:
            The connection instance (self), with app credentials filled in.
        """
        if self.oauth is None:
            return self
        prefix = self.oauth.provider.upper()
        if not self.client_id:
            self.client_id = os.environ.get(f"{prefix}_CLIENT_ID", "")
        if not self.client_secret:
            self.client_secret = os.environ.get(f"{prefix}_CLIENT_SECRET", "")
        return self
