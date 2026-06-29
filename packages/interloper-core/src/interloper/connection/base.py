"""Connection: a resource for holding connection/credential values with env-loading support."""

from __future__ import annotations

import os
from typing import Any, ClassVar

from pydantic import model_validator
from pydantic_settings import BaseSettings

from interloper.oauth import OAuthConfig
from interloper.resource import InputField, Resource, ResourceDefinition, SecretField


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

    Connections that support OAuth subclass ``OAuthConnection`` rather than
    ``Connection``; the ``oauth`` config lives there, not on this base.
    """

    kind: ClassVar[str] = "connection"
    tags: ClassVar[list[str]] = []


class OAuthConnection(Connection):
    """A connection authenticated via OAuth — the home for all OAuth support.

    Declares the standard credential trio (``client_id`` / ``client_secret`` /
    ``refresh_token``) that ``OAuthConfig``'s default ``fields`` mapping
    targets, so OAuth-enabled connections only add their own fields::

        @connection(oauth=OAuthConfig("linkedin", scope="r_ads"))
        class LinkedinAdsConnection(OAuthConnection):
            account_id: str

    The ``oauth`` config (provider, scope, token→field mapping) is declared
    via the ``@connection(oauth=...)`` decorator kwarg or an equivalent
    ``oauth: ClassVar[OAuthConfig]`` in the class body. Only ``OAuthConnection``
    subclasses carry it — ``definition()`` injects the ``x-oauth`` schema
    extension and tags the resolved credential fields so the form can render a
    "Sign in with X" button.

    The whole trio is optional (defaults to ``""``) so connections with a
    non-standard token shape — a single long-lived ``access_token`` and no
    refresh token, or differently named app credentials — can still subclass
    this base and add their own fields.

    ``client_id`` / ``client_secret`` are the *app* credentials. Left blank,
    they default to the in-house per-provider credentials resolved from the
    ``<PROVIDER>_CLIENT_ID`` / ``<PROVIDER>_CLIENT_SECRET`` environment (the
    same vars the API's token-exchange endpoint reads), so the in-house secret
    is never sent to the browser or stored per connection. Setting them
    explicitly overrides the in-house app — e.g. to use your own OAuth client.
    """

    oauth: ClassVar[OAuthConfig | None] = None

    client_id: str = InputField("")
    client_secret: str = SecretField("")
    refresh_token: str = SecretField("")

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        """Validate the oauth fields mapping as soon as the model is built."""
        super().__pydantic_init_subclass__(**kwargs)
        validate_oauth_fields(cls)

    @classmethod
    def definition(cls) -> ResourceDefinition:
        """Augment the resource definition with the OAuth schema metadata.

        Injects the ``x-oauth`` extension from the ``oauth`` config and tags
        the fields the sign-in flow resolves with ``x-oauth-managed`` so the
        form hides them in sign-in mode, surfacing them only for manual entry.

        Returns:
            The resource definition, enriched when ``oauth`` is configured.
        """
        definition = super().definition()
        if isinstance(cls.oauth, OAuthConfig):
            schema = definition.config_schema
            schema["x-oauth"] = cls.oauth.to_schema_ext()
            properties = schema.get("properties", {})
            for name in cls.oauth_managed_fields():
                if name in properties:
                    properties[name] = {**properties[name], "x-oauth-managed": True}
            definition.provider = cls.oauth.provider
        return definition

    @classmethod
    def oauth_managed_fields(cls) -> list[str]:
        """Credential fields the OAuth flow resolves — hidden in sign-in mode.

        The standard trio ``OAuthConnection`` adds over a plain ``Connection``
        (``client_id`` / ``client_secret`` from env, ``refresh_token`` from
        sign-in), unioned with the token-mapped fields a custom-shaped
        connection declares (e.g. a long-lived ``access_token``). Derived from
        the model, so the names live only at their declaration above.

        Returns:
            The credential field names, deduplicated in declaration order.
        """
        credentials = [f for f in OAuthConnection.model_fields if f not in Connection.model_fields]
        token_fields = cls.oauth.fields.values() if isinstance(cls.oauth, OAuthConfig) else ()
        return list(dict.fromkeys([*credentials, *token_fields]))

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
