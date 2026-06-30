"""Connection: a resource for holding connection/credential values with env-loading support."""

from __future__ import annotations

import os
from typing import ClassVar

from pydantic import model_validator
from pydantic_settings import BaseSettings

from interloper.oauth import OAuthConfig
from interloper.resource import InputField, Resource, ResourceDefinition, SecretField

#: OAuth roles whose fields are the in-house credentials resolved from env.
_ENV_CREDENTIAL_ROLES = ("client_id", "client_secret")


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

    Connections that support OAuth subclass ``OAuthConnection`` (custom token
    shape) or ``RefreshTokenOAuthConnection`` (standard refresh-token trio);
    the ``oauth`` config lives there, not on this base.
    """

    kind: ClassVar[str] = "connection"
    tags: ClassVar[list[str]] = []


class OAuthConnection(Connection):
    """OAuth connection base — drives the "Sign in with X" form from ``oauth``.

    Carries the ``oauth`` config but **no credential fields**. Subclass this
    directly when the connection's credential fields are named differently from
    the standard trio, declaring its own fields and a matching
    ``OAuthConfig.fields`` mapping::

        @connection(oauth=OAuthConfig("tiktok", fields={"refresh_token": "access_token"}))
        class TiktokAdsConnection(OAuthConnection):
            access_token: str = SecretField()

    ``OAuthConfig.fields`` maps the OAuth roles (``client_id`` /
    ``client_secret`` / ``refresh_token``) to the connection's field names, and
    drives the whole form: those fields are hidden in sign-in mode, the
    ``client_id`` / ``client_secret`` fields are resolved from env, and the
    ``refresh_token`` field receives the token from the sign-in response.

    Most connections use the standard refresh-token flow and should subclass
    ``RefreshTokenOAuthConnection`` instead, which declares the trio.
    """

    oauth: ClassVar[OAuthConfig | None] = None

    @classmethod
    def definition(cls) -> ResourceDefinition:
        """Inject the ``x-oauth`` extension so the form can render the button.

        The extension carries the provider metadata and the ``fields`` mapping;
        the form uses ``fields`` to hide the credential fields in sign-in mode
        and to place the token returned by the flow.

        Returns:
            The resource definition, enriched when ``oauth`` is configured.
        """
        definition = super().definition()
        if isinstance(cls.oauth, OAuthConfig):
            definition.config_schema["x-oauth"] = cls.oauth.to_schema_ext()
            definition.provider = cls.oauth.provider
        return definition

    @model_validator(mode="after")
    def _resolve_credentials(self) -> OAuthConnection:
        """Fill blank ``client_id`` / ``client_secret`` fields from env.

        The fields named by the ``client_id`` / ``client_secret`` roles read
        ``<PROVIDER>_CLIENT_ID`` / ``<PROVIDER>_CLIENT_SECRET`` (the same vars
        the token-exchange endpoint uses) when left blank. An explicitly set
        value (a per-connection override) always wins.

        Returns:
            The connection instance (self), with credentials filled in.
        """
        if not isinstance(self.oauth, OAuthConfig):
            return self
        prefix = self.oauth.provider.upper()
        for role in _ENV_CREDENTIAL_ROLES:
            field = self.oauth.fields.get(role)
            if field and field in type(self).model_fields and not getattr(self, field, ""):
                value = os.environ.get(f"{prefix}_{role.upper()}", "")
                if value:
                    setattr(self, field, value)
        return self


class RefreshTokenOAuthConnection(OAuthConnection):
    """A connection using the standard OAuth2 refresh-token flow.

    Declares the credential trio — ``client_id`` / ``client_secret`` (in-house
    OAuth credentials resolved from env) and ``refresh_token`` (filled on
    sign-in) — which the default ``OAuthConfig.fields`` mapping targets, so the
    standard connections only add their own fields::

        @connection(oauth=OAuthConfig("linkedin", scope="r_ads"))
        class LinkedinAdsConnection(RefreshTokenOAuthConnection):
            account_id: str

    Connections whose credential fields are named differently subclass
    ``OAuthConnection`` directly and pass their own ``fields`` mapping.

    ``client_id`` / ``client_secret`` default to the in-house per-provider
    credentials from ``<PROVIDER>_CLIENT_ID`` / ``<PROVIDER>_CLIENT_SECRET``
    (the same vars the token-exchange endpoint reads), so the in-house secret
    is never sent to the browser or stored per connection. Setting them
    explicitly overrides the in-house app — e.g. to use your own OAuth client.
    """

    client_id: str = InputField("")
    client_secret: str = SecretField("")
    refresh_token: str = SecretField()
