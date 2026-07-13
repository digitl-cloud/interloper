"""Connection: a resource for holding connection/credential values with env-loading support."""

from __future__ import annotations

import os
from typing import Any, ClassVar

from pydantic import model_validator

from interloper.oauth import OAuthConfig
from interloper.resource import InputField, Resource, ResourceDefinition, SecretField


class Connection(Resource):
    """A resource for database/service connection credentials.

    Like every ``Resource``, connection values can be loaded from
    environment variables, .env files, or passed directly::

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

    def check(self) -> bool:
        """Verify this connection with a lightweight authenticated call.

        Override in a subclass (sync or ``async``) to make the cheapest call
        that proves the credentials work — often a one-line delegation to an
        existing ``@fetch_field_provider`` method. Like fetch providers, it
        may run inside the API process, so it must use lightweight HTTP
        (``httpx``), never a heavy provider SDK.

        Returns ``True`` when the connection works, ``False`` when it
        provably doesn't. Exceptions are also failures: ``httpx`` errors are
        categorised by the caller (401/403 → bad credentials, timeouts →
        network); raise :class:`~interloper.errors.ConnectionCheckError` to
        surface a curated message instead.
        """
        raise NotImplementedError

    @classmethod
    def checkable(cls) -> bool:
        """Whether this connection class implements :meth:`check`.

        Returns:
            True when the class overrides the base hook.
        """
        return cls.check is not Connection.check

    @classmethod
    def definition(cls) -> ResourceDefinition:
        """Advertise :meth:`check` support so UIs can offer a test step.

        Returns:
            The resource definition with ``checkable`` set.
        """
        definition = super().definition()
        definition.checkable = cls.checkable()
        return definition


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

    @classmethod
    def env_credential(cls, suffix: str) -> str | None:
        """The in-house OAuth credential for ``suffix`` (e.g. ``"CLIENT_ID"``).

        Read from ``INTERLOPER_<PROVIDER>_<SUFFIX>`` — the same vars the
        token-exchange endpoint uses. A ``mode="before"`` validator falls back to
        this for its declared credential fields, so a required field can be
        satisfied by the in-house app. Returns ``None`` when unset, so a field
        left unfilled stays ``None`` and fails the required check rather than
        passing as an empty string.

        Returns:
            The env credential value, or ``None``.
        """
        if not isinstance(cls.oauth, OAuthConfig):
            return None
        return os.environ.get(f"INTERLOPER_{cls.oauth.provider.upper()}_{suffix}")

    @staticmethod
    def resolve_field(data: dict[str, Any], field: str, value: str | None) -> None:
        """Fill ``data[field]`` from ``value`` when the field is blank and value is set.

        A ``mode="before"`` ``resolve_credentials`` validator uses this per field.
        Leaves the field absent when there is no value, so a required field fails
        with "Field required" rather than being satisfied by an empty value.
        """
        if value and not data.get(field):
            data[field] = value


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
    ``OAuthConnection`` directly, declare their own fields, and inject them from
    env with a ``mode="before"`` validator calling :meth:`env_credential`.

    All three fields are **required**. ``client_id`` / ``client_secret`` may be
    supplied by the in-house per-provider credentials
    (``INTERLOPER_<PROVIDER>_CLIENT_ID`` / ``INTERLOPER_<PROVIDER>_CLIENT_SECRET`` — the
    same vars the token-exchange endpoint reads), injected before validation so
    the sign-in flow can omit them and the in-house secret is never sent to the
    browser or stored per connection. An explicit value overrides the in-house
    app; when neither the caller nor env supplies one, the required check fails.
    """

    client_id: str = InputField()
    client_secret: str = SecretField()
    refresh_token: str = SecretField()

    @model_validator(mode="before")
    @classmethod
    def resolve_credentials(cls, data: Any) -> Any:
        """Inject blank ``client_id`` / ``client_secret`` from the in-house env creds.

        Runs before validation so the in-house app can satisfy these required
        fields when the caller omits them; an explicit value is left untouched.

        Returns:
            The (possibly augmented) input data.
        """
        if isinstance(data, dict):
            cls.resolve_field(data, "client_id", cls.env_credential("CLIENT_ID"))
            cls.resolve_field(data, "client_secret", cls.env_credential("CLIENT_SECRET"))
        return data
