"""Connection: a resource for holding connection/credential values with env-loading support."""

from __future__ import annotations

import asyncio
import datetime as dt
import os
from typing import Any, ClassVar, cast

import httpx
from pydantic import BaseModel, Field, model_validator

from interloper.errors import format_exception
from interloper.oauth import PROVIDERS, OAuthAppCredentials, OAuthConfig
from interloper.operation import Operation, OperationContext, OperationResult
from interloper.resource import InputField, Resource, ResourceDefinition, SecretField
from interloper.utils.concurrency import invoke

#: Hard cap on one credential exchange — a renewal must never hold a run pod hostage.
_RENEWAL_TIMEOUT = 60

#: How long after a failed renewal the connection becomes due again.
_RENEWAL_RETRY_INTERVAL = dt.timedelta(hours=1)


class ConnectionState(BaseModel):
    """Machine-owned connection state (see ``Component.state_model``).

    Written by the renewal pipeline: the renewal controller stamps a
    provisional ``next_renewal_at`` when it enqueues a renewal run, and the
    run overwrites it with the real next due time on completion. Timestamps
    are canonical timezone-aware ISO-8601 strings, compared lexicographically
    in SQL like ``JobState``'s. ``last_run_at`` is stamped by run completion
    on every run's component — plumbing, hidden from state columns.
    """

    next_renewal_at: str | None = Field(default=None, title="Next renewal")
    last_renewed_at: str | None = Field(default=None, title="Last renewed")
    last_renewal_error: str | None = Field(default=None, title="Renewal error")
    last_run_at: str | None = Field(default=None, json_schema_extra={"x-hidden": True})


class Renewal(BaseModel):
    """The outcome of a successful :meth:`Connection.renew`.

    ``fields`` maps connection field names to their renewed values — the
    platform persists them into the stored (encrypted) config, so a rotated
    or re-issued credential replaces the aging one. Empty means the renewal
    succeeded but nothing needs persisting (a pure keep-alive).

    ``expires_in`` is the renewed credential's validity in seconds when the
    provider reports one; the platform schedules the next renewal from it
    (with a safety margin), falling back to the class's
    ``renewal_interval`` when absent.
    """

    fields: dict[str, str] = Field(default_factory=dict)
    expires_in: int | None = None


class Connection(Resource, Operation):
    """A resource for database/service connection credentials.

    Like every ``Resource``, connection values can be loaded from
    environment variables, .env files, or passed directly::

        class MyConnection(Connection):
            host: str = "localhost"
            port: int = 5432
            username: str
            password: str

        # Loads USERNAME, PASSWORD from environment if not passed explicitly
        connection = MyConnection()

    Connections that support OAuth subclass ``OAuthConnection`` (custom token
    shape) or ``RefreshTokenOAuthConnection`` (standard refresh-token trio);
    the ``oauth`` config lives there, not on this base.
    """

    kind: ClassVar[str] = "connection"
    tags: ClassVar[list[str]] = []
    state_model: ClassVar[type[BaseModel] | None] = ConnectionState
    billable: ClassVar[bool] = False
    capture_traceback: ClassVar[bool] = False
    renewal_interval: ClassVar[dt.timedelta] = dt.timedelta(days=1)

    auto_renew: bool = Field(
        default=True,
        title="Automatic renewal",
        description="Renew this connection's credentials on a schedule",
        json_schema_extra={"x-public": True},
    )

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

    def renew(self) -> Renewal:
        """Renew this connection's credentials before they age out.

        Override in a subclass (sync or ``async``) to exchange the stored
        credential for a fresh one — same execution contract as
        :meth:`check`: lightweight HTTP (``httpx``), never a heavy provider
        SDK. OAuth connections need no override: ``OAuthConnection`` derives
        the whole flow from the provider and the ``oauth.fields`` mapping.

        The renewal pipeline persists the returned :class:`Renewal.fields`
        into the stored config and schedules the next renewal; a raised
        exception marks the renewal failed (surfaced on the connection's
        state), typically meaning the credential is dead and a human must
        re-consent.
        """
        raise NotImplementedError

    @classmethod
    def renewable(cls) -> bool:
        """Whether this connection class implements :meth:`renew`.

        Returns:
            True when the class overrides the base hook.
        """
        return cls.renew is not Connection.renew

    @staticmethod
    def renewal_failure_message(error: Exception) -> str:
        """Describe a failed renewal in terms that are safe to persist.

        Provider token exchanges carry credentials in URLs and bodies, and
        httpx error strings embed the request URL — so raw messages must
        never reach the connection's state or the run's error event.
        HTTP-layer failures collapse to their category; anything else formats
        through :func:`~interloper.errors.format_exception` (which already
        strips pydantic input values).

        Args:
            error: The exception :meth:`renew` raised.

        Returns:
            A short, curated message.
        """
        if isinstance(error, httpx.HTTPStatusError):
            return f"The provider rejected the renewal (HTTP {error.response.status_code})."
        if isinstance(error, (httpx.TimeoutException, TimeoutError, asyncio.TimeoutError)):
            return "The renewal timed out."
        if isinstance(error, httpx.TransportError):
            return "Network error during renewal."
        return format_exception(error)

    async def execute(self, context: OperationContext) -> OperationResult:
        """Renew this connection's credentials: the connection's operation.

        The template over :meth:`renew`: caps the exchange at
        ``_RENEWAL_TIMEOUT``, returns any rotated credential fields as
        config effects, and stamps the next due time — half the reported
        validity when the provider gives one (floored at 15 minutes), the
        class's ``renewal_interval`` otherwise.

        Args:
            context: The facts this execution is scoped to, unused — a
                renewal needs nothing beyond the connection itself.

        Returns:
            The renewal's effects.
        """
        renewal = cast(Renewal, await asyncio.wait_for(invoke(self.renew), timeout=_RENEWAL_TIMEOUT))
        now = dt.datetime.now(dt.timezone.utc)
        if renewal.expires_in:
            due = now + max(dt.timedelta(seconds=renewal.expires_in / 2), dt.timedelta(minutes=15))
        else:
            due = now + type(self).renewal_interval
        return OperationResult(
            config=dict(renewal.fields),
            state={
                "next_renewal_at": due.isoformat(),
                "last_renewed_at": now.isoformat(),
                "last_renewal_error": None,
            },
        )

    def failure(self, error: Exception) -> OperationResult:
        """Describe a failed renewal: a curated message plus a retry slot.

        The message comes from :meth:`renewal_failure_message`, so raw
        provider errors (which embed credentials in URLs) never reach the
        connection's state or the run's failure event; the retry slot makes
        the connection due again without waiting a full interval.

        Args:
            error: The exception :meth:`execute` raised.

        Returns:
            A failed result stamping the curated error and the retry slot.
        """
        message = self.renewal_failure_message(error)
        retry_at = dt.datetime.now(dt.timezone.utc) + _RENEWAL_RETRY_INTERVAL
        return OperationResult(
            error=message,
            state={"next_renewal_at": retry_at.isoformat(), "last_renewal_error": message},
        )

    @classmethod
    def definition(cls) -> ResourceDefinition:
        """Advertise :meth:`check` / :meth:`renew` support so UIs can offer them.

        Returns:
            The resource definition with ``checkable`` and ``renewable`` set.
            ``auto_renew`` is dropped from the config schema when the class
            has nothing to renew — the toggle would be inert.
        """
        definition = super().definition()
        definition.checkable = cls.checkable()
        definition.renewable = cls.renewable()
        if not definition.renewable:
            definition.config_schema.get("properties", {}).pop("auto_renew", None)
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

        Args:
            suffix: Credential field suffix — ``CLIENT_ID``, ``CLIENT_SECRET``
                or ``REDIRECT_URI``; case-insensitive.

        Returns:
            The env credential value, or ``None``.
        """
        if not isinstance(cls.oauth, OAuthConfig):
            return None
        return os.environ.get(OAuthAppCredentials.env_name(cls.oauth.provider, suffix))

    @staticmethod
    def resolve_field(data: dict[str, Any], field: str, value: str | None) -> None:
        """Fill ``data[field]`` from ``value`` when the field is blank and value is set.

        A ``mode="before"`` ``resolve_credentials`` validator uses this per field.
        Leaves the field absent when there is no value, so a required field fails
        with "Field required" rather than being satisfied by an empty value.

        Args:
            data: The raw pre-validation input, mutated in place.
            field: Name of the model field to fill.
            value: The value to place, or ``None`` when nothing was resolved.
        """
        if value and not data.get(field):
            data[field] = value

    @classmethod
    def renewable(cls) -> bool:
        """Whether this connection can renew — derived, never hand-written.

        A bespoke ``renew`` override always counts. Otherwise renewability
        follows from the declarations alone: the registered provider must
        have a refresh flow (``supports_refresh``) and ``oauth.fields`` must
        name a connection field for each of the grant's credential roles.
        A qualifying connection renews with zero renewal code, and one whose
        provider has no refresh flow (TikTok: tokens do not expire) is
        excluded automatically.

        Returns:
            True when renewal is overridden or fully derivable.
        """
        if cls.renew is not OAuthConnection.renew:
            return True
        if not isinstance(cls.oauth, OAuthConfig):
            return False
        spec = PROVIDERS.get(cls.oauth.provider)
        if spec is None or not spec.supports_refresh:
            return False
        return {"client_id", "client_secret", "refresh_token"} <= set(cls.oauth.fields)

    async def renew(self) -> Renewal:
        """Renew the stored credential through the provider's refresh flow.

        One implementation serves every OAuth connection, driven entirely by
        what is already declared: the registered provider builds the grant
        and parses its response (dialect included — Facebook's
        ``fb_exchange_token``, Microsoft's scoped refresh), and
        ``oauth.fields`` names which connection fields play the grant's
        credential roles, the rotated credential landing back in the field
        playing the refresh-token role. Renewing keeps inactivity-expiring
        credentials alive and surfaces a dead one as a failure instead of a
        later run error.

        Returns:
            The rotated credential field when the provider issued a new one,
            and the credential's validity when reported.

        Raises:
            NotImplementedError: When the connection is not renewable (see
                :meth:`renewable`).
        """
        if not (self.renewable() and isinstance(self.oauth, OAuthConfig)):
            raise NotImplementedError(f"{type(self).__name__} has no renewal flow to derive")
        spec = PROVIDERS[self.oauth.provider]
        token_field = self.oauth.fields["refresh_token"]
        refresh_token = getattr(self, token_field)

        request = spec.refresh_token_request(
            client_id=getattr(self, self.oauth.fields["client_id"]),
            client_secret=getattr(self, self.oauth.fields["client_secret"]),
            refresh_token=refresh_token,
            scope=self.oauth.scope,
        )
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            response = await client.send(request)
        response.raise_for_status()
        parsed = spec.parse_refresh_token_response(response.json())

        rotated = parsed.refresh_token
        fields = {token_field: rotated} if rotated and rotated != refresh_token else {}
        return Renewal(fields=fields, expires_in=parsed.expires_in)


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

        Args:
            data: The raw pre-validation input; only a ``dict`` is augmented,
                anything else passes through untouched.

        Returns:
            The (possibly augmented) input data.
        """
        if isinstance(data, dict):
            cls.resolve_field(data, "client_id", cls.env_credential("CLIENT_ID"))
            cls.resolve_field(data, "client_secret", cls.env_credential("CLIENT_SECRET"))
        return data

