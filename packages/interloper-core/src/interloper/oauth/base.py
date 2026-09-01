"""OAuthProvider: a provider's identity and token-flow dialect, plus the registry.

A provider owns everything interloper needs to drive an OAuth2
authorization-code flow for a third-party service: where to send the user
(``auth_url``), and how to build the token requests — the code exchange and
the refresh grant. The base class speaks plain RFC 6749; a provider whose
dialect deviates overrides the request builders (or response parsing) on a
subclass, so each quirk lives with the provider that has it.

Connections reference providers by key through
:class:`~interloper.oauth.config.OAuthConfig`, which resolves display
metadata (auth_url, label, icon) from the registry; the API's sign-in
exchange and connection renewal both send whatever request the provider
builds.

Every provider — including the built-ins shipped by interloper-core —
registers through the ``interloper.oauth_providers`` entry-point group::

    [project.entry-points."interloper.oauth_providers"]
    acme = "my_pkg.oauth:ACME_PROVIDER"

The registry is loaded lazily from installed-package metadata, so discovery
works in any process where the package is installed — no import-order
dependence, no explicit registration calls.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, ClassVar, Literal

import httpx

from interloper.registry import Registry

# -- Provider spec -------------------------------------------------------------


@dataclass(frozen=True)
class RefreshTokenResponse:
    """Role-level outcome of a refresh-token grant.

    Args:
        refresh_token: The refresh credential the provider issued, or ``None``
            when the response carried none (no rotation).
        expires_in: Validity of the refresh credential in seconds, when the
            provider reports it.
    """

    refresh_token: str | None = None
    expires_in: int | None = None


@dataclass(frozen=True)
class OAuthProvider:
    """Identity and token-flow dialect of an OAuth2 provider.

    The base class speaks plain RFC 6749: the authorization-code exchange
    and the refresh-token grant are POSTs to ``token_url`` with the standard
    parameters. A provider whose dialect deviates — a different method,
    parameter names, an extra parameter, another grant entirely — overrides
    the request builders (and, for renewal, the response parsing) on a
    subclass; the provider is the single owner of its dialect, and the
    sign-in exchange and connection renewal consume it identically.

    ``token_encoding`` is the one wire knob a plain instance may set, and it
    only parameterizes the default builders' body encoding. Any other
    deviation is a method override, never a new field. ``supports_refresh``
    is a class trait of the dialect: a provider with no credential-refresh
    flow at all (TikTok: tokens do not expire) sets it ``False`` on its
    subclass, and connections on it derive as non-renewable.

    Args:
        key: Provider key (e.g. ``"amazon"``) — the registry key.
        auth_url: Authorization endpoint the user is sent to.
        token_url: Token endpoint the token requests are sent to.
        label: Display label (defaults to the titlecased key).
        icon: Icon identifier (e.g. ``"logos:facebook"``).
        token_encoding: Body encoding of the default token requests.
    """

    supports_refresh: ClassVar[bool] = True

    key: str
    auth_url: str
    token_url: str
    label: str = ""
    icon: str = ""
    token_encoding: Literal["json", "form"] = "json"

    def __post_init__(self) -> None:
        """Default the label to the titlecased key."""
        if not self.label:
            object.__setattr__(self, "label", self.key.title())

    # -- Token flows -------------------------------------------------------

    def authorization_code_request(
        self,
        *,
        code: str,
        redirect_uri: str,
        client_id: str,
        client_secret: str,
    ) -> httpx.Request:
        """Build the authorization-code grant's token request (RFC 6749 §4.1.3).

        Args:
            code: The authorization code from the provider's consent screen.
            redirect_uri: The redirect URI the code was issued against.
            client_id: The OAuth app's client id.
            client_secret: The OAuth app's client secret.

        Returns:
            The request to send; the caller owns the client and error handling.
        """
        return self._token_request(
            {
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": redirect_uri,
                "client_id": client_id,
                "client_secret": client_secret,
            }
        )

    def refresh_token_request(
        self,
        *,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        scope: str | None = None,
    ) -> httpx.Request:
        """Build the refresh-token grant request (RFC 6749 §6).

        The default grant omits ``scope`` even when given: it is optional per
        the RFC and some providers reject parameters they do not document.
        A provider that requires it overrides (Microsoft: ``AADSTS90023``).

        Args:
            client_id: The OAuth app's client id.
            client_secret: The OAuth app's client secret.
            refresh_token: The credential to exchange for a fresh one.
            scope: The connection's declared scope; ignored by the default
                grant.

        Returns:
            The request to send; the caller owns the client and error handling.
        """
        return self._token_request(
            {
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": client_id,
                "client_secret": client_secret,
            }
        )

    def parse_refresh_token_response(self, payload: dict[str, Any]) -> RefreshTokenResponse:
        """Read the refresh grant's response at the credential-role level.

        The default reads the RFC shape: ``refresh_token`` is the (possibly
        rotated) credential when present, and ``refresh_token_expires_in``
        its validity — ``expires_in`` describes the access token, which
        interloper does not store.

        Args:
            payload: The grant's JSON response body.

        Returns:
            The issued refresh credential and its validity, when reported.
        """
        return RefreshTokenResponse(
            refresh_token=payload.get("refresh_token"),
            expires_in=payload.get("refresh_token_expires_in"),
        )

    def _token_request(self, params: dict[str, str]) -> httpx.Request:
        """Build a POST to the token endpoint in the provider's body encoding.

        Args:
            params: The grant parameters to carry as the request body.

        Returns:
            The built request.
        """
        if self.token_encoding == "form":
            return httpx.Request("POST", self.token_url, data=params)
        return httpx.Request("POST", self.token_url, json=params)


# -- Registry ------------------------------------------------------------------


def _adopt_provider(_name: str, loaded: Any) -> tuple[str, OAuthProvider]:
    """Instantiate a loaded provider entry and key it by its own ``key``.

    Args:
        _name: The entry-point name, ignored — the provider's own ``key`` wins.
        loaded: The loaded entry point: an ``OAuthProvider`` or a class
            constructing one.

    Returns:
        The ``(key, provider)`` pair.
    """
    instance: OAuthProvider = loaded() if isinstance(loaded, type) else loaded
    return instance.key, instance


PROVIDERS: Registry[OAuthProvider] = Registry("interloper.oauth_providers", adopt=_adopt_provider)


# -- In-house app credentials (environment) ------------------------------------

@dataclass(frozen=True)
class OAuthAppCredentials:
    """The in-house OAuth app credential trio for one provider.

    Resolved from the environment complete-or-nothing: :meth:`from_env`
    never yields a partial set, so consumers cannot observe a
    half-configured provider.
    """

    client_id: str
    client_secret: str
    redirect_uri: str

    _ENV_FIELDS: ClassVar[dict[str, str]] = {
        "client_id": "CLIENT_ID",
        "client_secret": "CLIENT_SECRET",
        "redirect_uri": "REDIRECT_URI",
    }

    @staticmethod
    def env_name(key: str, suffix: str) -> str:
        """The environment variable carrying one in-house credential for provider ``key``.

        Single owner of the ``INTERLOPER_<PROVIDER>_<SUFFIX>`` naming convention —
        every consumer (token exchange, connection credential injection,
        availability checks) builds names through here.

        Args:
            key: Provider registry key (e.g. ``"amazon"``).
            suffix: Credential field suffix — ``CLIENT_ID``, ``CLIENT_SECRET``
                or ``REDIRECT_URI``; case-insensitive.

        Returns:
            The environment variable name.
        """
        return f"INTERLOPER_{key.upper()}_{suffix.upper()}"

    @classmethod
    def env_names(cls, key: str) -> dict[str, str]:
        """The environment variable carrying each credential field for provider ``key``.

        Args:
            key: Provider registry key (e.g. ``"amazon"``).

        Returns:
            ``{field: env_name}`` for the ``client_id`` / ``client_secret`` /
            ``redirect_uri`` trio.
        """
        return {field: cls.env_name(key, suffix) for field, suffix in cls._ENV_FIELDS.items()}

    @classmethod
    def is_configured(cls, key: str) -> bool:
        """Whether the in-house OAuth app credentials for ``key`` are set in the environment.

        Args:
            key: Provider registry key (e.g. ``"amazon"``).

        Returns:
            True only when the full credential trio is set — the provider is
            usable for sign-in.
        """
        return cls.from_env(key) is not None

    @classmethod
    def from_env(cls, key: str) -> OAuthAppCredentials | None:
        """Resolve the trio for provider ``key`` from the environment.

        Args:
            key: Provider registry key (e.g. ``"amazon"``).

        Returns:
            The credentials, or ``None`` unless all three variables are set
            and non-empty.
        """
        names = cls.env_names(key)
        client_id = os.environ.get(names["client_id"])
        client_secret = os.environ.get(names["client_secret"])
        redirect_uri = os.environ.get(names["redirect_uri"])
        if not (client_id and client_secret and redirect_uri):
            return None
        return cls(client_id=client_id, client_secret=client_secret, redirect_uri=redirect_uri)
