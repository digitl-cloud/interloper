"""OAuthProvider: identity and token-exchange spec, plus the provider registry.

A provider describes everything interloper needs to drive an OAuth2
authorization-code flow for a third-party service: where to send the user
(``auth_url``), where to exchange the code (``token_url``), and how that
exchange request is shaped.

Connections reference providers by key through
:class:`~interloper.oauth.config.OAuthConfig`, which resolves display
metadata (auth_url, label, icon) from the registry; the API drives the
token exchange generically from the same spec.

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
from dataclasses import dataclass, field
from typing import Any, Literal

from interloper.registry import Registry

# -- Provider spec -------------------------------------------------------------


# Logical token-exchange parameters.  ``OAuthProvider.token_params`` maps
# each logical name to its wire parameter name; logical names absent from
# the mapping are omitted from the exchange request entirely.
DEFAULT_TOKEN_PARAMS: dict[str, str] = {
    "grant_type": "grant_type",
    "code": "code",
    "redirect_uri": "redirect_uri",
    "client_id": "client_id",
    "client_secret": "client_secret",
}


def token_params(*omit: str, **rename: str) -> dict[str, str]:
    """Build a token_params mapping from the defaults.

    Args:
        *omit: Logical parameter names to drop from the request.
        **rename: Logical name → wire name overrides.

    Returns:
        A logical → wire parameter name mapping.
    """
    params = {k: v for k, v in DEFAULT_TOKEN_PARAMS.items() if k not in omit}
    params.update(rename)
    return params


@dataclass(frozen=True)
class OAuthProvider:
    """Identity and token-exchange spec for an OAuth2 provider.

    Args:
        key: Provider key (e.g. ``"amazon"``) — the registry key.
        auth_url: Authorization endpoint the user is sent to.
        token_url: Token endpoint the authorization code is exchanged at.
        label: Display label (defaults to the titlecased key).
        icon: Icon identifier (e.g. ``"logos:facebook"``).
        token_method: HTTP method of the exchange request.
        token_encoding: Body encoding for POST exchanges (GET always
            sends query params).
        token_params: Logical → wire parameter names.  Logical names
            absent from the mapping are not sent (e.g. TikTok takes no
            ``grant_type`` or ``redirect_uri``).
        token_basic_auth: Also send the client credentials as an HTTP
            Basic ``Authorization`` header.
    """

    key: str
    auth_url: str
    token_url: str
    label: str = ""
    icon: str = ""
    token_method: Literal["post", "get"] = "post"
    token_encoding: Literal["json", "form"] = "json"
    token_params: dict[str, str] = field(default_factory=lambda: dict(DEFAULT_TOKEN_PARAMS))
    token_basic_auth: bool = False

    def __post_init__(self) -> None:
        """Default the label to the titlecased key."""
        if not self.label:
            object.__setattr__(self, "label", self.key.title())


# -- Registry ------------------------------------------------------------------


def _adopt_provider(_name: str, loaded: Any) -> tuple[str, OAuthProvider]:
    """Instantiate a loaded provider entry and key it by its own ``key``.

    Returns:
        The ``(key, provider)`` pair.
    """
    instance: OAuthProvider = loaded() if isinstance(loaded, type) else loaded
    return instance.key, instance


PROVIDERS: Registry[OAuthProvider] = Registry("interloper.oauth_providers", adopt=_adopt_provider)


# -- In-house app credentials (environment) -------------------------------------

#: Credential field → env var suffix. The trio a provider needs for sign-in.
_ENV_FIELDS: dict[str, str] = {
    "client_id": "CLIENT_ID",
    "client_secret": "CLIENT_SECRET",
    "redirect_uri": "REDIRECT_URI",
}


def provider_env_name(key: str, suffix: str) -> str:
    """The environment variable carrying one in-house credential for provider ``key``.

    Single owner of the ``INTERLOPER_<PROVIDER>_<SUFFIX>`` naming convention —
    every consumer (token exchange, connection credential injection,
    availability checks) builds names through here.

    Returns:
        The environment variable name.
    """
    return f"INTERLOPER_{key.upper()}_{suffix.upper()}"


def provider_env_names(key: str) -> dict[str, str]:
    """The environment variable carrying each credential field for provider ``key``.

    Returns:
        ``{field: env_name}`` for the ``client_id`` / ``client_secret`` /
        ``redirect_uri`` trio.
    """
    return {field: provider_env_name(key, suffix) for field, suffix in _ENV_FIELDS.items()}


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

    @classmethod
    def from_env(cls, key: str) -> OAuthAppCredentials | None:
        """Resolve the trio for provider ``key`` from the environment.

        Returns:
            The credentials, or ``None`` unless all three variables are set
            and non-empty.
        """
        names = provider_env_names(key)
        client_id = os.environ.get(names["client_id"])
        client_secret = os.environ.get(names["client_secret"])
        redirect_uri = os.environ.get(names["redirect_uri"])
        if not (client_id and client_secret and redirect_uri):
            return None
        return cls(client_id=client_id, client_secret=client_secret, redirect_uri=redirect_uri)


def is_provider_configured(key: str) -> bool:
    """Whether the in-house OAuth app credentials for ``key`` are set in the environment.

    Returns:
        True only when the full credential trio is set — the provider is
        usable for sign-in.
    """
    return OAuthAppCredentials.from_env(key) is not None
