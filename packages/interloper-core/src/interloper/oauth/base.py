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

from dataclasses import dataclass, field
from functools import cache
from importlib.metadata import entry_points
from typing import Literal

from interloper.errors import ConfigError

_ENTRY_POINT = "interloper.oauth_providers"


# ------------------------------------------------------------------
# Provider spec
# ------------------------------------------------------------------


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


# ------------------------------------------------------------------
# Registry
# ------------------------------------------------------------------


@cache
def providers() -> dict[str, OAuthProvider]:
    """Load the OAuth provider registry from installed entry points.

    Every provider — including the built-ins — registers through the
    ``interloper.oauth_providers`` entry-point group; the provider's
    ``key`` is the registry key. Installed means discovered: no
    import-order dependence, and a new provider is one new package with
    one entry point.

    Returns:
        Mapping of provider key to provider spec.
    """
    registry: dict[str, OAuthProvider] = {}
    for entry_point in entry_points(group=_ENTRY_POINT):
        loaded = entry_point.load()
        instance: OAuthProvider = loaded() if isinstance(loaded, type) else loaded
        registry[instance.key] = instance
    return registry


def provider(key: str) -> OAuthProvider:
    """Return the OAuth provider registered under *key*.

    Returns:
        The registered provider spec.

    Raises:
        ConfigError: If no provider is registered under *key*.
    """
    registry = providers()
    if key not in registry:
        raise ConfigError(
            f"Unknown OAuth provider: {key!r} (available: {sorted(registry)}). "
            f"Is the matching interloper package installed?"
        )
    return registry[key]
