"""OAuth2 token exchange routes.

Providers come from the core registry (``interloper.oauth``): each
``OAuthProvider`` carries a declarative token-exchange spec (URL, method,
encoding, parameter names), so the exchange is performed generically —
adding a provider is an ``interloper.oauth_providers`` entry point, not a
new route.

The in-house *OAuth* credentials (``client_id`` / ``client_secret`` /
``redirect_uri``) are read from provider-scoped environment variables
(``INTERLOPER_<PROVIDER>_CLIENT_ID``, …) and used to perform the exchange. They are
never returned to the browser; connections resolve them from the same env
at runtime (see ``OAuthCredentialField``).

The ``GET /providers`` endpoint returns metadata for all providers that
have credentials configured, so the frontend knows which "Sign in with X"
buttons to render.
"""

from __future__ import annotations

import base64
import logging
import os
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException
from interloper.oauth import PROVIDERS, OAuthAppCredentials, OAuthProvider
from interloper_db import Profile
from pydantic import BaseModel

from interloper_api.dependencies import get_current_user

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/oauth", tags=["oauth"])


def log_provider_status() -> None:
    """Log which OAuth providers are usable, warning on partial credential trios.

    Called at app startup: a provider with only some of its three env vars set
    is invisible everywhere else (it simply isn't offered), so this warning is
    the one place a typo'd or forgotten variable surfaces.
    """
    active = []
    for key in PROVIDERS:
        names = OAuthAppCredentials.env_names(key)
        missing = [n for n in names.values() if not os.environ.get(n)]
        if not missing:
            active.append(key)
        elif len(missing) < len(names):
            logger.warning("OAuth provider '%s' is partially configured — missing %s", key, ", ".join(sorted(missing)))
    logger.info("OAuth sign-in providers: %s", ", ".join(sorted(active)) if active else "none configured")


# -- Generic token exchange ----------------------------------------------------


async def _exchange(
    client: httpx.AsyncClient,
    spec: OAuthProvider,
    config: OAuthAppCredentials,
    code: str,
) -> dict[str, Any]:
    """Exchange an authorization code for tokens, driven by the provider spec.

    Args:
        client: The HTTP client to use.
        spec: The provider's token-exchange spec.
        config: The in-house OAuth credentials.
        code: The authorization code.

    Returns:
        The provider's raw token response (e.g. ``refresh_token``). The OAuth
        credentials are *not* injected: they are the in-house per-provider
        values resolved from env at runtime, so the secret never leaves the
        server.
    """
    logical_values = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": config.redirect_uri,
        "client_id": config.client_id,
        "client_secret": config.client_secret,
    }
    params = {wire: logical_values[logical] for logical, wire in spec.token_params.items()}

    headers: dict[str, str] = {}
    if spec.token_basic_auth:
        creds = base64.b64encode(f"{config.client_id}:{config.client_secret}".encode()).decode()
        headers["Authorization"] = f"Basic {creds}"

    if spec.token_method == "get":
        response = await client.get(spec.token_url, params=params, headers=headers)
    elif spec.token_encoding == "form":
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        response = await client.post(spec.token_url, data=params, headers=headers)
    else:
        response = await client.post(spec.token_url, json=params, headers=headers)
    response.raise_for_status()

    return response.json()


# -- Routes --------------------------------------------------------------------


class TokenExchangeRequest(BaseModel):
    """Request body for exchanging an authorization code for tokens."""

    code: str


class ProviderInfo(BaseModel):
    """Public provider metadata (no secrets)."""

    key: str
    client_id: str
    redirect_uri: str
    auth_url: str = ""
    label: str = ""
    icon: str = ""


@router.get("/providers")
def list_providers() -> list[ProviderInfo]:
    """Return metadata for all configured OAuth providers.

    Only registered providers with ``CLIENT_ID``, ``CLIENT_SECRET``, and
    ``REDIRECT_URI`` environment variables set are included.  Metadata
    (auth_url, label, icon) comes from the provider registry.

    Returns:
        One entry per configured provider, carrying the public half of its
        credentials (client id and redirect URI) and its display metadata.
    """
    return [
        ProviderInfo(
            key=key,
            client_id=creds.client_id,
            redirect_uri=creds.redirect_uri,
            auth_url=spec.auth_url,
            label=spec.label,
            icon=spec.icon,
        )
        for key, spec in PROVIDERS.items()
        if (creds := OAuthAppCredentials.from_env(key)) is not None
    ]


@router.post("/{provider}")
async def exchange_token(
    provider: str,
    body: TokenExchangeRequest,
    _user: Profile = Depends(get_current_user),
) -> dict[str, Any]:
    """Exchange an authorization code for tokens. Requires authentication.

    Returns only the provider's token response (e.g. ``refresh_token``); the
    in-house OAuth credentials are never included — connections resolve them
    from env at runtime.

    Args:
        provider: The registry key of the provider to exchange against.
        body: The authorization code returned by the provider's consent screen.
        _user: The authenticated caller; the route is gated on a session, the
            identity itself is not used.

    Returns:
        The provider's raw token response.

    Raises:
        HTTPException: 400 when the provider is unknown or has no credentials
            configured, 500 when the exchange itself fails.
    """
    spec = PROVIDERS.get(provider)
    if spec is None:
        raise HTTPException(status_code=400, detail=f"Unknown OAuth provider: {provider}")

    config = OAuthAppCredentials.from_env(provider)
    if config is None:
        raise HTTPException(status_code=400, detail=f"OAuth provider {provider} is not configured")

    try:
        logger.info("Exchanging auth code for provider %s", provider)
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            result = await _exchange(client, spec, config, body.code)
        logger.info("Successfully exchanged auth code for provider %s", provider)
        return result
    except httpx.HTTPStatusError as exception:
        detail = exception.response.text
        logger.error("Token exchange failed for %s: %s %s", provider, exception.response.status_code, detail)
        raise HTTPException(status_code=500, detail=f"Failed to exchange auth code: {detail}")
    except Exception as exception:  # noqa: BLE001 - any provider failure becomes a 500
        logger.error("Token exchange failed for %s: %s", provider, exception)
        raise HTTPException(status_code=500, detail=f"Failed to exchange auth code: {exception}")
