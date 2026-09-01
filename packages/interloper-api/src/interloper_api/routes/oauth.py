"""OAuth2 token exchange routes.

Providers come from the core registry (``interloper.oauth``): each
``OAuthProvider`` builds its own token requests (its dialect included), so
the exchange is performed generically — adding a provider is an
``interloper.oauth_providers`` entry point, not a new route.

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

import logging
import os
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException
from interloper.oauth import PROVIDERS, OAuthAppCredentials
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


# -- Routes --------------------------------------------------------------------


class AuthorizationCodeExchangeRequest(BaseModel):
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
async def exchange_authorization_code(
    provider: str,
    body: AuthorizationCodeExchangeRequest,
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
        request = spec.authorization_code_request(
            code=body.code,
            redirect_uri=config.redirect_uri,
            client_id=config.client_id,
            client_secret=config.client_secret,
        )
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            response = await client.send(request)
        response.raise_for_status()
        logger.info("Successfully exchanged auth code for provider %s", provider)
        return response.json()
    except httpx.HTTPStatusError as exception:
        detail = exception.response.text
        logger.error("Token exchange failed for %s: %s %s", provider, exception.response.status_code, detail)
        raise HTTPException(status_code=500, detail=f"Failed to exchange auth code: {detail}")
    except Exception as exception:  # noqa: BLE001 - any provider failure becomes a 500
        logger.error("Token exchange failed for %s: %s", provider, exception)
        raise HTTPException(status_code=500, detail=f"Failed to exchange auth code: {exception}")
