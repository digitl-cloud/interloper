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
from interloper.oauth import OAuthProvider, providers
from interloper_db import Profile
from pydantic import BaseModel

from interloper_api.dependencies import get_current_user

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/oauth", tags=["oauth"])


# ---------------------------------------------------------------------------
# App credentials (environment)
# ---------------------------------------------------------------------------


class _ProviderConfig:
    """In-house OAuth credentials resolved from environment variables."""

    def __init__(self, key: str, *, env_prefix: str | None = None) -> None:
        prefix = (env_prefix or key).upper()
        self.key = key
        self.client_id = os.environ.get(f"INTERLOPER_{prefix}_CLIENT_ID", "")
        self.client_secret = os.environ.get(f"INTERLOPER_{prefix}_CLIENT_SECRET", "")
        self.redirect_uri = os.environ.get(f"INTERLOPER_{prefix}_REDIRECT_URI", "")

    @property
    def configured(self) -> bool:
        return bool(self.client_id and self.client_secret and self.redirect_uri)


def _load_providers() -> dict[str, _ProviderConfig]:
    return {key: _ProviderConfig(key) for key in providers()}


# ---------------------------------------------------------------------------
# Generic token exchange
# ---------------------------------------------------------------------------


async def _exchange(
    client: httpx.AsyncClient,
    spec: OAuthProvider,
    cfg: _ProviderConfig,
    code: str,
) -> dict[str, Any]:
    """Exchange an authorization code for tokens, driven by the provider spec.

    Args:
        client: The HTTP client to use.
        spec: The provider's token-exchange spec.
        cfg: The in-house OAuth credentials.
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
        "redirect_uri": cfg.redirect_uri,
        "client_id": cfg.client_id,
        "client_secret": cfg.client_secret,
    }
    params = {wire: logical_values[logical] for logical, wire in spec.token_params.items()}

    headers: dict[str, str] = {}
    if spec.token_basic_auth:
        creds = base64.b64encode(f"{cfg.client_id}:{cfg.client_secret}".encode()).decode()
        headers["Authorization"] = f"Basic {creds}"

    if spec.token_method == "get":
        resp = await client.get(spec.token_url, params=params, headers=headers)
    elif spec.token_encoding == "form":
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        resp = await client.post(spec.token_url, data=params, headers=headers)
    else:
        resp = await client.post(spec.token_url, json=params, headers=headers)
    resp.raise_for_status()

    return resp.json()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


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
    """
    specs = providers()
    return [
        ProviderInfo(
            key=cfg.key,
            client_id=cfg.client_id,
            redirect_uri=cfg.redirect_uri,
            auth_url=specs[cfg.key].auth_url,
            label=specs[cfg.key].label,
            icon=specs[cfg.key].icon,
        )
        for cfg in _load_providers().values()
        if cfg.configured
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
    """
    spec = providers().get(provider)
    if spec is None:
        raise HTTPException(status_code=400, detail=f"Unknown OAuth provider: {provider}")

    cfg = _ProviderConfig(provider)
    if not cfg.configured:
        raise HTTPException(status_code=400, detail=f"OAuth provider {provider} is not configured")

    try:
        logger.info("Exchanging auth code for provider %s", provider)
        async with httpx.AsyncClient(timeout=30) as client:
            result = await _exchange(client, spec, cfg, body.code)
        logger.info("Successfully exchanged auth code for provider %s", provider)
        return result
    except httpx.HTTPStatusError as exc:
        detail = exc.response.text
        logger.error("Token exchange failed for %s: %s %s", provider, exc.response.status_code, detail)
        raise HTTPException(status_code=500, detail=f"Failed to exchange auth code: {detail}")
    except Exception as exc:
        logger.error("Token exchange failed for %s: %s", provider, exc)
        raise HTTPException(status_code=500, detail=f"Failed to exchange auth code: {exc}")
