"""OAuth2 token exchange routes.

Each provider has an explicit exchange function with its own URL,
HTTP method, body encoding, and parameter names.  The ``client_id``
and ``client_secret`` are read from settings (:attr:`AppSettings.oauth`)
and injected into the token response so they can be stored alongside
the tokens.

The ``GET /providers`` endpoint returns metadata for all providers
that have credentials configured, so the frontend knows which
"Sign in with X" buttons to render.
"""

from __future__ import annotations

import base64
import logging
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException
from interloper.settings import AppSettings, OAuthSettings
from pydantic import BaseModel

from interloper_api.dependencies import get_catalog

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/oauth", tags=["oauth"])


# ---------------------------------------------------------------------------
# Provider registry
# ---------------------------------------------------------------------------


class _ProviderConfig:
    """Runtime provider config resolved from :class:`OAuthSettings`."""

    def __init__(self, key: str, oauth: OAuthSettings) -> None:
        self.key = key
        self.client_id = getattr(oauth, f"{key}_client_id", "")
        self.client_secret = getattr(oauth, f"{key}_client_secret", "")
        self.redirect_uri = getattr(oauth, f"{key}_redirect_uri", "")

    @property
    def configured(self) -> bool:
        return bool(self.client_id and self.client_secret and self.redirect_uri)


_PROVIDER_KEYS = [
    "amazon",
    "criteo",
    "facebook",
    "google",
    "linkedin",
    "microsoft",
    "pinterest",
    "snapchat",
    "tiktok",
]


def _load_providers() -> dict[str, _ProviderConfig]:
    oauth = AppSettings.get().oauth
    return {k: _ProviderConfig(k, oauth) for k in _PROVIDER_KEYS}


# ---------------------------------------------------------------------------
# Token exchange functions (one per provider)
# ---------------------------------------------------------------------------


async def _amazon(client: httpx.AsyncClient, cfg: _ProviderConfig, code: str) -> dict[str, Any]:
    resp = await client.post(
        "https://api.amazon.com/auth/o2/token",
        json={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": cfg.redirect_uri,
            "client_id": cfg.client_id,
            "client_secret": cfg.client_secret,
        },
    )
    resp.raise_for_status()
    result = resp.json()
    result["client_id"] = cfg.client_id
    result["client_secret"] = cfg.client_secret
    return result


async def _criteo(client: httpx.AsyncClient, cfg: _ProviderConfig, code: str) -> dict[str, Any]:
    resp = await client.post(
        "https://api.criteo.com/oauth2/token",
        json={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": cfg.redirect_uri,
            "client_id": cfg.client_id,
            "client_secret": cfg.client_secret,
        },
    )
    resp.raise_for_status()
    result = resp.json()
    result["client_id"] = cfg.client_id
    result["client_secret"] = cfg.client_secret
    return result


async def _facebook(client: httpx.AsyncClient, cfg: _ProviderConfig, code: str) -> dict[str, Any]:
    resp = await client.get(
        "https://graph.facebook.com/v19.0/oauth/access_token",
        params={
            "code": code,
            "redirect_uri": cfg.redirect_uri,
            "client_id": cfg.client_id,
            "client_secret": cfg.client_secret,
        },
    )
    resp.raise_for_status()
    result = resp.json()
    result["client_id"] = cfg.client_id
    result["client_secret"] = cfg.client_secret
    return result


async def _google(client: httpx.AsyncClient, cfg: _ProviderConfig, code: str) -> dict[str, Any]:
    resp = await client.post(
        "https://oauth2.googleapis.com/token",
        json={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": cfg.redirect_uri,
            "client_id": cfg.client_id,
            "client_secret": cfg.client_secret,
        },
    )
    resp.raise_for_status()
    result = resp.json()
    result["client_id"] = cfg.client_id
    result["client_secret"] = cfg.client_secret
    return result


async def _linkedin(client: httpx.AsyncClient, cfg: _ProviderConfig, code: str) -> dict[str, Any]:
    resp = await client.post(
        "https://www.linkedin.com/oauth/v2/accessToken",
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": cfg.redirect_uri,
            "client_id": cfg.client_id,
            "client_secret": cfg.client_secret,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    resp.raise_for_status()
    result = resp.json()
    result["client_id"] = cfg.client_id
    result["client_secret"] = cfg.client_secret
    return result


async def _microsoft(client: httpx.AsyncClient, cfg: _ProviderConfig, code: str) -> dict[str, Any]:
    resp = await client.post(
        "https://login.microsoftonline.com/common/oauth2/v2.0/token",
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": cfg.redirect_uri,
            "client_id": cfg.client_id,
            "client_secret": cfg.client_secret,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    resp.raise_for_status()
    result = resp.json()
    result["client_id"] = cfg.client_id
    result["client_secret"] = cfg.client_secret
    return result


async def _pinterest(client: httpx.AsyncClient, cfg: _ProviderConfig, code: str) -> dict[str, Any]:
    creds = base64.b64encode(f"{cfg.client_id}:{cfg.client_secret}".encode()).decode()
    resp = await client.post(
        "https://api.pinterest.com/v5/oauth/token",
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": cfg.redirect_uri,
            "client_id": cfg.client_id,
            "client_secret": cfg.client_secret,
        },
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {creds}",
        },
    )
    resp.raise_for_status()
    result = resp.json()
    result["client_id"] = cfg.client_id
    result["client_secret"] = cfg.client_secret
    return result


async def _snapchat(client: httpx.AsyncClient, cfg: _ProviderConfig, code: str) -> dict[str, Any]:
    resp = await client.post(
        "https://accounts.snapchat.com/login/oauth2/access_token",
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": cfg.redirect_uri,
            "client_id": cfg.client_id,
            "client_secret": cfg.client_secret,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    resp.raise_for_status()
    result = resp.json()
    result["client_id"] = cfg.client_id
    result["client_secret"] = cfg.client_secret
    return result


async def _tiktok(client: httpx.AsyncClient, cfg: _ProviderConfig, code: str) -> dict[str, Any]:
    resp = await client.post(
        "https://business-api.tiktok.com/open_api/v1.3/oauth2/access_token",
        json={
            "auth_code": code,
            "app_id": cfg.client_id,
            "secret": cfg.client_secret,
        },
    )
    resp.raise_for_status()
    result = resp.json()
    result["client_id"] = cfg.client_id
    result["client_secret"] = cfg.client_secret
    return result


_EXCHANGE_FNS: dict[str, Any] = {
    "amazon": _amazon,
    "criteo": _criteo,
    "facebook": _facebook,
    "google": _google,
    "linkedin": _linkedin,
    "microsoft": _microsoft,
    "pinterest": _pinterest,
    "snapchat": _snapchat,
    "tiktok": _tiktok,
}


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


def _extract_oauth_metadata(catalog: dict[str, Any]) -> dict[str, dict[str, str]]:
    """Extract OAuth metadata (auth_url, label, icon) from catalog definitions.

    Scans all resource definitions for ``x-oauth`` config schema extensions.
    Returns a dict keyed by provider key.
    """
    metadata: dict[str, dict[str, str]] = {}
    for defn in catalog.values():
        oauth_ext = defn.get("config_schema", {}).get("x-oauth")
        if oauth_ext and isinstance(oauth_ext, dict):
            provider = oauth_ext.get("provider", "")
            if provider and provider not in metadata:
                metadata[provider] = {
                    "auth_url": oauth_ext.get("auth_url", ""),
                    "label": oauth_ext.get("label", ""),
                    "icon": oauth_ext.get("icon", ""),
                }
    return metadata


@router.get("/providers")
def list_providers(catalog: dict[str, Any] = Depends(get_catalog)) -> list[ProviderInfo]:
    """Return metadata for all configured OAuth providers.

    Only providers with ``CLIENT_ID``, ``CLIENT_SECRET``, and
    ``REDIRECT_URI`` environment variables set are included.
    Metadata (auth_url, label, icon) is extracted from the catalog.
    """
    providers = _load_providers()
    oauth_meta = _extract_oauth_metadata(catalog)
    return [
        ProviderInfo(
            key=cfg.key,
            client_id=cfg.client_id,
            redirect_uri=cfg.redirect_uri,
            **(oauth_meta.get(cfg.key, {})),
        )
        for cfg in providers.values()
        if cfg.configured
    ]


@router.post("/{provider}")
async def exchange_token(provider: str, body: TokenExchangeRequest) -> dict[str, Any]:
    """Exchange an authorization code for tokens.

    The response includes ``client_id`` and ``client_secret`` so they
    can be stored alongside the tokens in the connection data.
    """
    exchange = _EXCHANGE_FNS.get(provider)
    if exchange is None:
        raise HTTPException(status_code=400, detail=f"Unknown OAuth provider: {provider}")

    providers = _load_providers()
    cfg = providers.get(provider)
    if cfg is None or not cfg.configured:
        raise HTTPException(status_code=400, detail=f"OAuth provider {provider} is not configured")

    try:
        logger.info("Exchanging auth code for provider %s", provider)
        async with httpx.AsyncClient(timeout=30) as client:
            result = await exchange(client, cfg, body.code)
        logger.info("Successfully exchanged auth code for provider %s", provider)
        return result
    except httpx.HTTPStatusError as exc:
        detail = exc.response.text
        logger.error("Token exchange failed for %s: %s %s", provider, exc.response.status_code, detail)
        raise HTTPException(status_code=500, detail=f"Failed to exchange auth code: {detail}")
    except Exception as exc:
        logger.error("Token exchange failed for %s: %s", provider, exc)
        raise HTTPException(status_code=500, detail=f"Failed to exchange auth code: {exc}")
