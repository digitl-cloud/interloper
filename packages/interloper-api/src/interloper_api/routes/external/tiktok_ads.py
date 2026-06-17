"""TikTok Ads external API routes."""

from __future__ import annotations

import os

import httpx
from fastapi import APIRouter, Depends, HTTPException
from interloper_db import Profile
from pydantic import BaseModel

from interloper_api.dependencies import require_viewer
from interloper_api.routes.external import handle_error

sub_router = APIRouter()

_BASE_URL = "https://business-api.tiktok.com/open_api/v1.3"
_ADVERTISERS_URL = f"{_BASE_URL}/oauth2/advertiser/get/"


class TiktokAdsConnectionRequest(BaseModel):
    """TikTok Ads connection credentials (matches TiktokAdsConnection fields)."""

    access_token: str


async def _list_advertisers(
    client: httpx.AsyncClient,
    access_token: str,
    app_id: str,
    secret: str,
) -> list[dict[str, str]]:
    """List the advertisers the access token can access, sorted by name."""
    resp = await client.get(
        _ADVERTISERS_URL,
        params={"app_id": app_id, "secret": secret},
        headers={"Access-Token": access_token},
    )
    resp.raise_for_status()
    body = resp.json()
    if body.get("code") != 0:
        raise HTTPException(status_code=502, detail=f"TikTok API error: {body.get('message')}")

    advertisers = body.get("data", {}).get("list", [])
    results = [
        {"advertiser_id": a["advertiser_id"], "name": a.get("advertiser_name") or a["advertiser_id"]}
        for a in advertisers
    ]
    return sorted(results, key=lambda a: a["name"].lower())


@sub_router.post("/tiktok-ads/advertisers")
async def tiktok_ads_advertisers(
    body: TiktokAdsConnectionRequest,
    _user: Profile = Depends(require_viewer),
) -> list[dict[str, str]]:
    """Fetch the TikTok advertisers accessible by the connection.

    Listing advertisers needs the connector app's ``app_id`` / ``secret`` — read
    from the provider-scoped environment (``TIKTOK_CLIENT_ID`` /
    ``TIKTOK_CLIENT_SECRET``) — alongside the connection's access token.
    """
    try:
        app_id = os.environ.get("TIKTOK_CLIENT_ID", "")
        secret = os.environ.get("TIKTOK_CLIENT_SECRET", "")
        async with httpx.AsyncClient(timeout=30) as client:
            return await _list_advertisers(client, body.access_token, app_id, secret)
    except Exception as exc:
        handle_error(exc, "fetching TikTok Ads advertisers")
        return []
