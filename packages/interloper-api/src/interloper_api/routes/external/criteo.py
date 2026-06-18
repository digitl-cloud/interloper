"""Criteo external API routes."""

from __future__ import annotations

import httpx
from fastapi import APIRouter, Depends
from interloper_db import Profile
from pydantic import BaseModel

from interloper_api.dependencies import require_viewer
from interloper_api.routes.external import handle_error

sub_router = APIRouter()

_BASE_URL = "https://api.criteo.com"
_TOKEN_URL = f"{_BASE_URL}/oauth2/token"
_ADVERTISERS_URL = f"{_BASE_URL}/2025-04/advertisers/me"


class CriteoConnectionRequest(BaseModel):
    """Criteo connection credentials (matches CriteoConnection fields)."""

    client_id: str
    client_secret: str
    refresh_token: str


async def _get_access_token(client: httpx.AsyncClient, body: CriteoConnectionRequest) -> str:
    """Exchange the refresh token for an access token."""
    resp = await client.post(
        _TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "client_id": body.client_id,
            "client_secret": body.client_secret,
            "refresh_token": body.refresh_token,
        },
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


async def _list_advertisers(client: httpx.AsyncClient, body: CriteoConnectionRequest) -> list[dict[str, str]]:
    """List the advertisers the connection can access, sorted by name."""
    token = await _get_access_token(client, body)
    resp = await client.get(_ADVERTISERS_URL, headers={"Authorization": f"Bearer {token}"})
    resp.raise_for_status()

    advertisers = resp.json().get("data", [])
    results = [
        {"id": a["id"], "name": a.get("attributes", {}).get("advertiserName") or a["id"]}
        for a in advertisers
    ]
    return sorted(results, key=lambda a: a["name"].lower())


@sub_router.post("/criteo/advertisers")
async def criteo_advertisers(
    body: CriteoConnectionRequest,
    _user: Profile = Depends(require_viewer),
) -> list[dict[str, str]]:
    """Fetch the Criteo advertisers accessible by the connection."""
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            return await _list_advertisers(client, body)
    except Exception as exc:
        handle_error(exc, "fetching Criteo advertisers")
        return []
