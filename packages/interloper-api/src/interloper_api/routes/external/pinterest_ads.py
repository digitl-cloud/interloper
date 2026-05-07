"""Pinterest Ads external API routes."""

from __future__ import annotations

import httpx
from fastapi import APIRouter, Depends
from interloper_db import Profile
from pydantic import BaseModel

from interloper_api.dependencies import require_viewer
from interloper_api.routes.external import handle_error

sub_router = APIRouter()

_BASE_URL = "https://api.pinterest.com/v5"


class PinterestAdsConnectionRequest(BaseModel):
    """Pinterest Ads connection credentials (matches PinterestAdsConnection fields)."""

    client_id: str
    client_secret: str
    refresh_token: str


async def _get_access_token(body: PinterestAdsConnectionRequest) -> str:
    """Exchange refresh token for an access token using HTTP Basic auth."""
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"{_BASE_URL}/oauth/token",
            auth=(body.client_id, body.client_secret),
            data={
                "grant_type": "refresh_token",
                "refresh_token": body.refresh_token,
            },
        )
        resp.raise_for_status()
        return resp.json()["access_token"]


@sub_router.post("/pinterest-ads/accounts")
async def pinterest_ads_accounts(
    body: PinterestAdsConnectionRequest,
    _user: Profile = Depends(require_viewer),
) -> list[dict[str, str]]:
    """Fetch Pinterest Ads ad accounts accessible by the connection."""
    try:
        token = await _get_access_token(body)
        headers = {"Authorization": f"Bearer {token}"}

        accounts: list[dict[str, str]] = []
        bookmark: str | None = None

        async with httpx.AsyncClient(timeout=30, headers=headers) as client:
            while True:
                params: dict[str, str] = {"page_size": "100"}
                if bookmark:
                    params["bookmark"] = bookmark

                resp = await client.get(f"{_BASE_URL}/ad_accounts", params=params)
                resp.raise_for_status()
                data = resp.json()

                for acct in data.get("items", []):
                    accounts.append({
                        "id": acct["id"],
                        "name": acct.get("name", acct["id"]),
                    })

                bookmark = data.get("bookmark")
                if not bookmark:
                    break

        return accounts
    except Exception as exc:
        handle_error(exc, "fetching Pinterest Ads accounts")
        return []
