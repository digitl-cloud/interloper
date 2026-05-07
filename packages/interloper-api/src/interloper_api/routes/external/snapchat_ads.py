"""Snapchat Ads external API routes."""

from __future__ import annotations

import httpx
from fastapi import APIRouter, Depends
from interloper_db import Profile
from pydantic import BaseModel

from interloper_api.dependencies import require_viewer
from interloper_api.routes.external import handle_error

sub_router = APIRouter()

_BASE_URL = "https://adsapi.snapchat.com/v1"
_TOKEN_URL = "https://accounts.snapchat.com/login/oauth2/access_token"


class SnapchatAdsConnectionRequest(BaseModel):
    """Snapchat Ads connection credentials (matches SnapchatAdsConnection fields)."""

    client_id: str
    client_secret: str
    refresh_token: str


async def _get_access_token(body: SnapchatAdsConnectionRequest) -> str:
    """Exchange refresh token for an access token."""
    async with httpx.AsyncClient(timeout=30) as client:
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


@sub_router.post("/snapchat-ads/ad-accounts")
async def snapchat_ads_accounts(
    body: SnapchatAdsConnectionRequest,
    _user: Profile = Depends(require_viewer),
) -> list[dict[str, str]]:
    """Fetch Snapchat Ads ad accounts accessible by the connection.

    Flow: authenticate → list organizations → list ad accounts per org.
    """
    try:
        token = await _get_access_token(body)
        headers = {"Authorization": f"Bearer {token}"}

        async with httpx.AsyncClient(timeout=30, headers=headers) as client:
            # 1. Get organizations
            org_resp = await client.get(f"{_BASE_URL}/me/organizations")
            org_resp.raise_for_status()
            orgs = org_resp.json().get("organizations", [])

            # 2. Get ad accounts for each organization
            accounts: list[dict[str, str]] = []
            for org_wrapper in orgs:
                org = org_wrapper.get("organization", {})
                org_id = org.get("id")
                if not org_id:
                    continue

                acct_resp = await client.get(f"{_BASE_URL}/organizations/{org_id}/adaccounts")
                acct_resp.raise_for_status()
                ad_accounts = acct_resp.json().get("adaccounts", [])

                for acct_wrapper in ad_accounts:
                    acct = acct_wrapper.get("adaccount", {})
                    if acct.get("status") != "ACTIVE":
                        continue
                    accounts.append({
                        "id": acct["id"],
                        "name": acct.get("name", acct["id"]),
                    })

        return accounts
    except Exception as exc:
        handle_error(exc, "fetching Snapchat Ads ad accounts")
        return []
