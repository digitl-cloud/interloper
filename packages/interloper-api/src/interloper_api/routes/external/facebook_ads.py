"""Facebook Ads external API routes."""

from __future__ import annotations

import httpx
from fastapi import APIRouter, Depends
from interloper_db import Profile
from pydantic import BaseModel

from interloper_api.dependencies import require_viewer
from interloper_api.routes.external import handle_error

sub_router = APIRouter()

_BASE_URL = "https://graph.facebook.com/v21.0"


class FacebookAdsConnectionRequest(BaseModel):
    """Facebook Ads connection credentials (matches FacebookAdsConnection fields)."""

    access_token: str
    app_id: str = ""
    app_secret: str = ""


@sub_router.post("/facebook-ads/accounts")
async def facebook_ads_accounts(
    body: FacebookAdsConnectionRequest,
    _user: Profile = Depends(require_viewer),
) -> list[dict[str, str]]:
    """Fetch Facebook Ads ad accounts accessible by the connection."""
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(
                f"{_BASE_URL}/me/adaccounts",
                params={
                    "access_token": body.access_token,
                    "fields": "account_id,name,account_status",
                    "limit": "500",
                },
            )
            resp.raise_for_status()
            data = resp.json().get("data", [])

        return [
            {
                "account_id": acct["account_id"],
                "name": f"{acct.get('name', acct['account_id'])}",
            }
            for acct in data
            if acct.get("account_status") == 1  # ACTIVE only
        ]
    except Exception as exc:
        handle_error(exc, "fetching Facebook Ads accounts")
        return []
