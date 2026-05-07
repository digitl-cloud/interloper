"""Amazon Ads external API routes."""

from __future__ import annotations

import httpx
from fastapi import APIRouter, Depends
from interloper_db import Profile
from pydantic import BaseModel

from interloper_api.dependencies import require_viewer
from interloper_api.routes.external import handle_error

sub_router = APIRouter()

_API_URLS: dict[str, str] = {
    "EU": "https://advertising-api-eu.amazon.com",
    "FE": "https://advertising-api-fe.amazon.com",
    "NA": "https://advertising-api.amazon.com",
}

_AUTH_URLS: dict[str, str] = {
    "EU": "https://api.amazon.co.uk",
    "FE": "https://api.amazon.co.jp",
    "NA": "https://api.amazon.com",
}


class AmazonAdsConnectionRequest(BaseModel):
    """Amazon Ads connection credentials (matches AmazonAdsConnection fields)."""

    location: str = "NA"
    client_id: str
    client_secret: str
    refresh_token: str


@sub_router.post("/amazon-ads/profiles")
async def amazon_ads_profiles(
    body: AmazonAdsConnectionRequest,
    _user: Profile = Depends(require_viewer),
) -> list[dict[str, str]]:
    """Fetch Amazon Ads advertising profiles for a connection."""
    api_url = _API_URLS.get(body.location, _API_URLS["NA"])
    auth_url = _AUTH_URLS.get(body.location, _AUTH_URLS["NA"])

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            token_resp = await client.post(
                f"{auth_url}/auth/o2/token",
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": body.refresh_token,
                    "client_id": body.client_id,
                    "client_secret": body.client_secret,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            token_resp.raise_for_status()
            access_token = token_resp.json()["access_token"]

            profiles_resp = await client.get(
                f"{api_url}/v2/profiles",
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Amazon-Advertising-API-ClientId": body.client_id,
                },
            )
            profiles_resp.raise_for_status()
            profiles = profiles_resp.json()

        return [
            {
                "profile_id": str(p["profileId"]),
                "name": f"{p['accountInfo']['name']} ({p['countryCode']})",
                "account_id": p["accountInfo"]["id"],
                "country_code": p["countryCode"],
            }
            for p in profiles
        ]
    except Exception as exc:
        handle_error(exc, "fetching Amazon Ads profiles")
        return []  # unreachable, but satisfies type checker
