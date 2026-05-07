"""Google Ads external API routes."""

from __future__ import annotations

import httpx
from fastapi import APIRouter, Depends
from interloper_db import Profile
from pydantic import BaseModel

from interloper_api.dependencies import require_viewer
from interloper_api.routes.external import handle_error

sub_router = APIRouter()

_TOKEN_URL = "https://oauth2.googleapis.com/token"
_BASE_URL = "https://googleads.googleapis.com/v20"


class GoogleAdsConnectionRequest(BaseModel):
    """Google Ads connection credentials (matches GoogleAdsConnection fields)."""

    client_id: str
    client_secret: str
    developer_token: str
    refresh_token: str


async def _get_access_token(client: httpx.AsyncClient, body: GoogleAdsConnectionRequest) -> str:
    """Exchange the refresh token for an access token."""
    resp = await client.post(
        _TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "refresh_token": body.refresh_token,
            "client_id": body.client_id,
            "client_secret": body.client_secret,
        },
    )
    resp.raise_for_status()
    return resp.json()["access_token"]  # type: ignore[no-any-return]


def _auth_headers(access_token: str, developer_token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {access_token}",
        "developer-token": developer_token,
    }


@sub_router.post("/google-ads/customers")
async def google_ads_customers(
    body: GoogleAdsConnectionRequest,
    _user: Profile = Depends(require_viewer),
) -> list[dict[str, str]]:
    """Fetch Google Ads customer accounts accessible by the connection."""
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            access_token = await _get_access_token(client, body)
            headers = _auth_headers(access_token, body.developer_token)

            # Step 1: List accessible customer resource names.
            list_resp = await client.get(
                f"{_BASE_URL}/customers:listAccessibleCustomers",
                headers=headers,
            )
            list_resp.raise_for_status()
            resource_names: list[str] = list_resp.json().get("resourceNames", [])

            # Step 2: Fetch descriptive name for each customer.
            results: list[dict[str, str]] = []
            for rn in resource_names:
                # rn is like "customers/1234567890"
                customer_id = rn.split("/")[-1]
                query = (
                    "SELECT customer.id, customer.descriptive_name, customer.status "
                    "FROM customer LIMIT 1"
                )
                try:
                    search_resp = await client.post(
                        f"{_BASE_URL}/{rn}/googleAds:searchStream",
                        headers=headers,
                        json={"query": query},
                    )
                    search_resp.raise_for_status()
                    batches = search_resp.json()
                    for batch in batches:
                        for row in batch.get("results", []):
                            customer = row.get("customer", {})
                            name = customer.get("descriptiveName", customer_id)
                            results.append({
                                "customer_id": customer_id,
                                "name": f"{name} ({customer_id})",
                            })
                except httpx.HTTPStatusError:
                    # Some customers may not be queryable (suspended, etc.)
                    results.append({
                        "customer_id": customer_id,
                        "name": customer_id,
                    })

        return results
    except Exception as exc:
        handle_error(exc, "fetching Google Ads customers")
        return []
