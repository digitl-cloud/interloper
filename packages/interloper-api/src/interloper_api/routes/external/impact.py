"""Impact external API routes."""

from __future__ import annotations

import httpx
from fastapi import APIRouter, Depends
from interloper_db import Profile
from pydantic import BaseModel

from interloper_api.dependencies import require_viewer
from interloper_api.routes.external import handle_error

sub_router = APIRouter()

_BASE_URL = "https://api.impact.com"


class ImpactConnectionRequest(BaseModel):
    """Impact connection credentials (matches ImpactConnection fields)."""

    account_sid: str
    auth_token: str


@sub_router.post("/impact/programs")
async def impact_programs(
    body: ImpactConnectionRequest,
    _user: Profile = Depends(require_viewer),
) -> list[dict[str, str]]:
    """Fetch Impact programs (campaigns) accessible by the connection."""
    try:
        programs: list[dict[str, str]] = []
        async with httpx.AsyncClient(
            timeout=30,
            auth=httpx.BasicAuth(body.account_sid, body.auth_token),
            headers={"Accept": "application/json"},
        ) as client:
            page, num_pages = 1, 1
            while page <= num_pages:
                resp = await client.get(
                    f"{_BASE_URL}/Advertisers/{body.account_sid}/Campaigns",
                    params={"Page": page},
                )
                resp.raise_for_status()
                data = resp.json()
                num_pages = int(data["@numpages"])
                for campaign in data.get("Campaigns", []):
                    programs.append({"Id": campaign["Id"], "Name": campaign.get("Name", campaign["Id"])})
                page += 1

        return programs
    except Exception as exc:
        handle_error(exc, "fetching Impact programs")
        return []
