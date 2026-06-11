"""Google Cloud external API routes."""

from __future__ import annotations

import json
import time
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException
from google.auth import crypt, jwt
from interloper_db import Profile
from pydantic import BaseModel, field_validator

from interloper_api.dependencies import require_viewer
from interloper_api.routes.external import handle_error

sub_router = APIRouter()

_TOKEN_URL = "https://oauth2.googleapis.com/token"
_PROJECTS_URL = "https://cloudresourcemanager.googleapis.com/v1/projects"
_SCOPE = "https://www.googleapis.com/auth/cloud-platform.read-only"


class GoogleCloudConnectionRequest(BaseModel):
    """Google Cloud connection credentials (matches GoogleCloudConnection fields)."""

    service_account_key: str

    @field_validator("service_account_key", mode="before")
    @classmethod
    def _serialize_key(cls, v: object) -> object:
        if isinstance(v, dict):
            return json.dumps(v)
        return v

    @property
    def key_info(self) -> dict[str, Any]:
        """The parsed service account key.

        Returns:
            The key as a dict.

        Raises:
            HTTPException: If the key is not valid JSON.
        """
        try:
            return json.loads(self.service_account_key)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="service_account_key is not valid JSON.")


def _make_assertion(key_info: dict[str, Any]) -> str:
    """Build a signed JWT-bearer assertion for the service account.

    Only the signing comes from google-auth; the token exchange itself goes
    through httpx like every other external route.

    Args:
        key_info: The parsed service account key.

    Returns:
        The signed JWT assertion.
    """
    signer = crypt.RSASigner.from_service_account_info(key_info)
    now = int(time.time())
    payload = {
        "iss": key_info["client_email"],
        "scope": _SCOPE,
        "aud": _TOKEN_URL,
        "iat": now,
        "exp": now + 600,
    }
    return jwt.encode(signer, payload).decode()


async def _get_access_token(client: httpx.AsyncClient, key_info: dict[str, Any]) -> str:
    """Exchange a service account JWT assertion for an access token."""
    resp = await client.post(
        _TOKEN_URL,
        data={
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": _make_assertion(key_info),
        },
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


async def _list_projects(client: httpx.AsyncClient, access_token: str) -> list[dict[str, str]]:
    """List the active projects visible to the credential, following pagination.

    Returns:
        Project options with ``project_id`` and a display ``name``.
    """
    results: list[dict[str, str]] = []
    page_token: str | None = None
    while True:
        params: dict[str, str] = {"filter": "lifecycleState:ACTIVE"}
        if page_token:
            params["pageToken"] = page_token
        resp = await client.get(
            _PROJECTS_URL,
            params=params,
            headers={"Authorization": f"Bearer {access_token}"},
        )
        resp.raise_for_status()
        data = resp.json()
        for project in data.get("projects", []):
            project_id = project["projectId"]
            name = project.get("name") or project_id
            results.append({"project_id": project_id, "name": f"{name} ({project_id})"})
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    return sorted(results, key=lambda p: p["name"].lower())


@sub_router.post("/google-cloud/projects")
async def google_cloud_projects(
    body: GoogleCloudConnectionRequest,
    _user: Profile = Depends(require_viewer),
) -> list[dict[str, str]]:
    """Fetch the Google Cloud projects accessible by the connection."""
    key_info = body.key_info
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            access_token = await _get_access_token(client, key_info)
            return await _list_projects(client, access_token)
    except Exception as exc:
        handle_error(exc, "fetching Google Cloud projects")
        return []  # unreachable, but satisfies type checker
