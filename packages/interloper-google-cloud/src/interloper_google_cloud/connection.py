"""Google Cloud connection resource for service account credentials."""

from __future__ import annotations

import json
import time
from typing import Any

import httpx
from google.auth import crypt, jwt
from interloper.connection import Connection, connection
from interloper.resource.fields import JsonField, fetch_field_provider
from pydantic import field_validator
from pydantic_settings import SettingsConfigDict

_TOKEN_URL = "https://oauth2.googleapis.com/token"
_PROJECTS_URL = "https://bigquery.googleapis.com/bigquery/v2/projects"
_BUCKETS_URL = "https://storage.googleapis.com/storage/v1/b"
_BIGQUERY_SCOPE = "https://www.googleapis.com/auth/bigquery.readonly"
_STORAGE_SCOPE = "https://www.googleapis.com/auth/devstorage.read_only"


def _make_assertion(key_info: dict[str, Any], scope: str) -> str:
    """Build a signed JWT-bearer assertion for the service account.

    Only the signing comes from google-auth; the token exchange itself goes
    through httpx like every other external fetch.

    Args:
        key_info: The parsed service account key.
        scope: OAuth scope to request.

    Returns:
        The signed JWT assertion.
    """
    signer = crypt.RSASigner.from_service_account_info(key_info)
    now = int(time.time())
    payload = {
        "iss": key_info["client_email"],
        "scope": scope,
        "aud": _TOKEN_URL,
        "iat": now,
        "exp": now + 600,
    }
    return jwt.encode(signer, payload).decode()


async def _get_access_token(client: httpx.AsyncClient, key_info: dict[str, Any], scope: str) -> str:
    """Exchange a service account JWT assertion for an access token."""
    resp = await client.post(
        _TOKEN_URL,
        data={
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": _make_assertion(key_info, scope),
        },
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


async def _list_projects(client: httpx.AsyncClient, access_token: str) -> list[dict[str, str]]:
    """List the projects the credential has BigQuery access to, following pagination.

    Returns:
        Project options with ``project_id`` and a display ``name``.
    """
    results: list[dict[str, str]] = []
    page_token: str | None = None
    while True:
        params: dict[str, str] = {"maxResults": "500"}
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
            project_id = project["id"]
            name = project.get("friendlyName") or project_id
            results.append({"project_id": project_id, "name": f"{name} ({project_id})"})
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    return sorted(results, key=lambda p: p["name"].lower())


async def _list_buckets(client: httpx.AsyncClient, access_token: str, project: str) -> list[dict[str, str]]:
    """List the Cloud Storage buckets in *project*, following pagination.

    Returns:
        Bucket options with ``name``.
    """
    results: list[dict[str, str]] = []
    page_token: str | None = None
    while True:
        params: dict[str, str] = {"project": project, "maxResults": "500"}
        if page_token:
            params["pageToken"] = page_token
        resp = await client.get(
            _BUCKETS_URL,
            params=params,
            headers={"Authorization": f"Bearer {access_token}"},
        )
        resp.raise_for_status()
        data = resp.json()
        results.extend({"name": bucket["name"]} for bucket in data.get("items", []))
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    return sorted(results, key=lambda b: b["name"].lower())


@connection(
    key="google_cloud_connection",
    name="Google Cloud",
    icon="devicon:googlecloud",
    tags=["Cloud"],
)
class GoogleCloudConnection(Connection):
    """Connection resource holding Google Cloud credentials."""

    model_config = SettingsConfigDict(env_prefix="google_cloud_")

    service_account_key: str = JsonField()

    @field_validator("service_account_key", mode="before")
    @classmethod
    def _serialize_key(cls, v: object) -> object:
        if isinstance(v, dict):
            return json.dumps(v)
        return v

    @fetch_field_provider
    async def projects(self) -> list[dict[str, str]]:
        """List the Google Cloud projects this connection has BigQuery access to.

        Backs the BigQuery destination's ``project`` ``FetchField``. Signs a
        JWT-bearer assertion from the service account key and exchanges it for
        an access token over httpx (only the signing uses google-auth), then
        pages through BigQuery's ``projects.list``.
        """
        key_info: dict[str, Any] = json.loads(self.service_account_key)
        async with httpx.AsyncClient(timeout=30) as client:
            access_token = await _get_access_token(client, key_info, _BIGQUERY_SCOPE)
            return await _list_projects(client, access_token)

    @fetch_field_provider
    async def buckets(self) -> list[dict[str, str]]:
        """List the Cloud Storage buckets in the service account's own project.

        Backs the GCS destination's ``bucket`` ``FetchField``. Same JWT-bearer
        exchange as ``projects``, with the read-only storage scope; the project
        comes from the key itself (bucket names are global, but listing is
        per-project).
        """
        key_info: dict[str, Any] = json.loads(self.service_account_key)
        async with httpx.AsyncClient(timeout=30) as client:
            access_token = await _get_access_token(client, key_info, _STORAGE_SCOPE)
            return await _list_buckets(client, access_token, key_info["project_id"])

    async def check(self) -> bool:
        """Prove the credentials work by running the ``projects`` lookup.

        Returns:
            True — any credential failure raises out of the lookup.
        """
        await self.projects()
        return True
