"""Tests for ``interloper_api.routes.external.google_cloud`` (FetchField resolution)."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from interloper_api.dependencies import require_viewer
from interloper_api.routes import external as external_module
from interloper_api.routes.external import google_cloud as google_cloud_module

_KEY_INFO = {"type": "service_account", "client_email": "sa@proj.iam.gserviceaccount.com", "private_key": "pem"}


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(external_module.router, prefix="/external")
    app.dependency_overrides[require_viewer] = lambda: None
    return TestClient(app)


class TestConnectionRequest:
    def test_accepts_string_key(self):
        body = google_cloud_module.GoogleCloudConnectionRequest(service_account_key=json.dumps(_KEY_INFO))
        assert body.key_info == _KEY_INFO

    def test_accepts_dict_key(self):
        # Matches GoogleCloudConnection's before-validator: stored data may
        # hold the key as a dict or a JSON string.
        body = google_cloud_module.GoogleCloudConnectionRequest.model_validate({"service_account_key": _KEY_INFO})
        assert body.key_info == _KEY_INFO

    def test_invalid_json_is_a_400(self):
        body = google_cloud_module.GoogleCloudConnectionRequest(service_account_key="not-json")
        with pytest.raises(Exception, match="not valid JSON"):
            _ = body.key_info


class TestListProjects:
    def _capture_client(self, pages: list[dict[str, Any]], captured: list[httpx.Request]) -> httpx.AsyncClient:
        responses = iter(pages)

        def handler(request: httpx.Request) -> httpx.Response:
            captured.append(request)
            return httpx.Response(200, json=next(responses))

        return httpx.AsyncClient(transport=httpx.MockTransport(handler))

    def test_follows_pagination_and_sorts(self):
        captured: list[httpx.Request] = []
        pages = [
            {
                "projects": [{"projectId": "proj-b", "name": "Bravo"}],
                "nextPageToken": "page-2",
            },
            {
                "projects": [{"projectId": "proj-a", "name": "alpha"}, {"projectId": "proj-c"}],
            },
        ]

        async def run() -> list[dict[str, str]]:
            async with self._capture_client(pages, captured) as client:
                return await google_cloud_module._list_projects(client, "the-token")

        results = asyncio.run(run())

        assert results == [
            {"project_id": "proj-a", "name": "alpha (proj-a)"},
            {"project_id": "proj-b", "name": "Bravo (proj-b)"},
            {"project_id": "proj-c", "name": "proj-c (proj-c)"},
        ]
        assert len(captured) == 2
        assert captured[0].headers["Authorization"] == "Bearer the-token"
        assert captured[0].url.params["filter"] == "lifecycleState:ACTIVE"
        assert "pageToken" not in captured[0].url.params
        assert captured[1].url.params["pageToken"] == "page-2"


class TestGetAccessToken:
    def test_exchanges_jwt_bearer_assertion(self, monkeypatch: pytest.MonkeyPatch):
        captured: list[httpx.Request] = []
        monkeypatch.setattr(google_cloud_module, "_make_assertion", lambda key_info: "signed-jwt")

        def handler(request: httpx.Request) -> httpx.Response:
            captured.append(request)
            return httpx.Response(200, json={"access_token": "the-token"})

        async def run() -> str:
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                return await google_cloud_module._get_access_token(client, _KEY_INFO)

        token = asyncio.run(run())

        assert token == "the-token"
        req = captured[0]
        assert str(req.url) == google_cloud_module._TOKEN_URL
        content = req.content.decode()
        assert "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer" in content
        assert "assertion=signed-jwt" in content


class TestRoute:
    def test_invalid_key_is_rejected_without_calling_out(self):
        resp = _client().post("/external/google-cloud/projects", json={"service_account_key": "not-json"})
        assert resp.status_code == 400

    def test_auth_failure_surfaces_google_message(self, monkeypatch: pytest.MonkeyPatch):
        async def fail(client: httpx.AsyncClient, key_info: dict[str, Any]) -> str:
            response = httpx.Response(
                403,
                request=httpx.Request("GET", google_cloud_module._PROJECTS_URL),
                json={"error": {"code": 403, "message": "Cloud Resource Manager API has not been used"}},
            )
            raise httpx.HTTPStatusError("forbidden", request=response.request, response=response)

        monkeypatch.setattr(google_cloud_module, "_get_access_token", fail)

        resp = _client().post(
            "/external/google-cloud/projects",
            json={"service_account_key": json.dumps(_KEY_INFO)},
        )

        assert resp.status_code == 403
        assert "Cloud Resource Manager API has not been used" in resp.json()["detail"]

    def test_token_endpoint_400_maps_to_502_with_description(self, monkeypatch: pytest.MonkeyPatch):
        async def fail(client: httpx.AsyncClient, key_info: dict[str, Any]) -> str:
            response = httpx.Response(
                400,
                request=httpx.Request("POST", google_cloud_module._TOKEN_URL),
                json={"error": "invalid_grant", "error_description": "Invalid JWT Signature."},
            )
            raise httpx.HTTPStatusError("bad request", request=response.request, response=response)

        monkeypatch.setattr(google_cloud_module, "_get_access_token", fail)

        resp = _client().post(
            "/external/google-cloud/projects",
            json={"service_account_key": json.dumps(_KEY_INFO)},
        )

        assert resp.status_code == 502
        assert "Invalid JWT Signature." in resp.json()["detail"]
