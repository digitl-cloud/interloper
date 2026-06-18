"""Tests for ``interloper_api.routes.external.criteo`` (FetchField resolution)."""

from __future__ import annotations

import asyncio

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from interloper_api.dependencies import require_viewer
from interloper_api.routes import external as external_module
from interloper_api.routes.external import criteo as criteo_module


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(external_module.router, prefix="/external")
    app.dependency_overrides[require_viewer] = lambda: None
    return TestClient(app)


def _body() -> criteo_module.CriteoConnectionRequest:
    return criteo_module.CriteoConnectionRequest(
        client_id="cid",
        client_secret="secret",
        refresh_token="refresh",
    )


class TestListAdvertisers:
    def _client(self, captured: list[httpx.Request]) -> httpx.AsyncClient:
        def handler(request: httpx.Request) -> httpx.Response:
            captured.append(request)
            if request.url.path == "/oauth2/token":
                return httpx.Response(200, json={"access_token": "the-token"})
            return httpx.Response(
                200,
                json={
                    "data": [
                        {"type": "advertiser", "id": "2", "attributes": {"advertiserName": "Bravo"}},
                        {"type": "advertiser", "id": "1", "attributes": {"advertiserName": "alpha"}},
                        {"type": "advertiser", "id": "3", "attributes": {}},  # missing name falls back to id
                    ]
                },
            )

        return httpx.AsyncClient(transport=httpx.MockTransport(handler))

    def test_maps_fields_and_sorts_by_name(self):
        captured: list[httpx.Request] = []

        async def run() -> list[dict[str, str]]:
            async with self._client(captured) as client:
                return await criteo_module._list_advertisers(client, _body())

        results = asyncio.run(run())

        assert results == [
            {"id": "3", "name": "3"},
            {"id": "1", "name": "alpha"},
            {"id": "2", "name": "Bravo"},
        ]
        # The refresh token is exchanged before listing, and the access token is forwarded.
        token_req, advertisers_req = captured
        assert token_req.url.path == "/oauth2/token"
        assert advertisers_req.headers["Authorization"] == "Bearer the-token"
        assert advertisers_req.url.path == "/2025-04/advertisers/me"


class TestRoute:
    def test_auth_failure_surfaces_as_401(self, monkeypatch: pytest.MonkeyPatch):
        async def fail(client: httpx.AsyncClient, *args: object) -> list[dict[str, str]]:
            response = httpx.Response(
                401,
                request=httpx.Request("POST", criteo_module._TOKEN_URL),
                json={"errors": [{"detail": "invalid_grant"}]},
            )
            raise httpx.HTTPStatusError("unauthorized", request=response.request, response=response)

        monkeypatch.setattr(criteo_module, "_list_advertisers", fail)

        resp = _client().post(
            "/external/criteo/advertisers",
            json={"client_id": "cid", "client_secret": "secret", "refresh_token": "bad"},
        )
        assert resp.status_code == 401
