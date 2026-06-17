"""Tests for ``interloper_api.routes.external.tiktok_ads`` (FetchField resolution)."""

from __future__ import annotations

import asyncio

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from interloper_api.dependencies import require_viewer
from interloper_api.routes import external as external_module
from interloper_api.routes.external import tiktok_ads as tiktok_ads_module


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(external_module.router, prefix="/external")
    app.dependency_overrides[require_viewer] = lambda: None
    return TestClient(app)


class TestListAdvertisers:
    def _capture_client(self, body: dict, captured: list[httpx.Request]) -> httpx.AsyncClient:
        def handler(request: httpx.Request) -> httpx.Response:
            captured.append(request)
            return httpx.Response(200, json=body)

        return httpx.AsyncClient(transport=httpx.MockTransport(handler))

    def test_maps_fields_and_sorts_by_name(self):
        captured: list[httpx.Request] = []
        body = {
            "code": 0,
            "message": "OK",
            "data": {
                "list": [
                    {"advertiser_id": "2", "advertiser_name": "Bravo"},
                    {"advertiser_id": "1", "advertiser_name": "alpha"},
                    {"advertiser_id": "3"},  # missing name falls back to the id
                ]
            },
        }

        async def run() -> list[dict[str, str]]:
            async with self._capture_client(body, captured) as client:
                return await tiktok_ads_module._list_advertisers(client, "the-token", "app", "secret")

        results = asyncio.run(run())

        assert results == [
            {"advertiser_id": "3", "name": "3"},
            {"advertiser_id": "1", "name": "alpha"},
            {"advertiser_id": "2", "name": "Bravo"},
        ]
        req = captured[0]
        assert req.headers["Access-Token"] == "the-token"
        assert req.url.params["app_id"] == "app"
        assert req.url.params["secret"] == "secret"

    def test_business_error_code_maps_to_502(self):
        body = {"code": 40001, "message": "Invalid app_id", "data": {}}

        async def run() -> list[dict[str, str]]:
            def handler(request: httpx.Request) -> httpx.Response:
                return httpx.Response(200, json=body)

            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                return await tiktok_ads_module._list_advertisers(client, "t", "app", "secret")

        with pytest.raises(Exception, match="Invalid app_id"):
            asyncio.run(run())


class TestRoute:
    def test_auth_failure_surfaces_as_401(self, monkeypatch: pytest.MonkeyPatch):
        async def fail(client: httpx.AsyncClient, *args: object) -> list[dict[str, str]]:
            response = httpx.Response(
                401,
                request=httpx.Request("GET", tiktok_ads_module._ADVERTISERS_URL),
                json={"message": "Access token is invalid"},
            )
            raise httpx.HTTPStatusError("unauthorized", request=response.request, response=response)

        monkeypatch.setattr(tiktok_ads_module, "_list_advertisers", fail)

        resp = _client().post("/external/tiktok-ads/advertisers", json={"access_token": "bad"})
        assert resp.status_code == 401
