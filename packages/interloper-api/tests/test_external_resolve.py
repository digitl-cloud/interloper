"""Tests for ``interloper_api.routes.external.resolve`` (generic FetchField resolver)."""

from __future__ import annotations

import httpx
import interloper as il
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from interloper_assets.facebook_ads import connection as fb_connection
from interloper_assets.facebook_ads.source import FacebookAds

from interloper_api.dependencies import get_catalog, require_viewer
from interloper_api.routes import external as external_module


def _client(catalog: il.Catalog) -> TestClient:
    app = FastAPI()
    app.include_router(external_module.router, prefix="/external")
    app.dependency_overrides[require_viewer] = lambda: None
    app.dependency_overrides[get_catalog] = lambda: catalog
    return TestClient(app)


@pytest.fixture
def catalog() -> il.Catalog:
    return il.Catalog.from_assets([FacebookAds])


@pytest.fixture
def mock_graph(monkeypatch: pytest.MonkeyPatch):
    """Patch the Facebook connection's httpx client with a mock transport."""

    def install(handler) -> None:
        real_client = httpx.AsyncClient

        def factory(*args, **kwargs):
            kwargs["transport"] = httpx.MockTransport(handler)
            return real_client(*args, **kwargs)

        monkeypatch.setattr(fb_connection.httpx, "AsyncClient", factory)

    return install


class TestResolve:
    def test_resolves_provider_options(self, catalog: il.Catalog, mock_graph):
        captured: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            captured.append(request)
            return httpx.Response(
                200,
                json={
                    "data": [
                        {"account_id": "111", "name": "Acme", "account_status": 1},
                        {"account_id": "222", "name": "Paused", "account_status": 2},  # filtered out
                    ]
                },
            )

        mock_graph(handler)

        resp = _client(catalog).post(
            "/external/resolve",
            json={
                "component_key": "facebook_ads",
                "field": "account_id",
                # Credentials carry an internal _id marker that must be stripped.
                "deps": {"connection": {"access_token": "TOK", "app_id": "A", "app_secret": "S", "_id": "x"}},
            },
        )

        assert resp.status_code == 200
        assert resp.json() == [{"account_id": "111", "name": "Acme"}]
        # The connection's access token reached the Graph call; _id was not sent as a field.
        assert captured[0].url.params["access_token"] == "TOK"

    def test_unknown_component_404(self, catalog: il.Catalog):
        resp = _client(catalog).post(
            "/external/resolve",
            json={"component_key": "nope", "field": "account_id", "deps": {}},
        )
        assert resp.status_code == 404

    def test_non_provider_field_400(self, catalog: il.Catalog):
        resp = _client(catalog).post(
            "/external/resolve",
            json={"component_key": "facebook_ads", "field": "dataset", "deps": {}},
        )
        assert resp.status_code == 400
