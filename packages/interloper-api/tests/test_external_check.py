"""Tests for ``interloper_api.routes.external.check`` (generic connection checker)."""

from __future__ import annotations

import httpx
import interloper as il
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from interloper_assets.facebook_ads import connection as fb_connection
from interloper_assets.facebook_ads.connection import FacebookAdsConnection
from interloper_assets.facebook_ads.source import FacebookAds

from interloper_api.dependencies import get_catalog, require_viewer
from interloper_api.routes import external as external_module

CONFIG = {"access_token": "TOK", "app_id": "A", "app_secret": "S", "_id": "x"}


class UncheckableConnection(il.Connection):
    """A connection with no ``check()`` hook — module-level so its path imports."""

    api_key: str = il.SecretField()


def _client(catalog: il.Catalog) -> TestClient:
    app = FastAPI()
    app.include_router(external_module.router, prefix="/external")
    app.dependency_overrides[require_viewer] = lambda: None
    app.dependency_overrides[get_catalog] = lambda: catalog
    return TestClient(app)


@pytest.fixture
def catalog() -> il.Catalog:
    return il.Catalog(components={FacebookAdsConnection.key: FacebookAdsConnection.definition()})


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


def _check(catalog: il.Catalog, config: dict) -> httpx.Response:
    return _client(catalog).post(
        "/external/check",
        json={"component_key": "facebook_ads_connection", "config": config},
    )


class TestCheck:
    def test_live_check_passes(self, catalog: il.Catalog, mock_graph):
        mock_graph(lambda request: httpx.Response(200, json={"data": []}))

        resp = _check(catalog, CONFIG)

        assert resp.status_code == 200
        body = resp.json()
        assert (body["ok"], body["live"]) == (True, True)

    def test_rejected_credentials_reported_as_auth(self, catalog: il.Catalog, mock_graph):
        mock_graph(lambda request: httpx.Response(401, json={"error": "bad token"}))

        body = _check(catalog, CONFIG).json()

        assert (body["ok"], body["live"], body["category"]) == (False, True, "auth")

    def test_unreachable_provider_reported_as_network(self, catalog: il.Catalog, mock_graph):
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("no route to host")

        mock_graph(handler)

        body = _check(catalog, CONFIG).json()

        assert (body["ok"], body["live"], body["category"]) == (False, True, "network")

    def test_invalid_config_reports_field_errors(self, catalog: il.Catalog):
        # Static tier: a missing required field never reaches the provider.
        body = _check(catalog, {"app_id": "A", "app_secret": "S"}).json()

        assert (body["ok"], body["live"], body["category"]) == (False, False, "config")
        assert [e["field"] for e in body["errors"]] == ["access_token"]

    def test_uncheckable_connection_is_static_only(self):
        catalog = il.Catalog(components={UncheckableConnection.key: UncheckableConnection.definition()})
        resp = _client(catalog).post(
            "/external/check",
            json={"component_key": "uncheckable_connection", "config": {"api_key": "k"}},
        )

        body = resp.json()
        assert (body["ok"], body["live"]) == (True, False)

    def test_unknown_component_404(self, catalog: il.Catalog):
        resp = _client(catalog).post("/external/check", json={"component_key": "nope", "config": {}})
        assert resp.status_code == 404

    def test_non_connection_component_404(self, catalog: il.Catalog):
        catalog = il.Catalog.from_assets([FacebookAds])
        resp = _client(catalog).post("/external/check", json={"component_key": "facebook_ads", "config": {}})
        assert resp.status_code == 404
