"""Tests for ``interloper_api.routes.catalog`` — viewer auth gating."""

from __future__ import annotations

import interloper as il
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from interloper_api.dependencies import get_catalog, get_store, require_viewer
from interloper_api.routes import catalog as catalog_module


def _app() -> FastAPI:
    app = FastAPI()
    app.include_router(catalog_module.router)
    app.dependency_overrides[get_store] = lambda: None
    app.dependency_overrides[get_catalog] = lambda: il.Catalog(components={})
    return app


@pytest.mark.parametrize(
    "path",
    ["/catalog/", "/catalog/resource-kinds", "/catalog/some-key", "/catalog/kind/source"],
)
def test_unauthenticated_requests_rejected(path: str):
    resp = TestClient(_app()).get(path)
    assert resp.status_code == 401


def test_viewer_can_read_catalog():
    app = _app()
    app.dependency_overrides[require_viewer] = lambda: None
    client = TestClient(app)

    assert client.get("/catalog/").status_code == 200
    assert client.get("/catalog/resource-kinds").status_code == 200


def _authorized_app(catalog: il.Catalog) -> FastAPI:
    app = FastAPI()
    app.include_router(catalog_module.router)
    app.dependency_overrides[get_store] = lambda: None
    app.dependency_overrides[get_catalog] = lambda: catalog
    app.dependency_overrides[require_viewer] = lambda: None
    return app


class TestGetByKey:
    """``GET /catalog/{key}`` — one definition, serialised."""

    def test_returns_the_definition(self):
        from interloper_assets.facebook_ads.source import FacebookAds

        catalog = il.Catalog.from_assets([FacebookAds])
        key = next(iter(catalog.components))

        response = TestClient(_authorized_app(catalog)).get(f"/catalog/{key}")

        assert response.status_code == 200
        assert response.json()["key"] == key

    def test_an_unknown_key_is_a_404(self):
        response = TestClient(_authorized_app(il.Catalog(components={}))).get("/catalog/nope")

        assert response.status_code == 404
        assert response.json()["detail"] == "Component 'nope' not found in catalog"


class TestListByKind:
    """``GET /catalog/kind/{kind}`` — narrowed to one component kind."""

    def test_returns_only_the_matching_kind(self):
        from interloper_assets.facebook_ads.source import FacebookAds

        catalog = il.Catalog.from_assets([FacebookAds])

        sources = TestClient(_authorized_app(catalog)).get("/catalog/kind/source").json()

        assert sources
        assert all(entry["kind"] == "source" for entry in sources.values())

    def test_an_unmatched_kind_is_an_empty_map(self):
        from interloper_assets.facebook_ads.source import FacebookAds

        catalog = il.Catalog.from_assets([FacebookAds])

        assert TestClient(_authorized_app(catalog)).get("/catalog/kind/nonesuch").json() == {}
