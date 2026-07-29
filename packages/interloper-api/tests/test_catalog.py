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
