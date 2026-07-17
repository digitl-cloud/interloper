"""Tests for ``interloper_api.routes.components`` type-level operations (resolve + check)."""

from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

import httpx
import interloper as il
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from interloper.errors import InUseError
from interloper_assets.facebook_ads import connection as fb_connection
from interloper_assets.facebook_ads.connection import FacebookAdsConnection
from interloper_assets.facebook_ads.source import FacebookAds

from interloper_api.dependencies import get_catalog, get_current_user, get_store, require_viewer
from interloper_api.routes import components as components_module

CONNECTION_CONFIG = {"access_token": "TOK", "app_id": "A", "app_secret": "S", "_id": "x"}


class UncheckableConnection(il.Connection):
    """A connection with no ``check()`` hook — module-level so its path imports."""

    api_key: str = il.SecretField()


def _client(catalog: il.Catalog) -> TestClient:
    app = FastAPI()
    app.include_router(components_module.router)
    app.dependency_overrides[require_viewer] = lambda: None
    app.dependency_overrides[get_catalog] = lambda: catalog
    return TestClient(app)


@pytest.fixture
def source_catalog() -> il.Catalog:
    return il.Catalog.from_assets([FacebookAds])


@pytest.fixture
def connection_catalog() -> il.Catalog:
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


class TestResolve:
    def test_resolves_provider_options(self, source_catalog: il.Catalog, mock_graph):
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

        resp = _client(source_catalog).post(
            "/components/resolve",
            json={
                "component_key": "facebook_ads",
                "field": "account_id",
                # Credentials carry an internal _id marker that must be stripped.
                "deps": {"connection": CONNECTION_CONFIG},
            },
        )

        assert resp.status_code == 200
        assert resp.json() == [{"account_id": "111", "name": "Acme"}]
        # The connection's access token reached the Graph call; _id was not sent as a field.
        assert captured[0].url.params["access_token"] == "TOK"

    def test_unknown_component_404(self, source_catalog: il.Catalog):
        resp = _client(source_catalog).post(
            "/components/resolve",
            json={"component_key": "nope", "field": "account_id", "deps": {}},
        )
        assert resp.status_code == 404

    def test_non_provider_field_400(self, source_catalog: il.Catalog):
        resp = _client(source_catalog).post(
            "/components/resolve",
            json={"component_key": "facebook_ads", "field": "dataset", "deps": {}},
        )
        assert resp.status_code == 400


def _check(catalog: il.Catalog, config: dict) -> httpx.Response:
    return _client(catalog).post(
        "/components/check",
        json={"component_key": "facebook_ads_connection", "config": config},
    )


class TestCheck:
    def test_live_check_passes(self, connection_catalog: il.Catalog, mock_graph):
        mock_graph(lambda request: httpx.Response(200, json={"data": []}))

        resp = _check(connection_catalog, CONNECTION_CONFIG)

        assert resp.status_code == 200
        body = resp.json()
        assert (body["ok"], body["live"]) == (True, True)

    def test_rejected_credentials_reported_as_auth(self, connection_catalog: il.Catalog, mock_graph):
        mock_graph(lambda request: httpx.Response(401, json={"error": "bad token"}))

        body = _check(connection_catalog, CONNECTION_CONFIG).json()

        assert (body["ok"], body["live"], body["category"]) == (False, True, "auth")

    def test_unreachable_provider_reported_as_network(self, connection_catalog: il.Catalog, mock_graph):
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("no route to host")

        mock_graph(handler)

        body = _check(connection_catalog, CONNECTION_CONFIG).json()

        assert (body["ok"], body["live"], body["category"]) == (False, True, "network")

    def test_invalid_config_reports_field_errors(self, connection_catalog: il.Catalog):
        # Static tier: a missing required field never reaches the provider.
        body = _check(connection_catalog, {"app_id": "A", "app_secret": "S"}).json()

        assert (body["ok"], body["live"], body["category"]) == (False, False, "config")
        assert [e["field"] for e in body["errors"]] == ["access_token"]

    def test_uncheckable_connection_is_static_only(self):
        catalog = il.Catalog(components={UncheckableConnection.key: UncheckableConnection.definition()})
        resp = _client(catalog).post(
            "/components/check",
            json={"component_key": "uncheckable_connection", "config": {"api_key": "k"}},
        )

        body = resp.json()
        assert (body["ok"], body["live"]) == (True, False)

    def test_unknown_component_404(self, connection_catalog: il.Catalog):
        resp = _client(connection_catalog).post("/components/check", json={"component_key": "nope", "config": {}})
        assert resp.status_code == 404

    def test_non_connection_component_404(self, source_catalog: il.Catalog):
        resp = _client(source_catalog).post(
            "/components/check", json={"component_key": "facebook_ads", "config": {}}
        )
        assert resp.status_code == 404


class TestDelete:
    """DELETE /components/{id} maps store errors to HTTP statuses."""

    def test_in_use_maps_to_409_with_referrers(self):
        org_id = uuid4()
        referrers: list[dict[str, str | None]] = [
            {"id": str(uuid4()), "kind": "source", "key": "facebook_ads", "name": "FB"}
        ]

        class FakeStore:
            def get_component(self, component_id):
                return SimpleNamespace(id=component_id, org_id=org_id)

            def get_user_role(self, user_id, org_id):
                return "admin"

            def delete_component(self, component_id):
                raise InUseError("Cannot delete connection 'C': in use by FB", referrers=referrers)

        app = FastAPI()
        app.include_router(components_module.router)
        app.dependency_overrides[get_store] = lambda: FakeStore()
        app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(id=uuid4(), is_super_admin=False)

        resp = TestClient(app).delete(f"/components/{uuid4()}")

        assert resp.status_code == 409
        assert resp.json()["detail"]["used_by"] == referrers
