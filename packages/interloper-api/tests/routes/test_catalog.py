"""Tests for ``interloper_api.routes.catalog``: the definitions and the operations on a type."""

from __future__ import annotations

import httpx2
import interloper as il
import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from interloper_assets.facebook_ads import connection as fb_connection
from interloper_assets.facebook_ads.connection import FacebookAdsConnection
from interloper_assets.facebook_ads.source import FacebookAds

from interloper_api.app import install_error_handlers
from interloper_api.dependencies import get_catalog, get_store, require_editor, require_viewer
from interloper_api.routes import catalog as catalog_module


def _app() -> FastAPI:
    app = FastAPI()
    install_error_handlers(app)
    app.include_router(catalog_module.router)
    app.dependency_overrides[get_store] = lambda: None
    app.dependency_overrides[get_catalog] = lambda: il.Catalog(components={})
    return app


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("get", "/catalog"),
        ("get", "/catalog/resource-kinds"),
        ("post", "/catalog/facebook_ads/resolve"),
        ("post", "/catalog/facebook_ads_connection/check"),
    ],
)
def test_unauthenticated_requests_rejected(method: str, path: str):
    resp = TestClient(_app()).request(method, path, json={"field": "x"})
    assert resp.status_code == 401


def test_viewer_can_read_catalog():
    app = _app()
    app.dependency_overrides[require_viewer] = lambda: None
    client = TestClient(app)

    assert client.get("/catalog").status_code == 200
    assert client.get("/catalog/resource-kinds").status_code == 200


def test_the_catalog_is_served_whole():
    catalog = il.Catalog.from_assets([FacebookAds])
    app = _app()
    app.dependency_overrides[get_catalog] = lambda: catalog
    app.dependency_overrides[require_viewer] = lambda: None

    response = TestClient(app).get("/catalog")

    assert response.status_code == 200
    assert response.json() == catalog.dump()


def test_resource_kinds_are_the_kinds_anchored_under_resource():
    catalog = il.Catalog.from_assets([FacebookAds])
    app = _app()
    app.dependency_overrides[get_catalog] = lambda: catalog
    app.dependency_overrides[require_viewer] = lambda: None

    response = TestClient(app).get("/catalog/resource-kinds")

    assert response.json() == ["connection"]


@pytest.mark.parametrize("path", ["/catalog/facebook_ads", "/catalog/kind/source"])
def test_the_per_key_and_per_kind_reads_are_gone(path: str):
    app = _app()
    app.dependency_overrides[require_viewer] = lambda: None

    assert TestClient(app).get(path).status_code == 404


# -- Operations on a type ------------------------------------------------------


CONNECTION_CONFIG = {"access_token": "TOK", "app_id": "A", "app_secret": "S", "_id": "x"}


class UncheckableConnection(il.Connection):
    """A connection with no ``check()`` hook, module-level so its path imports."""

    api_key: str = il.SecretField()


def _client(catalog: il.Catalog) -> TestClient:
    app = FastAPI()
    install_error_handlers(app)
    app.include_router(catalog_module.router)
    app.dependency_overrides[require_viewer] = lambda: None
    app.dependency_overrides[require_editor] = lambda: None
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
    """Patch the Facebook connection's httpx2 client with a mock transport.

    Returns:
        The list the transport records each handled request into.
    """

    def install(handler) -> None:
        real_client = httpx2.AsyncClient

        def factory(*args, **kwargs):
            kwargs["transport"] = httpx2.MockTransport(handler)
            return real_client(*args, **kwargs)

        monkeypatch.setattr(fb_connection.httpx2, "AsyncClient", factory)

    return install


class TestResolve:
    def test_resolves_provider_options(self, source_catalog: il.Catalog, mock_graph):
        captured: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            captured.append(request)
            return httpx2.Response(
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
            "/catalog/facebook_ads/resolve",
            json={
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
            "/catalog/nope/resolve",
            json={"field": "account_id", "deps": {}},
        )
        assert resp.status_code == 404

    def test_non_provider_field_400(self, source_catalog: il.Catalog):
        resp = _client(source_catalog).post(
            "/catalog/facebook_ads/resolve",
            json={"field": "dataset", "deps": {}},
        )
        assert resp.status_code == 400


def _check(catalog: il.Catalog, config: dict) -> httpx2.Response:
    return _client(catalog).post(
        "/catalog/facebook_ads_connection/check",
        json={"config": config},
    )


class TestCheck:
    def test_live_check_passes(self, connection_catalog: il.Catalog, mock_graph):
        mock_graph(lambda request: httpx2.Response(200, json={"data": []}))

        resp = _check(connection_catalog, CONNECTION_CONFIG)

        assert resp.status_code == 200
        body = resp.json()
        assert (body["ok"], body["live"]) == (True, True)

    def test_rejected_credentials_reported_as_auth(self, connection_catalog: il.Catalog, mock_graph):
        mock_graph(lambda request: httpx2.Response(401, json={"error": "bad token"}))

        body = _check(connection_catalog, CONNECTION_CONFIG).json()

        assert (body["ok"], body["live"], body["category"]) == (False, True, "auth")

    def test_unreachable_provider_reported_as_network(self, connection_catalog: il.Catalog, mock_graph):
        def handler(request: httpx2.Request) -> httpx2.Response:
            raise httpx2.ConnectError("no route to host")

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
            "/catalog/uncheckable_connection/check",
            json={"config": {"api_key": "k"}},
        )

        body = resp.json()
        assert (body["ok"], body["live"]) == (True, False)

    def test_unknown_component_404(self, connection_catalog: il.Catalog):
        resp = _client(connection_catalog).post("/catalog/nope/check", json={"config": {}})
        assert resp.status_code == 404

    def test_non_connection_component_404(self, source_catalog: il.Catalog):
        resp = _client(source_catalog).post(
            "/catalog/facebook_ads/check", json={"config": {}}
        )
        assert resp.status_code == 404


class TestHandleError:
    """``handle_error`` maps a provider failure to a status, never a traceback."""

    @staticmethod
    def _status_error(status: int) -> httpx2.HTTPStatusError:
        request = httpx2.Request("GET", "https://provider.example.com/x")
        return httpx2.HTTPStatusError(
            "boom", request=request, response=httpx2.Response(status, request=request)
        )

    @pytest.mark.parametrize("status", [401, 403])
    def test_an_auth_failure_keeps_its_status(self, status: int) -> None:
        with pytest.raises(HTTPException) as excinfo:
            catalog_module.handle_error(self._status_error(status), "resolving facebook.ads_stats")

        assert excinfo.value.status_code == status
        assert "Authorization failed while resolving facebook.ads_stats." == excinfo.value.detail

    def test_a_provider_404_stays_a_404(self) -> None:
        with pytest.raises(HTTPException) as excinfo:
            catalog_module.handle_error(self._status_error(404), "resolving x")

        assert excinfo.value.status_code == 404
        assert "Resource not found while resolving x." == excinfo.value.detail

    def test_another_provider_status_falls_through_to_500(self) -> None:
        with pytest.raises(HTTPException) as excinfo:
            catalog_module.handle_error(self._status_error(503), "resolving x")

        assert excinfo.value.status_code == 500
        assert excinfo.value.detail == "Failed resolving x."

    def test_an_http_exception_is_re_raised_as_is(self) -> None:
        original = HTTPException(status_code=409, detail="conflict")

        with pytest.raises(HTTPException) as excinfo:
            catalog_module.handle_error(original, "resolving x")

        assert excinfo.value is original

    def test_anything_else_becomes_a_500(self) -> None:
        with pytest.raises(HTTPException) as excinfo:
            catalog_module.handle_error(RuntimeError("kaboom"), "resolving x")

        assert excinfo.value.status_code == 500
        assert excinfo.value.detail == "Failed resolving x."


class TestCheckResponseFromFailure:
    """A failed connection check is a categorised result, never a raised error."""

    @staticmethod
    def _status_error(status: int) -> httpx2.HTTPStatusError:
        request = httpx2.Request("GET", "https://provider.example.com/x")
        return httpx2.HTTPStatusError(
            "boom", request=request, response=httpx2.Response(status, request=request)
        )

    def test_a_connection_check_error_carries_its_own_message(self) -> None:
        from interloper.errors import ConnectionCheckError

        response = catalog_module.CheckResponse.from_failure(
            ConnectionCheckError("missing httpx2 extra"), "facebook_ads"
        )

        assert (response.ok, response.live, response.category) == (False, True, "error")
        assert response.message == "missing httpx2 extra"

    @pytest.mark.parametrize("status", [401, 403])
    def test_a_rejected_credential_is_categorised_as_auth(self, status: int) -> None:
        response = catalog_module.CheckResponse.from_failure(self._status_error(status), "fb")

        assert response.category == "auth"
        assert response.message == "The provider rejected the credentials."

    def test_another_provider_status_is_reported_verbatim(self) -> None:
        response = catalog_module.CheckResponse.from_failure(self._status_error(503), "fb")

        assert response.category == "error"
        assert response.message == "The provider responded with HTTP 503."

    @pytest.mark.parametrize(
        "exception",
        [TimeoutError("slow"), httpx2.TimeoutException("slow")],
    )
    def test_a_timeout_is_categorised_as_network(self, exception: Exception) -> None:
        response = catalog_module.CheckResponse.from_failure(exception, "fb")

        assert response.category == "network"
        assert response.message == "The provider did not respond in time."

    def test_an_unreachable_provider_is_categorised_as_network(self) -> None:
        response = catalog_module.CheckResponse.from_failure(httpx2.ConnectError("no route"), "fb")

        assert response.category == "network"
        assert response.message == "The provider could not be reached."

    def test_anything_else_is_a_generic_error(self) -> None:
        # The raw exception text may carry credentials, so it is not echoed.
        response = catalog_module.CheckResponse.from_failure(RuntimeError("token=SECRET"), "fb")

        assert response.category == "error"
        assert response.message == "The connection check failed unexpectedly."
        assert "SECRET" not in response.message


class TestResolveEdgeCases:
    """``POST /catalog/{key}/resolve``: the guards between the field and the provider."""

    def test_an_unknown_relation_name_is_a_400(
        self, source_catalog: il.Catalog, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The FetchField names a relation the component does not declare.
        from interloper_assets.facebook_ads.source import FacebookAds

        monkeypatch.setattr(FacebookAds, "relations", {})

        response = _client(source_catalog).post(
            "/catalog/facebook_ads/resolve",
            json={"field": "account_id", "deps": {}},
        )

        assert response.status_code == 400
        assert "Relation 'connection' not found" in response.json()["detail"]
        assert "not declared from a component class" in response.json()["detail"]

    def test_credentials_that_cannot_build_the_resource_are_a_400_without_their_values(
        self, source_catalog: il.Catalog
    ) -> None:
        response = _client(source_catalog).post(
            "/catalog/facebook_ads/resolve",
            json={
                "field": "account_id",
                "deps": {"connection": {"access_token": ["s3cret-value"]}},
            },
        )

        assert response.status_code == 400
        detail = response.json()["detail"]
        assert detail.startswith("Cannot resolve 'account_id' from the 'connection' credentials given: ValidationError")
        assert "access_token" in detail
        assert "s3cret-value" not in detail

    def test_a_provider_failure_is_mapped_not_raised(
        self, source_catalog: il.Catalog, mock_graph
    ) -> None:
        def handler(request: httpx2.Request) -> httpx2.Response:
            raise httpx2.ConnectError("no route to host")

        mock_graph(handler)

        response = _client(source_catalog).post(
            "/catalog/facebook_ads/resolve",
            json={"field": "account_id", "deps": {"connection": CONNECTION_CONFIG}},
        )

        assert response.status_code == 500
        assert response.json()["detail"].startswith("Failed resolving facebook_ads.account_id")

    def test_a_relation_that_is_not_a_fetch_provider_is_a_403(
        self, source_catalog: il.Catalog, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Validated at catalog build, so this is a defensive guard.
        monkeypatch.setattr(catalog_module, "is_fetch_field_provider", lambda fn: False)

        response = _client(source_catalog).post(
            "/catalog/facebook_ads/resolve",
            json={"field": "account_id", "deps": {"connection": CONNECTION_CONFIG}},
        )

        assert response.status_code == 403
        assert "is not a fetch provider" in response.json()["detail"]


class TestCheckFalsyResult:
    """A check that returns falsy is a failure, not a pass."""

    def test_a_false_check_is_reported_as_an_error(
        self, connection_catalog: il.Catalog, mock_graph, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(FacebookAdsConnection, "check", lambda self: False)

        body = _check(connection_catalog, CONNECTION_CONFIG).json()

        assert (body["ok"], body["live"], body["category"]) == (False, True, "error")
        assert body["message"] == "The connection check failed."
