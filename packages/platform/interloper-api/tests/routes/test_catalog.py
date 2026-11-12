"""Tests for ``interloper_api.routes.catalog``: the definitions and the operations on a type."""

from __future__ import annotations

from uuid import uuid4

import httpx2
import interloper as il
import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from interloper.errors import ConnectionCheckError

from interloper_api.app import ORG_PREFIX, install_error_handlers
from interloper_api.dependencies import get_catalog, get_store, require_editor, require_viewer
from interloper_api.routes import catalog as catalog_module

_BASE_URL = f"http://testserver/organisations/{uuid4()}"
_ACCOUNTS_URL = "https://ads.example.com/accounts"


class AdsConnection(il.Connection):
    """A connection to an ads API, whose reachable accounts back the source's ``account_id``."""

    access_token: str = il.SecretField()

    @il.fetch_field_provider
    async def accounts(self) -> list[dict[str, str]]:
        """List the accounts the access token reaches.

        Returns:
            The options of the source's ``account_id`` field.
        """
        async with httpx2.AsyncClient() as client:
            response = await client.get(_ACCOUNTS_URL, params={"access_token": self.access_token})
            response.raise_for_status()
        return response.json()["data"]

    async def check(self) -> bool:
        """Prove the credentials work by listing the accounts.

        Returns:
            True: a credential failure raises out of the lookup.
        """
        await self.accounts()
        return True


class AdsSource(il.Source):
    """A source whose account is picked from the ones its connection reaches."""

    connection: AdsConnection

    account_id: str = il.FetchField(provider="connection.accounts", label_key="name", value_key="account_id")


def _app() -> FastAPI:
    app = FastAPI()
    install_error_handlers(app)
    app.include_router(catalog_module.org_router, prefix=ORG_PREFIX)
    app.dependency_overrides[get_store] = lambda: None
    app.dependency_overrides[get_catalog] = lambda: il.Catalog(components={})
    return app


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("get", "/catalog"),
        ("get", "/catalog/resource-kinds"),
        ("post", "/catalog/ads_source/resolve"),
        ("post", "/catalog/ads_connection/check"),
    ],
)
def test_unauthenticated_requests_rejected(method: str, path: str):
    resp = TestClient(_app(), base_url=_BASE_URL).request(method, path, json={"field": "x"})
    assert resp.status_code == 401


def test_viewer_can_read_catalog():
    app = _app()
    app.dependency_overrides[require_viewer] = lambda: None
    client = TestClient(app, base_url=_BASE_URL)

    assert client.get("/catalog").status_code == 200
    assert client.get("/catalog/resource-kinds").status_code == 200


def test_the_catalog_is_served_whole():
    catalog = il.Catalog.from_assets([AdsSource])
    app = _app()
    app.dependency_overrides[get_catalog] = lambda: catalog
    app.dependency_overrides[require_viewer] = lambda: None

    response = TestClient(app, base_url=_BASE_URL).get("/catalog")

    assert response.status_code == 200
    assert response.json() == catalog.dump()


def test_resource_kinds_are_the_kinds_anchored_under_resource():
    catalog = il.Catalog.from_assets([AdsSource])
    app = _app()
    app.dependency_overrides[get_catalog] = lambda: catalog
    app.dependency_overrides[require_viewer] = lambda: None

    response = TestClient(app, base_url=_BASE_URL).get("/catalog/resource-kinds")

    assert response.json() == ["connection"]


@pytest.mark.parametrize("path", ["/catalog/ads_source", "/catalog/kind/source"])
def test_the_per_key_and_per_kind_reads_are_gone(path: str):
    app = _app()
    app.dependency_overrides[require_viewer] = lambda: None

    assert TestClient(app, base_url=_BASE_URL).get(path).status_code == 404


# -- Operations on a type ------------------------------------------------------


CONNECTION_CONFIG = {"access_token": "TOK", "_id": "x"}


class UncheckableConnection(il.Connection):
    """A connection with no ``check()`` hook, module-level so its path imports."""

    api_key: str = il.SecretField()


def _client(catalog: il.Catalog) -> TestClient:
    app = FastAPI()
    install_error_handlers(app)
    app.include_router(catalog_module.org_router, prefix=ORG_PREFIX)
    app.dependency_overrides[require_viewer] = lambda: None
    app.dependency_overrides[require_editor] = lambda: None
    app.dependency_overrides[get_catalog] = lambda: catalog
    return TestClient(app, base_url=_BASE_URL)


@pytest.fixture
def source_catalog() -> il.Catalog:
    return il.Catalog.from_assets([AdsSource])


@pytest.fixture
def connection_catalog() -> il.Catalog:
    return il.Catalog(components={AdsConnection.key: AdsConnection.definition()})


@pytest.fixture
def mock_provider(monkeypatch: pytest.MonkeyPatch):
    """Route the ads connection's httpx2 client through a mock transport.

    Returns:
        A function installing the handler that answers the connection's requests.
    """

    def install(handler) -> None:
        real_client = httpx2.AsyncClient

        def factory(*args, **kwargs):
            kwargs["transport"] = httpx2.MockTransport(handler)
            return real_client(*args, **kwargs)

        monkeypatch.setattr(httpx2, "AsyncClient", factory)

    return install


class TestResolve:
    def test_resolves_provider_options(self, source_catalog: il.Catalog, mock_provider):
        captured: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            captured.append(request)
            return httpx2.Response(200, json={"data": [{"account_id": "111", "name": "Acme"}]})

        mock_provider(handler)

        resp = _client(source_catalog).post(
            "/catalog/ads_source/resolve",
            json={
                "field": "account_id",
                # Credentials carry an internal _id marker that must be stripped.
                "deps": {"connection": CONNECTION_CONFIG},
            },
        )

        assert resp.status_code == 200
        assert resp.json() == [{"account_id": "111", "name": "Acme"}]
        # The connection's access token reached the provider; _id was not sent as a field.
        assert captured[0].url.params["access_token"] == "TOK"

    def test_unknown_component_404(self, source_catalog: il.Catalog):
        resp = _client(source_catalog).post(
            "/catalog/nope/resolve",
            json={"field": "account_id", "deps": {}},
        )
        assert resp.status_code == 404

    def test_non_provider_field_400(self, source_catalog: il.Catalog):
        resp = _client(source_catalog).post(
            "/catalog/ads_source/resolve",
            json={"field": "dataset", "deps": {}},
        )
        assert resp.status_code == 400


def _check(catalog: il.Catalog, config: dict) -> httpx2.Response:
    return _client(catalog).post(
        "/catalog/ads_connection/check",
        json={"config": config},
    )


class TestCheck:
    def test_live_check_passes(self, connection_catalog: il.Catalog, mock_provider):
        mock_provider(lambda request: httpx2.Response(200, json={"data": []}))

        resp = _check(connection_catalog, CONNECTION_CONFIG)

        assert resp.status_code == 200
        body = resp.json()
        assert (body["ok"], body["live"]) == (True, True)

    def test_rejected_credentials_reported_as_auth(self, connection_catalog: il.Catalog, mock_provider):
        mock_provider(lambda request: httpx2.Response(401, json={"error": "bad token"}))

        body = _check(connection_catalog, CONNECTION_CONFIG).json()

        assert (body["ok"], body["live"], body["category"]) == (False, True, "auth")

    def test_unreachable_provider_reported_as_network(self, connection_catalog: il.Catalog, mock_provider):
        def handler(request: httpx2.Request) -> httpx2.Response:
            raise httpx2.ConnectError("no route to host")

        mock_provider(handler)

        body = _check(connection_catalog, CONNECTION_CONFIG).json()

        assert (body["ok"], body["live"], body["category"]) == (False, True, "network")

    def test_invalid_config_reports_field_errors(self, connection_catalog: il.Catalog):
        # Static tier: a missing required field never reaches the provider.
        body = _check(connection_catalog, {}).json()

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
        resp = _client(source_catalog).post("/catalog/ads_source/check", json={"config": {}})
        assert resp.status_code == 404


class TestHandleError:
    """``handle_error`` maps a provider failure to a status, never a traceback."""

    @staticmethod
    def _status_error(status: int) -> httpx2.HTTPStatusError:
        request = httpx2.Request("GET", "https://provider.example.com/x")
        return httpx2.HTTPStatusError("boom", request=request, response=httpx2.Response(status, request=request))

    @pytest.mark.parametrize("status", [401, 403])
    def test_an_auth_failure_keeps_its_status(self, status: int) -> None:
        with pytest.raises(HTTPException) as excinfo:
            catalog_module.handle_error(self._status_error(status), "resolving ads_source.account_id")

        assert excinfo.value.status_code == status
        assert "Authorization failed while resolving ads_source.account_id." == excinfo.value.detail

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

    def test_a_curated_provider_message_is_the_detail(self) -> None:
        message = "Ads API error 40001: the token was issued to another app"

        with pytest.raises(HTTPException) as excinfo:
            catalog_module.handle_error(ConnectionCheckError(message), "resolving x")

        assert excinfo.value.status_code == 500
        assert excinfo.value.detail == message

    def test_anything_else_becomes_a_500(self) -> None:
        with pytest.raises(HTTPException) as excinfo:
            catalog_module.handle_error(RuntimeError("kaboom"), "resolving x")

        assert excinfo.value.status_code == 500
        assert excinfo.value.detail == "Failed resolving x."


class TestResolveEdgeCases:
    """``POST /catalog/{key}/resolve``: the guards between the field and the provider."""

    def test_an_unknown_relation_name_is_a_400(
        self, source_catalog: il.Catalog, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The FetchField names a relation the component does not declare.
        monkeypatch.setattr(AdsSource, "relations", {})

        response = _client(source_catalog).post(
            "/catalog/ads_source/resolve",
            json={"field": "account_id", "deps": {}},
        )

        assert response.status_code == 400
        assert "Relation 'connection' not found" in response.json()["detail"]
        assert "not declared from a component class" in response.json()["detail"]

    def test_credentials_that_cannot_build_the_resource_are_a_400_without_their_values(
        self, source_catalog: il.Catalog
    ) -> None:
        response = _client(source_catalog).post(
            "/catalog/ads_source/resolve",
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

    def test_a_provider_failure_is_mapped_not_raised(self, source_catalog: il.Catalog, mock_provider) -> None:
        def handler(request: httpx2.Request) -> httpx2.Response:
            raise httpx2.ConnectError("no route to host")

        mock_provider(handler)

        response = _client(source_catalog).post(
            "/catalog/ads_source/resolve",
            json={"field": "account_id", "deps": {"connection": CONNECTION_CONFIG}},
        )

        assert response.status_code == 500
        assert response.json()["detail"].startswith("Failed resolving ads_source.account_id")

    def test_a_relation_that_is_not_a_fetch_provider_is_a_403(
        self, source_catalog: il.Catalog, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Validated at catalog build, so this is a defensive guard.
        monkeypatch.setattr(catalog_module, "is_fetch_field_provider", lambda fn: False)

        response = _client(source_catalog).post(
            "/catalog/ads_source/resolve",
            json={"field": "account_id", "deps": {"connection": CONNECTION_CONFIG}},
        )

        assert response.status_code == 403
        assert "is not a fetch provider" in response.json()["detail"]


class TestCheckFalsyResult:
    """A check that returns falsy is a failure, not a pass."""

    def test_a_false_check_is_reported_as_an_error(
        self, connection_catalog: il.Catalog, mock_provider, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(AdsConnection, "check", lambda self: False)

        body = _check(connection_catalog, CONNECTION_CONFIG).json()

        assert (body["ok"], body["live"], body["category"]) == (False, True, "error")
        assert body["message"] == "The connection check failed."
