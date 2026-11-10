"""Tests for ``interloper_databricks.connection``."""

import base64

import httpx2
import pytest
from pydantic import ValidationError

from interloper_databricks import DatabricksConnection
from interloper_databricks import connection as connection_module

HOST = "https://dbc-a1b2345c-d6e7.cloud.databricks.com"


def form(request):
    return dict(httpx2.QueryParams(request.content.decode()))


def _pat(**overrides):
    return DatabricksConnection(id="dbx", host=HOST, access_token="<pat>", **overrides)


def _m2m(**overrides):
    return DatabricksConnection(id="dbx", host=HOST, client_id="<client-id>", client_secret="<secret>", **overrides)


class TestHost:
    def test_bare_hostname_gets_a_scheme(self):
        conn = DatabricksConnection(host="dbc-a1b2345c-d6e7.cloud.databricks.com/", access_token="<pat>")

        assert conn.host == HOST
        assert conn.hostname == "dbc-a1b2345c-d6e7.cloud.databricks.com"

    def test_url_is_kept(self):
        assert _pat().host == HOST


class TestCredentials:
    def test_access_token_alone(self):
        assert _pat().access_token == "<pat>"

    def test_service_principal_alone(self):
        conn = _m2m()

        assert (conn.client_id, conn.client_secret, conn.access_token) == ("<client-id>", "<secret>", None)

    def test_both_are_rejected(self):
        with pytest.raises(ValidationError, match="not both"):
            _m2m(access_token="<pat>")

    def test_neither_is_rejected(self):
        with pytest.raises(ValidationError, match="Set a service principal"):
            DatabricksConnection(host=HOST)

    @pytest.mark.parametrize("half", [{"client_id": "<client-id>"}, {"client_secret": "<secret>"}])
    def test_half_a_service_principal_is_rejected(self, half):
        with pytest.raises(ValidationError, match="needs both"):
            DatabricksConnection(host=HOST, **half)

    def test_blank_fields_count_as_unset(self):
        conn = DatabricksConnection(host=HOST, client_id="", client_secret="", access_token="<pat>")

        assert conn.client_id is None
        assert conn.client_secret is None

    def test_loads_databricks_environment_names(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_HOST", HOST)
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "<client-id>")
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "<secret>")

        conn = DatabricksConnection()

        assert (conn.host, conn.client_id, conn.client_secret) == (HOST, "<client-id>", "<secret>")


class TestToken:
    def test_access_token_needs_no_exchange(self, workspace):
        assert _pat().token() == "<pat>"
        assert workspace.requests == []

    def test_service_principal_exchanges_client_credentials(self, workspace):
        assert _m2m().token() == "token-1"

        (request,) = workspace.requests
        assert str(request.url) == f"{HOST}/oidc/v1/token"
        assert request.method == "POST"
        assert form(request) == {"grant_type": "client_credentials", "scope": "all-apis"}
        expected = base64.b64encode(b"<client-id>:<secret>").decode()
        assert request.headers["Authorization"] == f"Basic {expected}"

    def test_token_is_cached_until_close_to_expiry(self, workspace):
        conn = _m2m()

        assert conn.token() == conn.token() == "token-1"
        assert len(workspace.requests) == 1

    def test_token_near_expiry_is_replaced(self, workspace, monkeypatch):
        conn = _m2m()
        clock = [1000.0]
        monkeypatch.setattr(connection_module.time, "monotonic", lambda: clock[0])

        assert conn.token() == "token-1"
        clock[0] += 3600 - 299
        assert conn.token() == "token-2"

    def test_rejected_secret_raises(self, workspace):
        workspace.token_status = 401

        with pytest.raises(httpx2.HTTPStatusError):
            _m2m().token()


class TestConnect:
    def test_access_token_goes_to_the_connector(self, session):
        _pat().connect(http_path="/sql/1.0/warehouses/abc", catalog="main")

        assert session.connect_kwargs == {
            "server_hostname": "dbc-a1b2345c-d6e7.cloud.databricks.com",
            "access_token": "<pat>",
            "http_path": "/sql/1.0/warehouses/abc",
            "catalog": "main",
        }

    def test_service_principal_goes_through_a_credentials_provider(self, session, workspace):
        _m2m().connect(http_path="/sql/1.0/warehouses/abc")

        kwargs = session.connect_kwargs
        assert set(kwargs) == {"server_hostname", "credentials_provider", "http_path"}
        assert "access_token" not in kwargs
        header_factory = kwargs["credentials_provider"]()
        assert header_factory() == {"Authorization": "Bearer token-1"}
        assert header_factory() == {"Authorization": "Bearer token-1"}
        assert len(workspace.to("/oidc/v1/token")) == 1


class TestClient:
    def test_is_cached(self):
        conn = _pat()

        assert conn.client is conn.client

    def test_carries_base_url_and_bearer_auth(self, workspace):
        _pat().check()

        (request,) = workspace.requests
        assert str(request.url) == f"{HOST}/api/2.0/preview/scim/v2/Me"
        assert request.headers["Authorization"] == "Bearer <pat>"


class TestCheck:
    def test_access_token(self, workspace):
        workspace.respond("/api/2.0/preview/scim/v2/Me", userName="loader")

        assert _pat().check() is True

    def test_service_principal_exchanges_then_calls_me(self, workspace):
        assert _m2m().check() is True

        assert workspace.paths == ["/oidc/v1/token", "/api/2.0/preview/scim/v2/Me"]
        assert workspace.requests[1].headers["Authorization"] == "Bearer token-1"

    def test_rejected_token_raises(self, workspace):
        workspace.respond("/api/2.0/preview/scim/v2/Me", status=401, error_code="UNAUTHENTICATED")

        with pytest.raises(httpx2.HTTPStatusError):
            _pat().check()


class TestProviders:
    def test_warehouses_page_and_sort_by_name(self, workspace):
        path = "/api/2.0/sql/warehouses"
        workspace.respond(
            path,
            warehouses=[{"id": "b", "name": "Serverless", "odbc_params": {"path": "/sql/1.0/warehouses/b"}}],
            next_page_token="page2",
        ).respond(
            path,
            warehouses=[{"id": "a", "name": "adhoc", "odbc_params": {"path": "/sql/1.0/warehouses/a"}}],
        )

        assert _pat().warehouses() == [
            {"path": "/sql/1.0/warehouses/a", "name": "adhoc"},
            {"path": "/sql/1.0/warehouses/b", "name": "Serverless"},
        ]
        assert workspace.params(path) == [{}, {"page_token": "page2"}]

    def test_no_warehouses(self, workspace):
        assert _pat().warehouses() == []

    def test_catalogs_ask_for_the_paginated_form(self, workspace):
        path = "/api/2.1/unity-catalog/catalogs"
        workspace.respond(path, catalogs=[{"name": "main"}, {"name": "Analytics"}], next_page_token="n")
        workspace.respond(path, catalogs=[{"name": "dev"}])

        assert _pat().catalogs() == [{"name": "Analytics"}, {"name": "dev"}, {"name": "main"}]
        assert workspace.params(path) == [{"max_results": "0"}, {"max_results": "0", "page_token": "n"}]
