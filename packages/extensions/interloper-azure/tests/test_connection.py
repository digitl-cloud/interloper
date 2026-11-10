"""Tests for ``interloper_azure.connection``."""

from typing import Any

import httpx2
import pytest

from interloper_azure import AzureConnection

FABRIC_TOKEN = "Bearer token-for-https://api.fabric.microsoft.com/.default"


def _connection() -> AzureConnection:
    return AzureConnection(tenant_id="tenant", client_id="client", client_secret="<secret>")


def _warehouses(server: str) -> dict:
    return {"value": [{"id": "w1", "displayName": "Sales", "properties": {"connectionString": server}}]}


class TestCredential:
    def test_built_from_the_principal(self):
        credential: Any = _connection().credential
        assert credential.args == ("tenant", "client", "<secret>")

    def test_is_cached(self):
        connection = _connection()

        assert connection.credential is connection.credential

    def test_token_asks_for_the_scope(self):
        connection = _connection()

        assert connection.token("https://database.windows.net/.default") == (
            "token-for-https://database.windows.net/.default"
        )
        credential: Any = connection.credential
        assert credential.scopes == ["https://database.windows.net/.default"]

    def test_loads_the_standard_environment_names(self, monkeypatch):
        monkeypatch.setenv("AZURE_TENANT_ID", "env-tenant")
        monkeypatch.setenv("AZURE_CLIENT_ID", "env-client")
        monkeypatch.setenv("AZURE_CLIENT_SECRET", "env-secret")

        connection = AzureConnection()

        assert (connection.tenant_id, connection.client_id, connection.client_secret) == (
            "env-tenant",
            "env-client",
            "env-secret",
        )


class TestCheck:
    def test_lists_one_page_of_workspaces(self, fabric):
        fabric.ok("/workspaces", value=[], continuationToken="more")

        assert _connection().check() is True
        assert [str(request.url) for request in fabric.requests] == ["https://api.fabric.microsoft.com/v1/workspaces"]
        assert fabric.requests[0].headers["Authorization"] == FABRIC_TOKEN

    def test_refused_principal_raises(self, fabric):
        fabric.respond("/workspaces", httpx2.Response(401, json={"errorCode": "Unauthorized"}))

        with pytest.raises(httpx2.HTTPStatusError):
            _connection().check()

    def test_is_checkable(self):
        assert AzureConnection.checkable()


class TestWorkspaces:
    def test_lists_workspaces_with_their_sql_endpoint(self, fabric):
        fabric.ok(
            "/workspaces",
            value=[{"id": "ws-b", "displayName": "marketing"}, {"id": "ws-a", "displayName": "Finance"}],
        )
        fabric.ok("/workspaces/ws-b/warehouses", **_warehouses("b.datawarehouse.fabric.microsoft.com"))
        fabric.ok("/workspaces/ws-a/warehouses", **_warehouses("a.datawarehouse.fabric.microsoft.com"))

        assert _connection().workspaces() == [
            {"id": "ws-a", "name": "Finance", "server": "a.datawarehouse.fabric.microsoft.com"},
            {"id": "ws-b", "name": "marketing", "server": "b.datawarehouse.fabric.microsoft.com"},
        ]
        assert all(request.headers["Authorization"] == FABRIC_TOKEN for request in fabric.requests)

    def test_follows_continuation_tokens(self, fabric):
        fabric.ok("/workspaces", value=[{"id": "ws-a", "displayName": "A"}], continuationToken="page2")
        fabric.ok("/workspaces", value=[{"id": "ws-b", "displayName": "B"}])
        fabric.ok("/workspaces/ws-a/warehouses", **_warehouses("a"))
        fabric.ok("/workspaces/ws-b/warehouses", **_warehouses("b"))

        assert [option["name"] for option in _connection().workspaces()] == ["A", "B"]
        assert fabric.requests[1].url.params["continuationToken"] == "page2"

    def test_skips_workspaces_without_a_warehouse(self, fabric):
        fabric.ok("/workspaces", value=[{"id": "ws-a", "displayName": "A"}, {"id": "ws-b", "displayName": "B"}])
        fabric.ok("/workspaces/ws-b/warehouses", **_warehouses("b"))

        assert _connection().workspaces() == [{"id": "ws-b", "name": "B", "server": "b"}]
        assert fabric.paths == ["/workspaces", "/workspaces/ws-a/warehouses", "/workspaces/ws-b/warehouses"]

    def test_http_error_raises(self, fabric):
        fabric.ok("/workspaces", value=[{"id": "ws-a", "displayName": "A"}])
        fabric.respond("/workspaces/ws-a/warehouses", httpx2.Response(403))

        with pytest.raises(httpx2.HTTPStatusError):
            _connection().workspaces()


class TestMetadata:
    def test_decorator(self):
        definition = AzureConnection.definition()
        assert (definition.key, definition.name, definition.icon, definition.tags) == (
            "azure_connection",
            "Microsoft Azure",
            "icon:azure",
            ["Cloud"],
        )

    def test_secret_is_a_password_field(self):
        extra = AzureConnection.model_fields["client_secret"].json_schema_extra
        assert extra["x-widget"] == "password"
