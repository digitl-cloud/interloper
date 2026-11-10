"""Tests for ``interloper_clickhouse.connection``."""

from interloper_clickhouse import ClickHouseConnection


def _connection(**overrides):
    return ClickHouseConnection(
        id="ch", host="abc123.eu-central-1.aws.clickhouse.cloud", password="<password>", **overrides
    )


class TestFields:
    def test_defaults_suit_clickhouse_cloud(self):
        connection = _connection()

        assert connection.username == "default"
        assert connection.secure is True
        assert connection.port is None

    def test_loads_from_the_environment(self, monkeypatch):
        monkeypatch.setenv("CLICKHOUSE_HOST", "clickhouse.internal")
        monkeypatch.setenv("CLICKHOUSE_PORT", "8123")
        monkeypatch.setenv("CLICKHOUSE_USERNAME", "loader")
        monkeypatch.setenv("CLICKHOUSE_PASSWORD", "<password>")
        monkeypatch.setenv("CLICKHOUSE_SECURE", "false")

        connection = ClickHouseConnection(id="ch")

        assert (connection.host, connection.port, connection.username, connection.secure) == (
            "clickhouse.internal",
            8123,
            "loader",
            False,
        )


class TestClient:
    def test_connects_without_session_ids(self, clickhouse):
        _ = _connection(username="loader").client

        assert clickhouse.get_client_kwargs == {
            "host": "abc123.eu-central-1.aws.clickhouse.cloud",
            "port": None,
            "username": "loader",
            "password": "<password>",
            "secure": True,
            "autogenerate_session_id": False,
        }

    def test_self_hosted_port_and_plain_http(self, clickhouse):
        _ = _connection(port=8123, secure=False).client

        assert clickhouse.get_client_kwargs["port"] == 8123
        assert clickhouse.get_client_kwargs["secure"] is False

    def test_is_cached(self, clickhouse):
        connection = _connection()

        assert connection.client is connection.client


class TestCheck:
    def test_runs_select_one(self, clickhouse):
        assert _connection().check() is True
        assert clickhouse.commands == ["SELECT 1"]


class TestDatabases:
    def test_lists_user_databases_sorted(self, clickhouse):
        for name in ("raw", "Marts", "analytics"):
            clickhouse.client.command(f"CREATE DATABASE `{name}`")

        names = [option["name"] for option in _connection().databases()]

        assert {"raw", "Marts", "analytics"} <= set(names)
        assert names == sorted(names, key=str.lower)
        assert not {"system", "information_schema", "INFORMATION_SCHEMA"} & set(names)
