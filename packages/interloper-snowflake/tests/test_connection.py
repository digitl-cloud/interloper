"""Tests for ``interloper_snowflake.connection``."""

from interloper_snowflake import SnowflakeConnection


def _connection(**overrides):
    return SnowflakeConnection(
        id="sf", account="xy12345.eu-central-1", user="loader", password="<password>", **overrides
    )


class TestClient:
    def test_connects_with_autocommit(self, session):
        _ = _connection(role="LOADER").client

        assert session.connect_kwargs == {
            "account": "xy12345.eu-central-1",
            "user": "loader",
            "password": "<password>",
            "role": "LOADER",
            "autocommit": True,
        }

    def test_connect_passes_session_settings(self, session):
        _connection().connect(warehouse="LOAD_WH", database="ANALYTICS")

        assert session.connect_kwargs["warehouse"] == "LOAD_WH"
        assert session.connect_kwargs["database"] == "ANALYTICS"
        assert session.connect_kwargs["autocommit"] is True

    def test_role_defaults_to_none(self, session):
        _ = _connection().client

        assert session.connect_kwargs["role"] is None

    def test_is_cached(self, session):
        conn = _connection()

        assert conn.client is conn.client


class TestCheck:
    def test_runs_select_one(self, session):
        assert _connection().check() is True
        assert session.statements == [("SELECT 1", None)]


class TestProviders:
    def test_databases_read_the_name_column_sorted(self, session):
        session.respond(
            "SHOW DATABASES",
            columns=["created_on", "name", "owner"],
            rows=[("t", "RAW", "x"), ("t", "analytics", "x"), ("t", "Marts", "x")],
        )

        assert _connection().databases() == [{"name": "analytics"}, {"name": "Marts"}, {"name": "RAW"}]
        assert session.statements == [("SHOW DATABASES", None)]

    def test_warehouses_read_the_name_column_sorted(self, session):
        session.respond("SHOW WAREHOUSES", columns=["name", "state"], rows=[("LOAD_WH", "STARTED"), ("adhoc", "x")])

        assert _connection().warehouses() == [{"name": "adhoc"}, {"name": "LOAD_WH"}]
        assert session.statements == [("SHOW WAREHOUSES", None)]
