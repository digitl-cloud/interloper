"""Tests for ``interloper_duckdb.connection``."""

from unittest.mock import patch

from interloper_duckdb import DuckDBConnection


class TestClient:
    def test_is_cached_across_calls(self, connection):
        assert connection.client is connection.client

    def test_local_file_opens_without_config(self, tmp_path):
        path = str(tmp_path / "local.duckdb")
        with patch("duckdb.connect") as connect:
            _ = DuckDBConnection(id="c", database=path).client
        connect.assert_called_once_with(path, config={})

    def test_motherduck_token_is_passed_as_config(self):
        with patch("duckdb.connect") as connect:
            _ = DuckDBConnection(id="c", database="md:my_db", motherduck_token="token").client
        connect.assert_called_once_with("md:my_db", config={"motherduck_token": "token"})

    def test_loads_from_the_environment(self, monkeypatch, tmp_path):
        monkeypatch.setenv("DUCKDB_DATABASE", str(tmp_path / "env.duckdb"))
        assert DuckDBConnection(id="c").database == str(tmp_path / "env.duckdb")


class TestCheck:
    def test_a_database_that_opens_passes(self, connection):
        assert connection.check() is True


class TestSchemas:
    def test_lists_user_schemas_sorted_without_system_ones(self, connection):
        cursor = connection.client.cursor()
        cursor.execute('CREATE SCHEMA "marketing"')
        cursor.execute('CREATE SCHEMA "Finance"')
        cursor.close()

        assert connection.schemas() == [{"name": "Finance"}, {"name": "main"}, {"name": "marketing"}]
