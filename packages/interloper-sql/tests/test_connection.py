"""Tests for ``interloper_sql.connection``."""

import pytest
from sqlalchemy import event, exc

from interloper_sql import SQLConnection


class TestEngine:
    def test_is_cached_across_calls(self, connection):
        assert connection.engine is connection.engine

    def test_pre_pings_pooled_connections(self, connection):
        assert connection.engine.pool._pre_ping is True

    def test_url_loads_from_the_environment(self, monkeypatch, tmp_path):
        url = f"sqlite:///{tmp_path / 'env.db'}"
        monkeypatch.setenv("SQL_URL", url)

        assert SQLConnection(id="sql").url == url


class TestCheck:
    def test_passes_on_a_reachable_database(self, connection):
        assert connection.check() is True

    def test_raises_on_an_unreachable_database(self, tmp_path):
        conn = SQLConnection(id="sql", url=f"sqlite:///{tmp_path / 'missing' / 'x.db'}")

        with pytest.raises(exc.OperationalError):
            conn.check()


class TestSchemas:
    def test_lists_schemas_sorted_case_insensitively(self, connection, tmp_path):
        # SQLite's schemas are its attached databases; attach on every pooled connection.
        @event.listens_for(connection.engine, "connect")
        def attach(dbapi_connection, _record):
            dbapi_connection.execute(f"ATTACH DATABASE '{tmp_path / 'z.db'}' AS Zeta")
            dbapi_connection.execute(f"ATTACH DATABASE '{tmp_path / 'a.db'}' AS alpha")

        assert connection.schemas() == [{"name": "alpha"}, {"name": "main"}, {"name": "Zeta"}]
