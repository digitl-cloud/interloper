"""Shared ClickHouse client backed by an embedded chDB engine.

``clickhouse_connect.get_client`` is replaced by a client on clickhouse-connect's
own chDB backend, so every statement the package renders runs on a real
ClickHouse engine in-process, through the same client methods it calls on a
server; nothing reaches the network. The client is wrapped in a recorder that
logs each call and can make a statement fail.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import clickhouse_connect
import pandas as pd
import pytest
from clickhouse_connect.driver.client import Client

from interloper_clickhouse.destination import _quote

_create_client = clickhouse_connect.get_client


class Recorder:
    """A client wrapper recording each call and failing the statements it is told to."""

    def __init__(self, client: Client) -> None:
        """Wrap a client, with no calls recorded yet.

        ``get_client_kwargs`` holds the arguments the package last passed to
        ``clickhouse_connect.get_client``.

        Args:
            client: The chDB-backed client the calls go to.
        """
        self.client = client
        self.calls: list[tuple[str, str, Any]] = []
        self.failures: dict[str, Exception] = {}
        self.inserts: list[tuple[str, pd.DataFrame, dict[str, Any]]] = []
        self.get_client_kwargs: dict[str, Any] = {}

    def _record(self, kind: str, sql: str, parameters: Any) -> None:
        """Log a call, then raise the failure scripted for the first prefix it starts with."""
        self.calls.append((kind, sql, parameters))
        for prefix, error in self.failures.items():
            if sql.startswith(prefix):
                raise error

    def command(self, cmd: str, parameters: Any = None, **kwargs: Any) -> Any:
        """Record and run a command.

        Returns:
            The client's result.
        """
        self._record("command", cmd, parameters)
        return self.client.command(cmd, parameters=parameters, **kwargs)

    def query(self, query: str, parameters: Any = None, **kwargs: Any) -> Any:
        """Record and run a query.

        Returns:
            The client's result.
        """
        self._record("query", query, parameters)
        return self.client.query(query, parameters=parameters, **kwargs)

    def query_df(self, query: str, parameters: Any = None, **kwargs: Any) -> pd.DataFrame:
        """Record and run a DataFrame query.

        Returns:
            The client's result.
        """
        self._record("query_df", query, parameters)
        return self.client.query_df(query, parameters=parameters, **kwargs)

    def insert_df(self, table: str, df: pd.DataFrame, **kwargs: Any) -> Any:
        """Record and run a DataFrame insert.

        Returns:
            The client's result.
        """
        self._record("insert_df", f"INSERT INTO {table}", None)
        self.inserts.append((table, df.copy(), kwargs))
        return self.client.insert_df(table=table, df=df, **kwargs)

    @property
    def commands(self) -> list[str]:
        """The commands run, oldest first.

        Returns:
            The statement texts.
        """
        return [sql for kind, sql, _ in self.calls if kind == "command"]

    def rows(self, sql: str) -> list[tuple[Any, ...]]:
        """Query the engine directly, without recording.

        Returns:
            The result rows.
        """
        return [tuple(row) for row in self.client.query(sql).result_rows]


@pytest.fixture(scope="session")
def engine() -> Iterator[Client]:
    """Open one in-memory chDB engine for the session; chDB allows one per process.

    Yields:
        A client on the engine.
    """
    client = _create_client(interface="chdb")
    yield client
    client.close()


@pytest.fixture
def clickhouse(engine: Client, monkeypatch: pytest.MonkeyPatch) -> Iterator[Recorder]:
    """Route ``clickhouse_connect.get_client`` to a recorder over the chDB engine.

    Databases and ``default`` tables a test creates are dropped afterwards.

    Yields:
        The recorder the test scripts and inspects.
    """
    recorder = Recorder(engine)

    def get_client(**kwargs: Any) -> Recorder:
        recorder.get_client_kwargs = kwargs
        return recorder

    monkeypatch.setattr(clickhouse_connect, "get_client", get_client)
    databases = set(recorder.rows("SELECT name FROM system.databases"))
    tables = set(recorder.rows("SELECT name FROM system.tables WHERE database = 'default'"))
    yield recorder
    for (name,) in set(recorder.rows("SELECT name FROM system.databases")) - databases:
        engine.command(f"DROP DATABASE IF EXISTS {_quote(name)} SYNC")
    for (name,) in set(recorder.rows("SELECT name FROM system.tables WHERE database = 'default'")) - tables:
        engine.command(f"DROP TABLE IF EXISTS `default`.{_quote(name)} SYNC")
