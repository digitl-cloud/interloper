"""Shared Snowflake connector fake.

Every statement goes through the connection's ``client`` session, so the
fake replaces ``snowflake.connector.connect`` with a session that records
each statement and serves scripted results; nothing reaches the network.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd
import pytest
import snowflake.connector


@dataclass
class Result:
    """What a scripted statement returns.

    Attributes:
        rows: What ``fetchall`` returns.
        columns: The column names ``description`` reports.
        frame: What ``fetch_pandas_all`` returns.
    """

    rows: list[tuple[Any, ...]] = field(default_factory=list)
    columns: list[str] = field(default_factory=list)
    frame: pd.DataFrame | None = None


class FakeCursor:
    """A cursor recording statements on its session."""

    def __init__(self, session: FakeSession) -> None:
        """Bind the cursor to the session that records its statements.

        Args:
            session: The owning fake session.
        """
        self.session = session
        self.result = Result()
        self.closed = False

    def execute(self, sql: str, params: Any = None) -> FakeCursor:
        """Record a statement and load its scripted result.

        Returns:
            The cursor, like the connector's.
        """
        self.session.statements.append((sql, params))
        self.session.cursors_used.append(self)
        self.result = self.session.result_for(sql, params)
        return self

    @property
    def description(self) -> list[tuple[Any, ...]]:
        """The result's columns, shaped like the connector's metadata tuples.

        Returns:
            One tuple per column, the name first.
        """
        return [(name, 2, None, None, None, None, True) for name in self.result.columns]

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Return the scripted rows.

        Returns:
            The rows.
        """
        return self.result.rows

    def fetch_pandas_all(self) -> pd.DataFrame:
        """Return the scripted frame.

        Returns:
            The frame, empty when none was scripted.
        """
        return self.result.frame if self.result.frame is not None else pd.DataFrame()

    def close(self) -> None:
        """Mark the cursor closed."""
        self.closed = True


class FakeSession:
    """A connector session recording statements and serving scripted results.

    Tables listed in ``tables`` (as ``(schema, table)``) answer the
    information-schema existence probe; other statements answer with the
    first scripted result whose prefix they start with, or an empty one.
    """

    def __init__(self) -> None:
        """Start with no tables, no script and no recorded traffic."""
        self.connect_kwargs: dict[str, Any] = {}
        self.statements: list[tuple[str, Any]] = []
        self.cursors_used: list[FakeCursor] = []
        self.tables: set[tuple[str, str]] = set()
        self._script: list[tuple[str, Result]] = []

    def respond(self, prefix: str, **result: Any) -> FakeSession:
        """Script the result of statements starting with *prefix*.

        Returns:
            The session, for chaining.
        """
        self._script.append((prefix, Result(**result)))
        return self

    def result_for(self, sql: str, params: Any) -> Result:
        """Resolve the result a statement gets.

        Returns:
            The scripted result, or an empty one.
        """
        if "information_schema.tables" in sql:
            return Result(rows=[(1,)] if tuple(params) in self.tables else [])
        for prefix, result in self._script:
            if sql.startswith(prefix):
                return result
        return Result()

    def cursor(self) -> FakeCursor:
        """Open a cursor on this session.

        Returns:
            A new recording cursor.
        """
        return FakeCursor(self)

    @property
    def sql(self) -> list[str]:
        """The statements run, without the existence probes.

        Returns:
            The statement texts, oldest first.
        """
        return [sql for sql, _ in self.statements if "information_schema.tables" not in sql]


@pytest.fixture
def session(monkeypatch: pytest.MonkeyPatch) -> FakeSession:
    """Make ``snowflake.connector.connect`` return a recording fake session.

    Returns:
        The fake session the test scripts and inspects.
    """
    fake = FakeSession()

    def connect(**kwargs: Any) -> FakeSession:
        fake.connect_kwargs = kwargs
        return fake

    monkeypatch.setattr(snowflake.connector, "connect", connect)
    return fake
