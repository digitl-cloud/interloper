"""Shared Databricks fakes.

Two wires are faked, and nothing reaches the network. The REST API (the
connection's check, pickers and OAuth token exchange) runs on a real
``RESTClient`` over an ``httpx2.MockTransport``. The SQL connector is
replaced by a session that records each statement, reads back each
``PUT`` upload, and serves scripted results.
"""

from __future__ import annotations

import io
import re
from dataclasses import dataclass, field
from typing import Any

import httpx2
import pandas as pd
import pyarrow as pa
import pytest
from databricks import sql

from interloper_databricks import connection as connection_module


@pytest.fixture(autouse=True)
def _no_ambient_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep a developer's own ``DATABRICKS_*`` variables out of the tests."""
    for name in ("HOST", "CLIENT_ID", "CLIENT_SECRET", "ACCESS_TOKEN"):
        monkeypatch.delenv(f"DATABRICKS_{name}", raising=False)


# -- REST ------------------------------------------------------------------------


class FakeWorkspace:
    """Records workspace REST requests and serves scripted responses.

    The token endpoint answers with a fresh token per exchange
    (``token-1``, ``token-2``, ...) valid for ``expires_in`` seconds. Other
    paths answer with the responses queued for them, in order, then ``{}``.
    """

    def __init__(self) -> None:
        """Start with no script and no recorded traffic."""
        self.requests: list[httpx2.Request] = []
        self.expires_in = 3600
        self.token_status = 200
        self._script: dict[str, list[httpx2.Response]] = {}
        self._params: list[tuple[str, dict[str, str]]] = []

    def respond(self, path: str, status: int = 200, **payload: Any) -> FakeWorkspace:
        """Queue a response for *path*.

        Returns:
            The fake, for chaining.
        """
        self._script.setdefault(path, []).append(httpx2.Response(status, json=payload))
        return self

    def to(self, path: str) -> list[httpx2.Request]:
        """The requests sent to *path*.

        Returns:
            The requests, oldest first.
        """
        return [request for request in self.requests if request.url.path == path]

    def params(self, path: str) -> list[dict[str, str]]:
        """The query parameters of each request sent to *path*, as sent.

        A paginator advances one request object in place, so the parameters
        are captured when each request goes out.

        Returns:
            One dict per request, oldest first.
        """
        return [params for sent, params in self._params if sent == path]

    @property
    def paths(self) -> list[str]:
        """The paths called, in order.

        Returns:
            The request paths, oldest first.
        """
        return [request.url.path for request in self.requests]

    def handle(self, request: httpx2.Request) -> httpx2.Response:
        """Record *request* and answer it.

        Returns:
            The token, the next scripted response, or ``{}``.
        """
        self.requests.append(request)
        self._params.append((request.url.path, dict(request.url.params)))
        if request.url.path == "/oidc/v1/token":
            if self.token_status != 200:
                return httpx2.Response(self.token_status, json={"error": "invalid_client"})
            issued = len(self.to("/oidc/v1/token"))
            return httpx2.Response(
                200, json={"access_token": f"token-{issued}", "token_type": "Bearer", "expires_in": self.expires_in}
            )
        queued = self._script.get(request.url.path)
        return queued.pop(0) if queued else httpx2.Response(200, json={})


@pytest.fixture
def workspace(monkeypatch: pytest.MonkeyPatch) -> FakeWorkspace:
    """Give the connection real RESTClients wired to a recording transport.

    Returns:
        The recording fake the test scripts and inspects.
    """
    fake = FakeWorkspace()
    real = connection_module.RESTClient

    def factory(*args: Any, **kwargs: Any) -> Any:
        return real(*args, transport=httpx2.MockTransport(fake.handle), **kwargs)

    monkeypatch.setattr(connection_module, "RESTClient", factory)
    return fake


# -- SQL connector -----------------------------------------------------------------


@dataclass
class Result:
    """What a scripted statement returns.

    Attributes:
        rows: What ``fetchall`` returns.
        arrow: What ``fetchall_arrow`` returns.
    """

    rows: list[tuple[Any, ...]] = field(default_factory=list)
    arrow: pa.Table | None = None


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

    def execute(self, operation: str, parameters: Any = None, input_stream: Any = None) -> FakeCursor:
        """Record a statement and load its scripted result.

        Returns:
            The cursor, like the connector's.
        """
        self.result = self.session.run(operation, parameters, input_stream)
        return self

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Return the scripted rows.

        Returns:
            The rows.
        """
        return self.result.rows

    def fetchall_arrow(self) -> pa.Table:
        """Return the scripted Arrow table.

        Returns:
            The table, empty when none was scripted.
        """
        return self.result.arrow if self.result.arrow is not None else pa.table({})

    def close(self) -> None:
        """Mark the cursor closed."""
        self.closed = True


_PROBE = re.compile(r"table_schema = '(.+?)' AND table_name = '(.+?)'")
_PUT = re.compile(r"PUT '__input_stream__' INTO '(.+?)' OVERWRITE")


class FakeSession:
    """A connector session recording statements and serving scripted results.

    Tables listed in ``tables`` (as lower-case ``(schema, table)``) answer
    the information-schema probe. A ``PUT`` reads its input stream back as
    a DataFrame into ``uploads`` and adds the path to ``files``, a
    ``REMOVE`` takes it out again. A statement starting with a prefix in
    ``failures`` raises that error; anything else answers with the first
    scripted result whose prefix it starts with, or an empty one.
    """

    def __init__(self) -> None:
        """Start with no tables, no script and no recorded traffic."""
        self.connect_calls: list[dict[str, Any]] = []
        self.statements: list[tuple[str, Any]] = []
        self.uploads: list[pd.DataFrame] = []
        self.raw_uploads: list[bytes] = []
        self.files: set[str] = set()
        self.failures: dict[str, Exception] = {}
        self.cursors: list[FakeCursor] = []
        self.tables: set[tuple[str, str]] = set()
        self.on_execute: Any = None
        self._script: list[tuple[str, Result]] = []

    @property
    def connect_kwargs(self) -> dict[str, Any]:
        """The arguments of the last ``connect``.

        Returns:
            The keyword arguments.
        """
        return self.connect_calls[-1]

    def respond(self, prefix: str, **result: Any) -> FakeSession:
        """Script the result of statements starting with *prefix*.

        Returns:
            The session, for chaining.
        """
        self._script.append((prefix, Result(**result)))
        return self

    def run(self, sql_text: str, parameters: Any, input_stream: Any) -> Result:
        """Record one statement and resolve its result.

        Returns:
            The scripted result, or an empty one.
        """
        self.statements.append((sql_text, parameters))
        if self.on_execute is not None:
            self.on_execute(sql_text)
        for prefix, error in self.failures.items():
            if sql_text.startswith(prefix):
                raise error
        if sql_text.startswith("PUT "):
            match = _PUT.fullmatch(sql_text)
            assert match is not None, sql_text
            assert input_stream is not None
            data = input_stream.read()
            self.raw_uploads.append(data)
            self.uploads.append(pd.read_parquet(io.BytesIO(data)))
            self.files.add(match.group(1))
            return Result()
        assert input_stream is None
        if sql_text.startswith("REMOVE "):
            self.files.discard(sql_text.removeprefix("REMOVE '").removesuffix("'"))
            return Result()
        if "information_schema.tables" in sql_text:
            match = _PROBE.search(sql_text)
            assert match is not None
            return Result(rows=[(1,)] if match.groups() in self.tables else [])
        for prefix, result in self._script:
            if sql_text.startswith(prefix):
                return result
        return Result()

    def cursor(self) -> FakeCursor:
        """Open a cursor on this session.

        Returns:
            A new recording cursor.
        """
        cursor = FakeCursor(self)
        self.cursors.append(cursor)
        return cursor

    @property
    def sql(self) -> list[str]:
        """The statements run, without the existence probes.

        Returns:
            The statement texts, oldest first.
        """
        return [text for text, _ in self.statements if "information_schema.tables" not in text]


@pytest.fixture
def session(monkeypatch: pytest.MonkeyPatch) -> FakeSession:
    """Make ``databricks.sql.connect`` return a recording fake session.

    Returns:
        The fake session the test scripts and inspects.
    """
    fake = FakeSession()

    def connect(**kwargs: Any) -> FakeSession:
        fake.connect_calls.append(kwargs)
        return fake

    monkeypatch.setattr(sql, "connect", connect)
    return fake
