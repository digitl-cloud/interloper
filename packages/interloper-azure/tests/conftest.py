"""Shared fakes for the Entra credential, the Fabric REST API and the warehouse driver.

Nothing reaches the network: the credential hands out canned tokens, the
connection's ``RESTClient`` runs on an ``httpx2.MockTransport``, and
``mssql_python.connect`` returns a recording session over an in-memory
catalog.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

import httpx2
import mssql_python
import pytest

from interloper_azure import connection as connection_module

# -- Credential ----------------------------------------------------------------


@dataclass
class FakeToken:
    """What ``get_token`` returns.

    Attributes:
        token: The bearer token.
    """

    token: str


class FakeCredential:
    """A ``ClientSecretCredential`` stand-in handing out one token per scope."""

    def __init__(self, tenant_id: str, client_id: str, client_secret: str) -> None:
        """Record the principal the credential was built for.

        Args:
            tenant_id: The tenant ID.
            client_id: The client ID.
            client_secret: The client secret.
        """
        self.args = (tenant_id, client_id, client_secret)
        self.scopes: list[str] = []

    def get_token(self, *scopes: str) -> FakeToken:
        """Return a token naming its scope.

        Returns:
            The token.
        """
        self.scopes.extend(scopes)
        return FakeToken(f"token-for-{scopes[0]}")


@pytest.fixture(autouse=True)
def credential(monkeypatch: pytest.MonkeyPatch) -> type[FakeCredential]:
    """Replace the Entra credential class with the fake.

    Returns:
        The fake credential class.
    """
    monkeypatch.setattr(connection_module, "ClientSecretCredential", FakeCredential)
    return FakeCredential


# -- Fabric REST API -----------------------------------------------------------


class FakeFabric:
    """Records Fabric REST requests and answers them from per-path scripts.

    Each path's responses are consumed in order; a path with no script left
    answers ``{"value": []}``.
    """

    def __init__(self) -> None:
        """Start with no script and no recorded traffic."""
        self.requests: list[httpx2.Request] = []
        self._script: dict[str, list[httpx2.Response]] = {}

    def respond(self, path: str, response: httpx2.Response) -> FakeFabric:
        """Queue a response for a path under ``/v1``.

        Returns:
            The fake, for chaining.
        """
        self._script.setdefault(path, []).append(response)
        return self

    def ok(self, path: str, **body: Any) -> FakeFabric:
        """Queue a 200 JSON response for a path under ``/v1``.

        Returns:
            The fake, for chaining.
        """
        return self.respond(path, httpx2.Response(200, json=body))

    @property
    def paths(self) -> list[str]:
        """The paths requested, without the ``/v1`` prefix.

        Returns:
            The paths, oldest first.
        """
        return [request.url.path.removeprefix("/v1") for request in self.requests]

    def handle(self, request: httpx2.Request) -> httpx2.Response:
        """Record a request and answer it from its path's script.

        Returns:
            The next scripted response.
        """
        self.requests.append(request)
        queue = self._script.get(request.url.path.removeprefix("/v1"), [])
        return queue.pop(0) if queue else httpx2.Response(200, json={"value": []})


@pytest.fixture
def fabric(monkeypatch: pytest.MonkeyPatch) -> FakeFabric:
    """Give the connection a real RESTClient wired to a recording transport.

    Returns:
        The recording fake the test scripts and inspects.
    """
    fake = FakeFabric()
    real = connection_module.RESTClient

    def factory(*args: Any, **kwargs: Any) -> Any:
        return real(*args, transport=httpx2.MockTransport(fake.handle), **kwargs)

    monkeypatch.setattr(connection_module, "RESTClient", factory)
    return fake


# -- Warehouse driver ----------------------------------------------------------

_COLUMNS_PROBE = "SELECT c.name FROM sys.columns"
_SCHEMA_PROBE = "SELECT 1 FROM sys.schemas"
_IDENTIFIER = r"\[((?:\]\]|[^\]])*)\]"


def _unquote(identifier: str) -> str:
    return identifier.replace("]]", "]")


class FakeCursor:
    """A cursor recording statements on its warehouse."""

    def __init__(self, session: FakeSession) -> None:
        """Bind the cursor to the session it runs on.

        Args:
            session: The owning fake session.
        """
        self.session = session
        self.names: list[str] = []
        self.rows: list[tuple[Any, ...]] = []
        self.closed = False

    def execute(self, sql: str, parameters: Any = None) -> FakeCursor:
        """Record a statement and load its result.

        Returns:
            The cursor, like the driver's.
        """
        self.names, self.rows = self.session.warehouse.run(self.session, sql, parameters)
        return self

    @property
    def description(self) -> list[tuple[Any, ...]] | None:
        """The result's columns, shaped like DB-API description tuples.

        Returns:
            One tuple per column, the name first, or ``None`` without a result.
        """
        return [(name, str, None, None, None, None, True) for name in self.names] or None

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Return the result's rows.

        Returns:
            The rows.
        """
        return self.rows

    def close(self) -> None:
        """Mark the cursor closed."""
        self.closed = True


class FakeSession:
    """One driver connection to the fake warehouse."""

    def __init__(self, warehouse: FakeWarehouse, number: int) -> None:
        """Open the session.

        Args:
            warehouse: The warehouse it talks to.
            number: Its ordinal among the warehouse's sessions.
        """
        self.warehouse = warehouse
        self.number = number
        self.closed = False

    def cursor(self) -> FakeCursor:
        """Open a cursor.

        Returns:
            A new recording cursor.
        """
        return FakeCursor(self)

    def close(self) -> None:
        """Mark the session closed."""
        self.closed = True


@dataclass
class Statement:
    """One statement the warehouse ran.

    Attributes:
        sql: The statement text.
        parameters: Its parameters, ``None`` when none were bound.
        session: The ordinal of the session it ran on.
    """

    sql: str
    parameters: Any
    session: int


@dataclass
class FakeWarehouse:
    """An in-memory warehouse catalog recording every statement.

    ``CREATE SCHEMA`` and ``CREATE TABLE`` update the catalog the probes
    read; ``SELECT *`` and the ``count`` query answer with scripted rows; a
    statement starting with a prefix in ``failures`` raises that error.

    Attributes:
        schemas: The schemas that exist.
        tables: Each ``(schema, table)`` to its column names.
        results: Scripted ``(names, rows)`` keyed by statement prefix.
        failures: Errors to raise, keyed by statement prefix.
        statements: Every statement run, oldest first.
        sessions: Every session opened, oldest first.
        connects: The arguments of every ``connect`` call.
    """

    schemas: set[str] = field(default_factory=lambda: {"dbo"})
    tables: dict[tuple[str, str], list[str]] = field(default_factory=dict)
    results: dict[str, tuple[list[str], list[tuple[Any, ...]]]] = field(default_factory=dict)
    failures: dict[str, Exception] = field(default_factory=dict)
    statements: list[Statement] = field(default_factory=list)
    sessions: list[FakeSession] = field(default_factory=list)
    connects: list[tuple[tuple[Any, ...], dict[str, Any]]] = field(default_factory=list)

    def connect(self, *args: Any, **kwargs: Any) -> FakeSession:
        """Open a session, like ``mssql_python.connect``.

        Returns:
            The new session.
        """
        self.connects.append((args, kwargs))
        session = FakeSession(self, len(self.sessions))
        self.sessions.append(session)
        return session

    def table(self, schema: str, table: str, *columns: str) -> FakeWarehouse:
        """Declare an existing table (and its schema).

        Returns:
            The warehouse, for chaining.
        """
        self.schemas.add(schema)
        self.tables[(schema, table)] = list(columns)
        return self

    def run(self, session: FakeSession, sql: str, parameters: Any) -> tuple[list[str], list[tuple[Any, ...]]]:
        """Run a statement against the catalog.

        Returns:
            The result's column names and rows.
        """
        self.statements.append(Statement(sql, parameters, session.number))
        for prefix, error in self.failures.items():
            if sql.startswith(prefix):
                raise error
        if sql.startswith(_COLUMNS_PROBE):
            return ["name"], [(name,) for name in self.tables.get(tuple(parameters), [])]
        if sql.startswith(_SCHEMA_PROBE):
            return [""], [(1,)] if parameters[0] in self.schemas else []
        if sql.startswith("CREATE SCHEMA "):
            match = re.fullmatch(f"CREATE SCHEMA {_IDENTIFIER}", sql)
            assert match is not None
            self.schemas.add(_unquote(match.group(1)))
        elif sql.startswith("CREATE TABLE "):
            match = re.match(rf"CREATE TABLE {_IDENTIFIER}\.{_IDENTIFIER} \((.*)\)$", sql)
            assert match is not None
            schema, table, body = match.groups()
            columns = [_unquote(name) for name in re.findall(rf"{_IDENTIFIER} \w", body)]
            self.tables[(_unquote(schema), _unquote(table))] = columns
        for prefix, result in self.results.items():
            if sql.startswith(prefix):
                return result
        return [], []

    @property
    def sql(self) -> list[str]:
        """The statements run, without the catalog probes.

        Returns:
            The statement texts, oldest first.
        """
        return [s.sql for s in self.statements if not s.sql.startswith((_COLUMNS_PROBE, _SCHEMA_PROBE))]

    @property
    def verbs(self) -> list[str]:
        """The leading keyword of every non-probe statement.

        Returns:
            The verbs, oldest first.
        """
        return [sql.split(" ")[0] for sql in self.sql]


@pytest.fixture
def warehouse(monkeypatch: pytest.MonkeyPatch) -> FakeWarehouse:
    """Make ``mssql_python.connect`` open sessions on a recording fake warehouse.

    Returns:
        The fake warehouse the test scripts and inspects.
    """
    fake = FakeWarehouse()
    monkeypatch.setattr(mssql_python, "connect", fake.connect)
    return fake
