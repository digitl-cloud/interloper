"""Tests for ``interloper_api.routes.websocket``.

The realtime path has three separable halves: the connection registry, the
background thread that turns PostgreSQL NOTIFY into broadcasts, and the
endpoint that authenticates a socket before registering it.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any, Self
from uuid import uuid4

import pytest
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.testclient import TestClient
from interloper_db import Audience
from sqlalchemy.engine import make_url
from starlette.types import Message

from interloper_api.app import ORG_PREFIX, install_error_handlers
from interloper_api.dependencies import get_store
from interloper_api.routes import websocket as websocket_module
from interloper_api.routes.websocket import ConnectionManager

_ORG_ID = uuid4()


class FakeWebSocket(WebSocket):
    """A socket that records what the manager sends it; can be made to fail mid-broadcast."""

    def __init__(self, fail: bool = False) -> None:
        """Set up the fake over a websocket scope nothing is ever received from.

        Args:
            fail: Whether ``send_json`` raises, standing in for a peer that
                vanished between the snapshot and the send.
        """
        super().__init__({"type": "websocket"}, self._nothing_received, self._nothing_sent)
        self.sent: list[dict[str, object]] = []
        self._fail = fail

    async def send_json(self, data: Any, mode: str = "text") -> None:
        """Record or refuse the message.

        Args:
            data: The payload being broadcast.
            mode: The frame type, unused.

        Raises:
            RuntimeError: When this fake is configured to fail.
        """
        if self._fail:
            raise RuntimeError("peer is gone")
        self.sent.append(data)

    @staticmethod
    async def _nothing_received() -> Message:
        """Refuse a receive: the manager only ever sends.

        Raises:
            AssertionError: Always.
        """
        raise AssertionError("the manager never receives")

    @staticmethod
    async def _nothing_sent(message: Message) -> None:
        """Refuse a raw send: the manager sends JSON, which this fake records.

        Args:
            message: The ASGI message.

        Raises:
            AssertionError: Always.
        """
        raise AssertionError("the manager sends through send_json")


class TestConnectionManager:
    """Connections are grouped by organisation, and one org never sees another's."""

    async def test_a_broadcast_reaches_every_connection_of_the_org(self) -> None:
        manager = ConnectionManager()
        first, second = FakeWebSocket(), FakeWebSocket()
        await manager.connect(first, "org-1")
        await manager.connect(second, "org-1")

        await manager.broadcast("org-1", {"table": "runs"})

        assert first.sent == [{"table": "runs"}]
        assert second.sent == [{"table": "runs"}]

    async def test_organisations_are_isolated(self) -> None:
        manager = ConnectionManager()
        mine, theirs = FakeWebSocket(), FakeWebSocket()
        await manager.connect(mine, "org-1")
        await manager.connect(theirs, "org-2")

        await manager.broadcast("org-1", {"table": "runs"})

        assert mine.sent == [{"table": "runs"}]
        assert theirs.sent == []

    async def test_broadcasting_to_an_unknown_org_is_a_no_op(self) -> None:
        await ConnectionManager().broadcast("org-nobody", {"table": "runs"})

    async def test_a_disconnected_socket_stops_receiving(self) -> None:
        manager = ConnectionManager()
        socket = FakeWebSocket()
        await manager.connect(socket, "org-1")

        await manager.disconnect(socket, "org-1")
        await manager.broadcast("org-1", {"table": "runs"})

        assert socket.sent == []

    async def test_the_last_disconnect_drops_the_org_entry(self) -> None:
        manager = ConnectionManager()
        socket = FakeWebSocket()
        await manager.connect(socket, "org-1")

        await manager.disconnect(socket, "org-1")

        assert manager._connections == {}

    async def test_disconnecting_an_unregistered_socket_is_a_no_op(self) -> None:
        manager = ConnectionManager()

        await manager.disconnect(FakeWebSocket(), "org-nobody")

    async def test_one_dead_peer_does_not_stop_the_others(self) -> None:
        # The dead socket's own disconnect handler unregisters it; a failed
        # send must not abort the rest of the broadcast.
        manager = ConnectionManager()
        dead, alive = FakeWebSocket(fail=True), FakeWebSocket()
        await manager.connect(dead, "org-1")
        await manager.connect(alive, "org-1")

        await manager.broadcast("org-1", {"table": "runs"})

        assert alive.sent == [{"table": "runs"}]

    async def test_the_same_socket_registers_once(self) -> None:
        manager = ConnectionManager()
        socket = FakeWebSocket()

        await manager.connect(socket, "org-1")
        await manager.connect(socket, "org-1")
        await manager.broadcast("org-1", {"table": "runs"})

        assert socket.sent == [{"table": "runs"}]


class StopListener(Exception):
    """Raised from a patched call to break the listener's infinite loops."""


class FakeConnection:
    """psycopg connection stand-in serving a fixed queue of notifications."""

    def __init__(self, notifies: list[Any], executed: list[str]) -> None:
        """Set up the fake.

        Args:
            notifies: Notifications ``notifies`` yields, in order.
            executed: Shared list the executed SQL is appended to.
        """
        self._notifies = notifies
        self._executed = executed

    def __enter__(self) -> Self:
        """Enter the context.

        Returns:
            This connection.
        """
        return self

    def __exit__(self, *args: object) -> None:
        """Leave the context.

        Args:
            *args: Exception triple, ignored.
        """

    def execute(self, statement: str) -> None:
        """Record a statement.

        Args:
            statement: The SQL executed.
        """
        self._executed.append(statement)

    def notifies(self) -> Iterator[Any]:
        """Yield the queued notifications, then fail as a dropped connection does.

        Yields:
            Each queued notification.

        Raises:
            StopListener: Once the queue is drained, ending the pass.
        """
        yield from self._notifies
        raise StopListener("one pass only")


@pytest.fixture
def listener_loop() -> Iterator[asyncio.AbstractEventLoop]:
    """The loop the listener is handed; it never runs, since scheduling onto it is faked.

    Yields:
        A new event loop, closed once the test finishes.
    """
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def listener_harness(monkeypatch: pytest.MonkeyPatch, listener_loop: asyncio.AbstractEventLoop) -> Any:
    """Drive ``_start_notify_listener`` through exactly one pass, then stop it.

    The connection yields its notifications and then fails, so the queue is
    drained once; the retry arm's ``time.sleep`` then raises to unwind the
    otherwise infinite outer loop.

    Args:
        monkeypatch: Fixture used to swap psycopg, the scheduling and sleep.
        listener_loop: The loop the listener is handed.

    Returns:
        A callable taking the notification payloads and returning the
        recorded connect arguments, SQL and scheduled broadcasts.
    """

    def run(payloads: list[str]) -> dict[str, Any]:
        notifies = [SimpleNamespace(payload=payload) for payload in payloads]
        executed: list[str] = []
        connection = FakeConnection(notifies, executed)
        connects: list[dict[str, Any]] = []
        scheduled: list[tuple[str, dict[str, object]]] = []

        def fake_connect(dsn: str, **kwargs: Any) -> FakeConnection:
            connects.append({"dsn": dsn, **kwargs})
            return connection

        def fake_schedule(coroutine: Any, loop: Any) -> None:
            # The manager coroutine is not awaited here; close it so Python
            # does not warn about an un-awaited coroutine.
            scheduled.append(coroutine.cr_frame.f_locals["org_id"])
            coroutine.close()

        def fake_sleep(seconds: float) -> None:
            raise StopListener(f"retry after {seconds}s")

        monkeypatch.setattr(websocket_module.psycopg, "connect", fake_connect)
        monkeypatch.setattr(websocket_module.asyncio, "run_coroutine_threadsafe", fake_schedule)
        monkeypatch.setattr(websocket_module, "time", SimpleNamespace(sleep=fake_sleep))

        with pytest.raises(StopListener):
            websocket_module._start_notify_listener("postgresql://x", listener_loop)

        return {"connects": connects, "executed": executed, "scheduled": scheduled}

    return run


class TestNotifyListener:
    """The background thread that turns PostgreSQL NOTIFY into broadcasts."""

    def test_it_listens_on_the_table_changes_channel(self, listener_harness: Any) -> None:
        result = listener_harness([])

        assert result["executed"] == ["LISTEN table_changes"]
        # LISTEN only takes effect once committed, and notifications arrive only outside a transaction.
        assert result["connects"] == [{"dsn": "postgresql://x", "autocommit": True}]

    def test_a_notification_is_broadcast_to_its_org(self, listener_harness: Any) -> None:
        payload = json.dumps({"org_id": str(_ORG_ID), "table": "runs", "op": "INSERT", "record": {"id": 1}})

        result = listener_harness([payload])

        assert result["scheduled"] == [str(_ORG_ID)]

    def test_every_queued_notification_is_drained(self, listener_harness: Any) -> None:
        payloads = [
            json.dumps({"org_id": str(_ORG_ID), "table": "runs", "op": "INSERT"}),
            json.dumps({"org_id": "org-2", "table": "components", "op": "UPDATE"}),
        ]

        result = listener_harness(payloads)

        assert result["scheduled"] == [str(_ORG_ID), "org-2"]

    def test_malformed_json_is_logged_and_skipped(
        self, listener_harness: Any, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level("WARNING", logger="interloper_api.routes.websocket"):
            result = listener_harness(["{not json"])

        assert result["scheduled"] == []
        assert "Invalid NOTIFY payload" in caplog.text

    def test_a_payload_missing_org_id_is_logged_and_skipped(
        self, listener_harness: Any, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level("WARNING", logger="interloper_api.routes.websocket"):
            result = listener_harness([json.dumps({"table": "runs", "op": "INSERT"})])

        assert result["scheduled"] == []
        assert "Invalid NOTIFY payload" in caplog.text

    def test_a_bad_notification_does_not_stop_the_good_ones(self, listener_harness: Any) -> None:
        payloads = [
            "{not json",
            json.dumps({"org_id": str(_ORG_ID), "table": "runs", "op": "INSERT"}),
        ]

        result = listener_harness(payloads)

        assert result["scheduled"] == [str(_ORG_ID)]

    def test_a_connection_failure_is_logged_and_retried(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
        listener_loop: asyncio.AbstractEventLoop,
    ) -> None:
        # This thread is the only realtime path, so nothing may escape it.
        def failing_connect(dsn: str, **kwargs: Any) -> None:
            raise OSError("postgres is down")

        def fake_sleep(seconds: float) -> None:
            raise StopListener("retry")

        monkeypatch.setattr(websocket_module.psycopg, "connect", failing_connect)
        monkeypatch.setattr(websocket_module, "time", SimpleNamespace(sleep=fake_sleep))

        with caplog.at_level("ERROR", logger="interloper_api.routes.websocket"), pytest.raises(StopListener):
            websocket_module._start_notify_listener("postgresql://x", listener_loop)

        assert "postgres is down" in caplog.text
        assert "reconnecting in 5s" in caplog.text


class TestRealtimeLifespan:
    """Startup arms the listener thread from the already-initialized engine."""

    async def test_starts_a_daemon_thread_with_the_unmasked_libpq_dsn(self, monkeypatch: pytest.MonkeyPatch) -> None:
        started: list[dict[str, Any]] = []

        class FakeThread:
            def __init__(self, **kwargs: Any) -> None:
                started.append(kwargs)

            def start(self) -> None:
                started[-1]["started"] = True

        engine = SimpleNamespace(url=make_url("postgresql+psycopg://user:p%40ss@host:5432/db"))
        monkeypatch.setattr("interloper_db.get_engine", lambda: engine)
        monkeypatch.setattr(websocket_module.threading, "Thread", FakeThread)

        async with websocket_module.realtime_lifespan(FastAPI()):
            pass

        assert len(started) == 1
        assert started[0]["name"] == "NotifyListener"
        assert started[0]["daemon"] is True
        assert started[0]["started"] is True
        # libpq refuses the driver name, and str(engine.url) would mask the password as '***'.
        assert started[0]["args"][0] == "postgresql://user:p%40ss@host:5432/db"


class FakeStore:
    """Stand-in exposing the ``tokens``, ``members`` and ``organisations`` facets the endpoint reads."""

    def __init__(self, *, authenticated: bool = True, role: str | None = "viewer") -> None:
        """Set up the fake.

        Args:
            authenticated: Whether the session cookie resolves.
            role: The caller's role in every organisation; ``None`` means not a member.
        """
        self.authenticated = authenticated
        self.role = role
        self.presented: list[tuple[str, Audience]] = []
        self._profile = SimpleNamespace(id=uuid4(), email="user@example.com", is_super_admin=False)
        self.tokens = SimpleNamespace(resolve=self._resolve)
        self.members = SimpleNamespace(role_of=self._role_of)
        self.organisations = SimpleNamespace(get=lambda org_id: SimpleNamespace(id=org_id))

    def _resolve(self, raw: str, audience: Audience) -> tuple[Any, Any] | None:
        self.presented.append((raw, audience))
        return (self._profile, SimpleNamespace(kind="session")) if self.authenticated else None

    def _role_of(self, org_id: Any, user: Any) -> str | None:
        return self.role


@pytest.fixture
def endpoint_client(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Mount the websocket route under the organisation prefix on an app resolving a fake store.

    Args:
        monkeypatch: Fixture the manager swap below rides on.

    Returns:
        A callable taking the fake store and returning a TestClient.
    """

    def build(store: FakeStore) -> TestClient:
        app = FastAPI()
        app.dependency_overrides[get_store] = lambda: store
        install_error_handlers(app)
        app.include_router(websocket_module.org_router, prefix=ORG_PREFIX)
        return TestClient(app)

    return build


_PATH = f"/organisations/{_ORG_ID}/ws"


class TestWebSocketEndpoint:
    """Authentication and authorisation happen before the socket is accepted or registered."""

    @pytest.fixture(autouse=True)
    def fresh_manager(self, monkeypatch: pytest.MonkeyPatch) -> ConnectionManager:
        """Give each test its own registry.

        Args:
            monkeypatch: Fixture used to install the fresh manager.

        Returns:
            The manager the endpoint will register into.
        """
        manager = ConnectionManager()
        monkeypatch.setattr(websocket_module, "_manager", manager)
        return manager

    def test_no_cookie_is_closed_with_4001(self, endpoint_client: Any) -> None:
        client = endpoint_client(FakeStore())

        with client.websocket_connect(_PATH) as socket, pytest.raises(WebSocketDisconnect) as excinfo:
            socket.receive_text()

        assert excinfo.value.code == 4001

    def test_an_unresolvable_token_is_refused(self, endpoint_client: Any) -> None:
        client = endpoint_client(FakeStore(authenticated=False))
        client.cookies.set("session_token", "stale")

        with client.websocket_connect(_PATH) as socket, pytest.raises(WebSocketDisconnect) as excinfo:
            socket.receive_text()

        assert excinfo.value.code == 4001

    def test_the_cookie_is_resolved_as_a_session(self, endpoint_client: Any) -> None:
        store = FakeStore()
        client = endpoint_client(store)
        client.cookies.set("session_token", "tok")

        with client.websocket_connect(_PATH):
            pass

        assert store.presented == [("tok", Audience.APP)]

    def test_a_non_member_is_refused_distinctly(self, endpoint_client: Any) -> None:
        # 4004, not 4001: the caller is authenticated but cannot act in the organisation.
        client = endpoint_client(FakeStore(role=None))
        client.cookies.set("session_token", "tok")

        with client.websocket_connect(_PATH) as socket, pytest.raises(WebSocketDisconnect) as excinfo:
            socket.receive_text()

        assert excinfo.value.code == 4004

    def test_an_authenticated_socket_is_registered_and_then_released(
        self, endpoint_client: Any, fresh_manager: ConnectionManager
    ) -> None:
        client = endpoint_client(FakeStore())
        client.cookies.set("session_token", "tok")

        with client.websocket_connect(_PATH):
            assert set(fresh_manager._connections) == {str(_ORG_ID)}

        assert fresh_manager._connections == {}

    def test_a_registered_socket_receives_a_broadcast(
        self, endpoint_client: Any, fresh_manager: ConnectionManager
    ) -> None:
        client = endpoint_client(FakeStore())
        client.cookies.set("session_token", "tok")

        with client.websocket_connect(_PATH) as socket:
            connection = next(iter(fresh_manager._connections[str(_ORG_ID)]))
            asyncio.run(connection.send_json({"table": "runs", "event": "INSERT"}))

            assert socket.receive_json() == {"table": "runs", "event": "INSERT"}


class TestOpenSocketRecheck:
    """An open socket is closed once its caller may no longer read the organisation."""

    @pytest.fixture(autouse=True)
    def fresh_manager(self, monkeypatch: pytest.MonkeyPatch) -> ConnectionManager:
        """Give each test its own registry and a recheck interval short enough to wait on.

        Args:
            monkeypatch: Fixture used to install the fresh manager and the interval.

        Returns:
            The manager the endpoint will register into.
        """
        manager = ConnectionManager()
        monkeypatch.setattr(websocket_module, "_manager", manager)
        monkeypatch.setattr(websocket_module, "RECHECK_SECONDS", 0.01)
        return manager

    def test_a_revoked_session_is_closed_with_4001(
        self, endpoint_client: Any, fresh_manager: ConnectionManager
    ) -> None:
        store = FakeStore()
        client = endpoint_client(store)
        client.cookies.set("session_token", "tok")

        with client.websocket_connect(_PATH) as socket:
            store.authenticated = False
            with pytest.raises(WebSocketDisconnect) as excinfo:
                socket.receive_text()

        assert excinfo.value.code == 4001
        assert fresh_manager._connections == {}

    def test_a_removed_member_is_closed_with_4004(self, endpoint_client: Any, fresh_manager: ConnectionManager) -> None:
        store = FakeStore()
        client = endpoint_client(store)
        client.cookies.set("session_token", "tok")

        with client.websocket_connect(_PATH) as socket:
            store.role = None
            with pytest.raises(WebSocketDisconnect) as excinfo:
                socket.receive_text()

        assert excinfo.value.code == 4004
        assert fresh_manager._connections == {}

    def test_a_caller_still_allowed_keeps_receiving(
        self, endpoint_client: Any, fresh_manager: ConnectionManager
    ) -> None:
        store = FakeStore()
        client = endpoint_client(store)
        client.cookies.set("session_token", "tok")

        with client.websocket_connect(_PATH) as socket:
            while len(store.presented) < 3:
                time.sleep(0.01)
            asyncio.run(fresh_manager.broadcast(str(_ORG_ID), {"table": "runs", "event": "INSERT"}))

            assert socket.receive_json() == {"table": "runs", "event": "INSERT"}
