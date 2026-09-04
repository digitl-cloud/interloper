"""Tests for ``interloper_api.routes.websocket``.

The realtime path has three separable halves: the connection registry, the
background thread that turns PostgreSQL NOTIFY into broadcasts, and the
endpoint that authenticates a socket before registering it.
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest
from fastapi import FastAPI, WebSocketDisconnect
from fastapi.testclient import TestClient
from typing_extensions import Self

from interloper_api.dependencies import state as state_module
from interloper_api.routes import websocket as websocket_module
from interloper_api.routes.websocket import ConnectionManager

_ORG_ID = uuid4()


class FakeWebSocket:
    """Records what the manager sends it; can be made to fail mid-broadcast."""

    def __init__(self, fail: bool = False) -> None:
        """Set up the fake.

        Args:
            fail: Whether ``send_json`` raises, standing in for a peer that
                vanished between the snapshot and the send.
        """
        self.sent: list[dict[str, object]] = []
        self._fail = fail

    async def send_json(self, message: dict[str, object]) -> None:
        """Record or refuse the message.

        Args:
            message: The payload being broadcast.

        Raises:
            RuntimeError: When this fake is configured to fail.
        """
        if self._fail:
            raise RuntimeError("peer is gone")
        self.sent.append(message)


class TestConnectionManager:
    """Connections are grouped by organisation, and one org never sees another's."""

    async def test_a_broadcast_reaches_every_connection_of_the_org(self) -> None:
        manager = ConnectionManager()
        first, second = FakeWebSocket(), FakeWebSocket()
        await manager.connect(first, "org-1")  # ty: ignore[invalid-argument-type]
        await manager.connect(second, "org-1")  # ty: ignore[invalid-argument-type]

        await manager.broadcast("org-1", {"table": "runs"})

        assert first.sent == [{"table": "runs"}]
        assert second.sent == [{"table": "runs"}]

    async def test_organisations_are_isolated(self) -> None:
        manager = ConnectionManager()
        mine, theirs = FakeWebSocket(), FakeWebSocket()
        await manager.connect(mine, "org-1")  # ty: ignore[invalid-argument-type]
        await manager.connect(theirs, "org-2")  # ty: ignore[invalid-argument-type]

        await manager.broadcast("org-1", {"table": "runs"})

        assert mine.sent == [{"table": "runs"}]
        assert theirs.sent == []

    async def test_broadcasting_to_an_unknown_org_is_a_no_op(self) -> None:
        await ConnectionManager().broadcast("org-nobody", {"table": "runs"})

    async def test_a_disconnected_socket_stops_receiving(self) -> None:
        manager = ConnectionManager()
        socket = FakeWebSocket()
        await manager.connect(socket, "org-1")  # ty: ignore[invalid-argument-type]

        await manager.disconnect(socket, "org-1")  # ty: ignore[invalid-argument-type]
        await manager.broadcast("org-1", {"table": "runs"})

        assert socket.sent == []

    async def test_the_last_disconnect_drops_the_org_entry(self) -> None:
        manager = ConnectionManager()
        socket = FakeWebSocket()
        await manager.connect(socket, "org-1")  # ty: ignore[invalid-argument-type]

        await manager.disconnect(socket, "org-1")  # ty: ignore[invalid-argument-type]

        assert manager._connections == {}

    async def test_disconnecting_an_unregistered_socket_is_a_no_op(self) -> None:
        manager = ConnectionManager()

        await manager.disconnect(FakeWebSocket(), "org-nobody")  # ty: ignore[invalid-argument-type]

    async def test_one_dead_peer_does_not_stop_the_others(self) -> None:
        # The dead socket's own disconnect handler unregisters it; a failed
        # send must not abort the rest of the broadcast.
        manager = ConnectionManager()
        dead, alive = FakeWebSocket(fail=True), FakeWebSocket()
        await manager.connect(dead, "org-1")  # ty: ignore[invalid-argument-type]
        await manager.connect(alive, "org-1")  # ty: ignore[invalid-argument-type]

        await manager.broadcast("org-1", {"table": "runs"})

        assert alive.sent == [{"table": "runs"}]

    async def test_the_same_socket_registers_once(self) -> None:
        manager = ConnectionManager()
        socket = FakeWebSocket()

        await manager.connect(socket, "org-1")  # ty: ignore[invalid-argument-type]
        await manager.connect(socket, "org-1")  # ty: ignore[invalid-argument-type]
        await manager.broadcast("org-1", {"table": "runs"})

        assert socket.sent == [{"table": "runs"}]


class StopListener(Exception):
    """Raised from a patched call to break the listener's infinite loops."""


class FakeCursor:
    """Context-managed cursor recording the statements it was given."""

    def __init__(self, executed: list[str]) -> None:
        """Set up the fake.

        Args:
            executed: Shared list the executed SQL is appended to.
        """
        self._executed = executed

    def __enter__(self) -> Self:
        """Enter the context.

        Returns:
            This cursor.
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


class FakeConnection:
    """psycopg2 connection stand-in serving a fixed queue of notifications."""

    def __init__(self, notifies: list[Any], executed: list[str]) -> None:
        """Set up the fake.

        Args:
            notifies: Notifications ``poll`` makes available, drained in order.
            executed: Shared list the executed SQL is appended to.
        """
        self.notifies = notifies
        self.isolation_level: int | None = None
        self._executed = executed

    def set_isolation_level(self, level: int) -> None:
        """Record the isolation level.

        Args:
            level: The level psycopg2 was asked for.
        """
        self.isolation_level = level

    def cursor(self) -> FakeCursor:
        """Return a context-managed cursor.

        Returns:
            The fake cursor.
        """
        return FakeCursor(self._executed)

    def poll(self) -> None:
        """Accept the poll; notifications are pre-loaded."""


@pytest.fixture
def listener_harness(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Drive ``_start_notify_listener`` through exactly one pass, then stop it.

    ``select`` succeeds once and raises on the second call, so the notify
    queue is drained once; the retry arm's ``time.sleep`` then raises to
    unwind the otherwise infinite outer loop.

    Args:
        monkeypatch: Fixture used to swap psycopg2, select and sleep.

    Returns:
        A callable taking the notification payloads and returning the
        recorded connection, SQL and scheduled broadcasts.
    """

    def run(payloads: list[str]) -> dict[str, Any]:
        notifies = [SimpleNamespace(payload=payload) for payload in payloads]
        executed: list[str] = []
        connection = FakeConnection(notifies, executed)
        scheduled: list[tuple[str, dict[str, object]]] = []
        selects = {"count": 0}

        def fake_select(*args: object) -> list[object]:
            selects["count"] += 1
            if selects["count"] > 1:
                raise StopListener("one pass only")
            return [connection]

        def fake_schedule(coroutine: Any, loop: Any) -> None:
            # The manager coroutine is not awaited here; close it so Python
            # does not warn about an un-awaited coroutine.
            scheduled.append(coroutine.cr_frame.f_locals["org_id"])
            coroutine.close()

        def fake_sleep(seconds: float) -> None:
            raise StopListener(f"retry after {seconds}s")

        monkeypatch.setattr(websocket_module.psycopg2, "connect", lambda dsn: connection)
        monkeypatch.setattr(websocket_module._select, "select", fake_select)
        monkeypatch.setattr(websocket_module.asyncio, "run_coroutine_threadsafe", fake_schedule)
        monkeypatch.setattr("time.sleep", fake_sleep)

        with pytest.raises(StopListener):
            websocket_module._start_notify_listener("postgresql://x", object())  # ty: ignore[invalid-argument-type]

        return {"connection": connection, "executed": executed, "scheduled": scheduled}

    return run


class TestNotifyListener:
    """The background thread that turns PostgreSQL NOTIFY into broadcasts."""

    def test_it_listens_on_the_table_changes_channel(self, listener_harness: Any) -> None:
        result = listener_harness([])

        assert result["executed"] == ["LISTEN table_changes"]
        assert result["connection"].isolation_level is not None

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
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        # This thread is the only realtime path, so nothing may escape it.
        def failing_connect(dsn: str) -> None:
            raise OSError("postgres is down")

        def fake_sleep(seconds: float) -> None:
            raise StopListener("retry")

        monkeypatch.setattr(websocket_module.psycopg2, "connect", failing_connect)
        monkeypatch.setattr("time.sleep", fake_sleep)

        with caplog.at_level("ERROR", logger="interloper_api.routes.websocket"), pytest.raises(StopListener):
            websocket_module._start_notify_listener("postgresql://x", object())  # ty: ignore[invalid-argument-type]

        assert "postgres is down" in caplog.text
        assert "reconnecting in 5s" in caplog.text


class TestRealtimeLifespan:
    """Startup arms the listener thread from the already-initialized engine."""

    async def test_starts_a_daemon_thread_with_the_unmasked_dsn(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        started: list[dict[str, Any]] = []

        class FakeThread:
            def __init__(self, **kwargs: Any) -> None:
                started.append(kwargs)

            def start(self) -> None:
                started[-1]["started"] = True

        engine = SimpleNamespace(
            url=SimpleNamespace(render_as_string=lambda hide_password: "postgresql://user:pw@host/db")
        )
        monkeypatch.setattr("interloper_db.get_engine", lambda: engine)
        monkeypatch.setattr(websocket_module.threading, "Thread", FakeThread)

        async with websocket_module.realtime_lifespan(FastAPI()):
            pass

        assert len(started) == 1
        assert started[0]["name"] == "NotifyListener"
        assert started[0]["daemon"] is True
        assert started[0]["started"] is True
        # str(engine.url) would mask the password as '***'; psycopg2 needs it.
        assert started[0]["args"][0] == "postgresql://user:pw@host/db"


class FakeStore:
    """Stand-in exposing only ``auth.resolve_session``."""

    def __init__(self, session: tuple[Any, Any] | None) -> None:
        """Set up the fake.

        Args:
            session: What ``resolve_session`` returns; ``None`` means unknown.
        """
        self.auth = SimpleNamespace(resolve_session=lambda token: session)


@pytest.fixture
def endpoint_client(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Mount the websocket route with the global store swapped for a fake.

    The endpoint calls ``get_store()`` directly rather than through
    ``Depends``, so the module-level state is what has to be replaced.

    Args:
        monkeypatch: Fixture used to install the fake store.

    Returns:
        A callable taking the resolved session and returning a TestClient.
    """

    def build(session: tuple[Any, Any] | None) -> TestClient:
        monkeypatch.setattr(state_module, "_store", FakeStore(session))
        app = FastAPI()
        app.include_router(websocket_module.router)
        return TestClient(app)

    return build


class TestWebSocketEndpoint:
    """Authentication happens before the socket is accepted or registered."""

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

    def test_no_cookie_is_refused_before_accept(self, endpoint_client: Any) -> None:
        client = endpoint_client(None)

        with pytest.raises(WebSocketDisconnect) as excinfo, client.websocket_connect("/ws"):
            pass  # pragma: no cover - the handshake never completes

        assert excinfo.value.code == 4001

    def test_an_unresolvable_token_is_refused(self, endpoint_client: Any) -> None:
        client = endpoint_client(None)
        client.cookies.set("session_token", "stale")

        with pytest.raises(WebSocketDisconnect) as excinfo, client.websocket_connect("/ws"):
            pass  # pragma: no cover - the handshake never completes

        assert excinfo.value.code == 4001

    def test_a_session_without_an_organisation_is_refused_distinctly(self, endpoint_client: Any) -> None:
        # 4002, not 4001: the caller is authenticated but has no org selected.
        profile = SimpleNamespace(id=uuid4(), email="user@example.com")
        client = endpoint_client((profile, SimpleNamespace(organisation_id=None)))
        client.cookies.set("session_token", "tok")

        with pytest.raises(WebSocketDisconnect) as excinfo, client.websocket_connect("/ws"):
            pass  # pragma: no cover - the handshake never completes

        assert excinfo.value.code == 4002

    def test_an_authenticated_socket_is_registered_and_then_released(
        self, endpoint_client: Any, fresh_manager: ConnectionManager
    ) -> None:
        profile = SimpleNamespace(id=uuid4(), email="user@example.com")
        client = endpoint_client((profile, SimpleNamespace(organisation_id=_ORG_ID)))
        client.cookies.set("session_token", "tok")

        with client.websocket_connect("/ws"):
            assert set(fresh_manager._connections) == {str(_ORG_ID)}

        assert fresh_manager._connections == {}

    def test_a_registered_socket_receives_a_broadcast(
        self, endpoint_client: Any, fresh_manager: ConnectionManager
    ) -> None:
        profile = SimpleNamespace(id=uuid4(), email="user@example.com")
        client = endpoint_client((profile, SimpleNamespace(organisation_id=_ORG_ID)))
        client.cookies.set("session_token", "tok")

        with client.websocket_connect("/ws") as socket:
            connection = next(iter(fresh_manager._connections[str(_ORG_ID)]))
            asyncio.run(connection.send_json({"table": "runs", "event": "INSERT"}))

            assert socket.receive_json() == {"table": "runs", "event": "INSERT"}
