"""WebSocket realtime endpoint — bridges PostgreSQL NOTIFY to connected clients.

Architecture:
- A background thread LISTENs on the ``table_changes`` PostgreSQL channel.
- When a notification arrives, the payload (table, op, org_id, record) is
  broadcast to all WebSocket clients belonging to that org.
- Clients subscribe by connecting to ``/api/ws`` with a valid session cookie.
"""

from __future__ import annotations

import asyncio
import json
import logging
import select as _select
import threading
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import psycopg2
import psycopg2.extensions
from fastapi import APIRouter, Cookie, FastAPI, WebSocket, WebSocketDisconnect

from interloper_api.dependencies import get_store

logger = logging.getLogger(__name__)
router = APIRouter(tags=["websocket"])


# -- Connection manager --------------------------------------------------------


class ConnectionManager:
    """Manages WebSocket connections grouped by organisation ID."""

    def __init__(self) -> None:
        """Start with no registered connections."""
        self._connections: dict[str, set[WebSocket]] = {}
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket, org_id: str) -> None:
        """Register a WebSocket for an org.

        Args:
            websocket: The accepted WebSocket connection.
            org_id: The organisation the connection belongs to.
        """
        async with self._lock:
            if org_id not in self._connections:
                self._connections[org_id] = set()
            self._connections[org_id].add(websocket)

    async def disconnect(self, websocket: WebSocket, org_id: str) -> None:
        """Unregister a WebSocket.

        Args:
            websocket: The WebSocket connection to drop.
            org_id: The organisation the connection belongs to.
        """
        async with self._lock:
            if org_id in self._connections:
                self._connections[org_id].discard(websocket)
                if not self._connections[org_id]:
                    del self._connections[org_id]

    async def broadcast(self, org_id: str, message: dict[str, object]) -> None:
        """Send a message to all connections for an org.

        Args:
            org_id: The organisation whose connections receive the message.
            message: The JSON-serializable payload to send.
        """
        async with self._lock:
            connections = list(self._connections.get(org_id, []))
        for websocket in connections:
            try:
                await websocket.send_json(message)
            except Exception:  # noqa: BLE001, S110 — one dead peer must not fail the broadcast
                # A client that vanished mid-broadcast is unremarkable: the
                # socket's own disconnect handler unregisters it.
                pass


_manager = ConnectionManager()


# -- NOTIFY listener -----------------------------------------------------------


def _start_notify_listener(dsn: str, loop: asyncio.AbstractEventLoop) -> None:
    """Background thread: listens for PostgreSQL NOTIFY and dispatches to WebSocket clients.

    Args:
        dsn: PostgreSQL connection string.
        loop: The asyncio event loop to schedule coroutines on.
    """
    while True:
        try:
            connection = psycopg2.connect(dsn)
            connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

            with connection.cursor() as cursor:
                cursor.execute("LISTEN table_changes")

            logger.info("[Realtime] NOTIFY listener started")

            while True:
                if _select.select([connection], [], [], 1.0):
                    connection.poll()
                    while connection.notifies:
                        notify = connection.notifies.pop(0)
                        try:
                            payload = json.loads(notify.payload)
                            org_id = str(payload["org_id"])
                            message = {
                                "table": payload["table"],
                                "event": payload["op"],
                                "record": payload.get("record"),
                            }
                            asyncio.run_coroutine_threadsafe(
                                _manager.broadcast(org_id, message), loop
                            )
                        except (json.JSONDecodeError, KeyError) as e:
                            logger.warning("[Realtime] Invalid NOTIFY payload: %s", e)
        except Exception as e:  # noqa: BLE001
            # This thread is the only realtime path; letting anything escape
            # would end it for the life of the process, so every failure is
            # logged and the connection retried instead.
            logger.error("[Realtime] NOTIFY listener error: %s, reconnecting in 5s...", e)
            import time
            time.sleep(5)


@asynccontextmanager
async def realtime_lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Start the NOTIFY listener thread on app startup.

    Reads the DSN from the engine already initialized by ``init_engine()``.

    Args:
        app: The FastAPI application being started.

    Yields:
        None, once the listener thread is running.
    """
    from interloper_db import get_engine

    engine = get_engine()
    # str(engine.url) masks the password as '***' — psycopg2 needs the real one.
    dsn = engine.url.render_as_string(hide_password=False)

    loop = asyncio.get_running_loop()
    thread = threading.Thread(
        target=_start_notify_listener,
        args=(dsn, loop),
        daemon=True,
        name="NotifyListener",
    )
    thread.start()
    yield


# -- Endpoints -----------------------------------------------------------------


@router.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket,
    session_token: str | None = Cookie(default=None),
) -> None:
    """Authenticate via session cookie, then stream table change events.

    Args:
        websocket: The incoming WebSocket connection.
        session_token: The session cookie value; None closes the connection as
            unauthorized.
    """
    store = get_store()

    if not session_token:
        await websocket.close(code=4001, reason="Unauthorized")
        return

    result = store.resolve_session(session_token)
    if not result:
        await websocket.close(code=4001, reason="Unauthorized")
        return

    _, session_row = result
    if not session_row.organisation_id:
        await websocket.close(code=4002, reason="No organisation selected")
        return

    org_id = str(session_row.organisation_id)
    logger.info("[WS] Authenticated. org=%s — accepting connection", org_id)

    await websocket.accept()
    await _manager.connect(websocket, org_id)

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        await _manager.disconnect(websocket, org_id)
