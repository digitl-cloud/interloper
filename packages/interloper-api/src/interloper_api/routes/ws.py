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
router = APIRouter()


class ConnectionManager:
    """Manages WebSocket connections grouped by organisation ID."""

    def __init__(self) -> None:
        self._connections: dict[str, set[WebSocket]] = {}
        self._lock = asyncio.Lock()

    async def connect(self, ws: WebSocket, org_id: str) -> None:
        """Register a WebSocket for an org."""
        async with self._lock:
            if org_id not in self._connections:
                self._connections[org_id] = set()
            self._connections[org_id].add(ws)

    async def disconnect(self, ws: WebSocket, org_id: str) -> None:
        """Unregister a WebSocket."""
        async with self._lock:
            if org_id in self._connections:
                self._connections[org_id].discard(ws)
                if not self._connections[org_id]:
                    del self._connections[org_id]

    async def broadcast(self, org_id: str, message: dict[str, object]) -> None:
        """Send a message to all connections for an org."""
        async with self._lock:
            connections = list(self._connections.get(org_id, []))
        for ws in connections:
            try:
                await ws.send_json(message)
            except Exception:
                pass


_manager = ConnectionManager()


def _start_notify_listener(dsn: str, loop: asyncio.AbstractEventLoop) -> None:
    """Background thread: listens for PostgreSQL NOTIFY and dispatches to WebSocket clients.

    Args:
        dsn: PostgreSQL connection string.
        loop: The asyncio event loop to schedule coroutines on.
    """
    while True:
        try:
            conn = psycopg2.connect(dsn)
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

            with conn.cursor() as cur:
                cur.execute("LISTEN table_changes")

            logger.info("[Realtime] NOTIFY listener started")

            while True:
                if _select.select([conn], [], [], 1.0):
                    conn.poll()
                    while conn.notifies:
                        notify = conn.notifies.pop(0)
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
        except Exception as e:
            logger.error("[Realtime] NOTIFY listener error: %s, reconnecting in 5s...", e)
            import time
            time.sleep(5)


@asynccontextmanager
async def realtime_lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Start the NOTIFY listener thread on app startup.

    Reads the DSN from the engine already initialized by ``init_engine()``.
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


@router.websocket("/ws")
async def websocket_endpoint(
    ws: WebSocket,
    session_token: str | None = Cookie(default=None),
) -> None:
    """Authenticate via session cookie, then stream table change events."""
    store = get_store()

    if not session_token:
        await ws.close(code=4001, reason="Unauthorized")
        return

    result = store.resolve_session(session_token)
    if not result:
        await ws.close(code=4001, reason="Unauthorized")
        return

    _, session_row = result
    if not session_row.organisation_id:
        await ws.close(code=4002, reason="No organisation selected")
        return

    org_id = str(session_row.organisation_id)
    logger.info("[WS] Authenticated. org=%s — accepting connection", org_id)

    await ws.accept()
    await _manager.connect(ws, org_id)

    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        await _manager.disconnect(ws, org_id)
