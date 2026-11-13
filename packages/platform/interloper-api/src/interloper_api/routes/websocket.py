"""WebSocket realtime endpoint — bridges PostgreSQL NOTIFY to connected clients.

Architecture:
- A background thread LISTENs on the ``table_changes`` PostgreSQL channel.
- When a notification arrives, the payload (table, op, org_id, record) is
  broadcast to all WebSocket clients belonging to that org.
- Clients subscribe to one organisation by connecting to
  ``/api/organisations/{org_id}/ws``, authenticated and authorised as every
  organisation route is, and again every ``RECHECK_SECONDS`` while the socket
  is open: a revoked or expired credential, or an ended membership, closes it.
  The check reads the database, so it holds whichever replica serves the socket.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from uuid import UUID

import psycopg
from fastapi import APIRouter, Cookie, FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from interloper_db import Store
from starlette.concurrency import run_in_threadpool

from interloper_api.dependencies import StoreDep, get_caller_credential, get_org_scope

logger = logging.getLogger(__name__)
org_router = APIRouter(tags=["websocket"])

RECHECK_SECONDS = 30.0


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
        dsn: libpq connection string of the database to listen on.
        loop: The asyncio event loop to schedule coroutines on.
    """
    while True:
        try:
            with psycopg.connect(dsn, autocommit=True) as connection:
                connection.execute("LISTEN table_changes")
                logger.info("[Realtime] NOTIFY listener started")

                for notify in connection.notifies():
                    try:
                        payload = json.loads(notify.payload)
                        org_id = str(payload["org_id"])
                        message = {
                            "table": payload["table"],
                            "event": payload["op"],
                            "record": payload.get("record"),
                        }
                        asyncio.run_coroutine_threadsafe(_manager.broadcast(org_id, message), loop)
                    except (json.JSONDecodeError, KeyError) as e:
                        logger.warning("[Realtime] Invalid NOTIFY payload: %s", e)
        except Exception as e:  # noqa: BLE001
            # This thread is the only realtime path; letting anything escape
            # would end it for the life of the process, so every failure is
            # logged and the connection retried instead.
            logger.error("[Realtime] NOTIFY listener error: %s, reconnecting in 5s...", e)
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
    # libpq refuses SQLAlchemy's driver name, and str(engine.url) would mask the password as '***'.
    dsn = engine.url.set(drivername="postgresql").render_as_string(hide_password=False)

    loop = asyncio.get_running_loop()
    thread = threading.Thread(
        target=_start_notify_listener,
        args=(dsn, loop),
        daemon=True,
        name="NotifyListener",
    )
    thread.start()
    yield


# -- Subscription --------------------------------------------------------------


class Subscription:
    """One socket streaming an organisation's changes for as long as its caller may read them."""

    def __init__(
        self,
        websocket: WebSocket,
        org_id: UUID,
        store: Store,
        session_token: str | None,
        authorization: str | None,
    ) -> None:
        """Hold what the caller presented, so it can be checked again while the socket is open.

        Args:
            websocket: The incoming WebSocket connection.
            org_id: The organisation whose changes are streamed.
            store: The Store the credential and membership are read from.
            session_token: The session cookie value, if any.
            authorization: The ``Authorization`` header value, if any.
        """
        self._websocket = websocket
        self._org_id = org_id
        self._channel = str(org_id)
        self._store = store
        self._session_token = session_token
        self._authorization = authorization

    async def serve(self) -> None:
        """Accept the socket, stream until the client leaves, and close it once the caller is refused."""
        refusal = await self._refusal()
        await self._websocket.accept()
        if refusal is None:
            refusal = await self._stream()
        if refusal is not None:
            unauthenticated = refusal.status_code == 401
            await self._websocket.close(
                code=4001 if unauthenticated else 4004, reason="Unauthorized" if unauthenticated else "Not found"
            )

    async def _refusal(self) -> HTTPException | None:
        """Check the caller's credential and membership as every organisation route does.

        Returns:
            The refusal an organisation route would raise for the caller now,
            or ``None`` while they may still read the organisation.
        """
        try:
            await run_in_threadpool(self._authorize)
        except HTTPException as exception:
            return exception
        return None

    def _authorize(self) -> None:
        """Resolve the caller's credential and their role in the organisation, raising when either fails."""
        user = get_caller_credential(self._store, self._session_token, self._authorization).user
        get_org_scope(self._org_id, user, self._store)

    async def _stream(self) -> HTTPException | None:
        """Broadcast to the socket, checking the caller again every ``RECHECK_SECONDS``.

        Returns:
            The refusal that ended the stream, or ``None`` when the client left.
        """
        await _manager.connect(self._websocket, self._channel)
        receiver = asyncio.ensure_future(self._drain())
        try:
            while True:
                done, _ = await asyncio.wait({receiver}, timeout=RECHECK_SECONDS)
                if done:
                    return None
                refusal = await self._refusal()
                if refusal is not None:
                    return refusal
        finally:
            receiver.cancel()
            await _manager.disconnect(self._websocket, self._channel)

    async def _drain(self) -> None:
        """Read and discard what the client sends, returning once it disconnects."""
        try:
            while True:
                await self._websocket.receive_text()
        except WebSocketDisconnect:
            pass


# -- Endpoints -----------------------------------------------------------------


@org_router.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket,
    org_id: UUID,
    store: StoreDep,
    session_token: str | None = Cookie(default=None),
) -> None:
    """Authenticate and authorise the caller in the path's organisation, then stream its table changes.

    A caller who cannot be authenticated is closed with 4001, one who cannot
    act in the organisation with 4004, at connect and whenever a later check
    finds the credential revoked or expired or the membership ended. The socket
    is accepted first: a close during the handshake reaches the browser as a
    1006 without its code, and the app reads these two as final instead of
    reconnecting.

    Args:
        websocket: The incoming WebSocket connection.
        org_id: The organisation whose changes are streamed, from the path.
        store: The store the caller is authenticated and authorised against.
        session_token: The session cookie value.
    """
    await Subscription(websocket, org_id, store, session_token, websocket.headers.get("authorization")).serve()
