"""Agent API: ADK-powered chat sessions with SSE streaming.

Provides endpoints for creating agent chat sessions, sending messages,
and streaming responses. The ADK Runner executes the interloper agent
in-process, reusing the API's authenticated Store and catalog.

Available when ``interloper-agent`` is installed.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Response
from fastapi.responses import StreamingResponse
from google.adk.apps import App
from google.adk.artifacts.in_memory_artifact_service import InMemoryArtifactService
from google.adk.runners import Runner
from google.adk.sessions.in_memory_session_service import InMemorySessionService
from google.genai import types
from interloper.catalog.base import Catalog
from interloper_db import Profile, Store
from pydantic import BaseModel

from interloper_api.dependencies import get_catalog, get_org_id, get_store, require_editor, require_viewer

logger = logging.getLogger(__name__)

router = APIRouter()

APP_NAME = "interloper_agent"

# ---------------------------------------------------------------------------
# Lazy runner singleton
# ---------------------------------------------------------------------------

_runner: Runner | None = None
_session_service: InMemorySessionService | None = None


def _get_session_service() -> InMemorySessionService:
    """Return the shared session service, creating it on first call."""
    global _session_service  # noqa: PLW0603
    if _session_service is None:
        _session_service = InMemorySessionService()
    return _session_service


def _get_runner(store: Store, catalog: Catalog) -> Runner:
    """Return the shared Runner, creating it on first call.

    On first call, injects the API's Store and catalog into the agent
    context so that agent tools can access them.

    Args:
        store: The Store instance (from API dependencies).
        catalog: The Catalog instance (from API dependencies).
    """
    global _runner  # noqa: PLW0603
    if _runner is None:
        from interloper_agent.agent import root_agent
        from interloper_agent.context import set_catalog, set_store

        if store is not None:
            set_store(store)
        if catalog is not None:
            set_catalog(catalog)

        app = App(name=APP_NAME, root_agent=root_agent)
        _runner = Runner(
            app=app,
            session_service=_get_session_service(),
            artifact_service=InMemoryArtifactService(),
        )
        logger.info("Agent runner initialized")
    return _runner


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


class ChatRequest(BaseModel):
    """Request body for sending a chat message."""

    message: str


class SessionResponse(BaseModel):
    """Response body for an agent session."""

    id: str
    user_id: str
    app_name: str
    state: dict[str, Any]
    last_update_time: float
    event_count: int


def _session_to_response(session: Any) -> SessionResponse:
    """Convert an ADK Session to a response model."""
    return SessionResponse(
        id=session.id,
        user_id=session.user_id,
        app_name=session.app_name,
        state=session.state,
        last_update_time=session.last_update_time,
        event_count=len(session.events),
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("/sessions")
async def create_session(
    user: Profile = Depends(require_editor),
    org_id: UUID = Depends(get_org_id),
    store: Store = Depends(get_store),
    catalog: Catalog = Depends(get_catalog),
) -> SessionResponse:
    """Create a new agent chat session.

    The authenticated user's ``org_id`` is injected into the ADK session
    state so agent tools are scoped to the correct organisation.
    """
    _get_runner(store=store, catalog=catalog)
    session_service = _get_session_service()
    session = await session_service.create_session(
        app_name=APP_NAME,
        user_id=str(user.id),
        state={"org_id": str(org_id)},
    )
    return _session_to_response(session)


@router.get("/sessions")
async def list_sessions(
    user: Profile = Depends(require_viewer),
) -> list[SessionResponse]:
    """List all agent sessions for the current user."""
    session_service = _get_session_service()
    result = await session_service.list_sessions(
        app_name=APP_NAME,
        user_id=str(user.id),
    )
    sessions = result.sessions if hasattr(result, "sessions") else result
    return [_session_to_response(s) for s in sessions]


@router.get("/sessions/{session_id}")
async def get_session(
    session_id: str,
    user: Profile = Depends(require_viewer),
) -> Response:
    """Get a session with its full event history.

    Returns the session object including all events (messages and tool calls).
    """
    session_service = _get_session_service()
    session = await session_service.get_session(
        app_name=APP_NAME,
        user_id=str(user.id),
        session_id=session_id,
    )
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    return Response(
        content=session.model_dump_json(exclude_none=True, by_alias=True),
        media_type="application/json",
    )


@router.delete("/sessions/{session_id}")
async def delete_session(
    session_id: str,
    user: Profile = Depends(require_editor),
) -> dict[str, str]:
    """Delete an agent session."""
    session_service = _get_session_service()
    session = await session_service.get_session(
        app_name=APP_NAME,
        user_id=str(user.id),
        session_id=session_id,
    )
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    await session_service.delete_session(
        app_name=APP_NAME,
        user_id=str(user.id),
        session_id=session_id,
    )
    return {"status": "deleted"}


@router.post("/sessions/{session_id}/chat")
async def chat(
    session_id: str,
    body: ChatRequest,
    user: Profile = Depends(require_editor),
    store: Store = Depends(get_store),
    catalog: Catalog = Depends(get_catalog),
) -> StreamingResponse:
    """Send a message and stream the agent's response as SSE.

    Each SSE ``data`` line contains a JSON-serialized ADK Event.
    Events with ``content.parts`` containing ``text`` are the agent's
    text responses. Events with ``function_call`` or ``function_response``
    parts represent tool invocations.
    """
    runner = _get_runner(store=store, catalog=catalog)
    user_id = str(user.id)
    message = types.Content(parts=[types.Part(text=body.message)], role="user")

    async def event_stream() -> AsyncIterator[str]:
        async for event in runner.run_async(
            user_id=user_id,
            session_id=session_id,
            new_message=message,
        ):
            yield f"data: {event.model_dump_json(exclude_none=True, by_alias=True)}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")
