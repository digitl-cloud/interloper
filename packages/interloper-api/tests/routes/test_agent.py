"""Tests for ``interloper_api.routes.agent``.

A real store over in-memory SQLite and the assistant over pydantic-ai's
deterministic test models, so a turn streams end to end without a network.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Iterator
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import interloper as il
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from interloper_agent import build_agent
from interloper_agent.toolset import toolset
from interloper_db import engine as engine_module
from interloper_db.models import (
    Backfill,
    Component,
    ComponentRelation,
    Conversation,
    Event,
    Organisation,
    Profile,
    Quota,
    Run,
    Usage,
    UserOrganisation,
)
from interloper_db.store import Store
from interloper_toolkit import ToolkitContext
from pydantic_ai import Agent, DeferredToolRequests
from pydantic_ai.messages import ModelMessage
from pydantic_ai.models.function import AgentInfo, DeltaToolCall, DeltaToolCalls, FunctionModel
from pydantic_ai.models.test import TestModel
from sqlalchemy import Engine, event
from sqlalchemy.pool import StaticPool

from interloper_api import app as app_module
from interloper_api.dependencies import (
    get_agent,
    get_catalog,
    get_current_user,
    get_org_id,
    get_store,
    require_editor,
    require_viewer,
)
from interloper_api.routes import agent as agent_module


@pytest.fixture
def agent_db() -> Iterator[Engine]:
    """A fresh in-memory database with the auth and data tables the routes touch.

    Yields:
        The engine bound to that database, disposed once the test finishes.
    """
    eng = engine_module.init_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)

    @event.listens_for(eng, "connect")
    def _configure_connection(dbapi_connection: Any, _record: Any) -> None:
        dbapi_connection.execute("PRAGMA foreign_keys=ON")
        dbapi_connection.create_function("gen_random_uuid", 0, lambda: uuid4().hex)

    models = (Profile, Organisation, UserOrganisation, Component, ComponentRelation, Backfill, Run, Event, Quota, Usage)
    for model in (*models, Conversation):
        model.__table__.create(eng)  # ty: ignore[unresolved-attribute]
    try:
        yield eng
    finally:
        eng.dispose()
        engine_module._engine = None


@pytest.fixture
def store(agent_db: Engine) -> Store:
    return Store(catalog=il.Catalog(components={}))


@pytest.fixture
def member(store: Store) -> SimpleNamespace:
    """An editor in a fresh organisation.

    Returns:
        The profile and organisation ids the routes resolve.
    """
    profile = store.auth.upsert_profile(google_id="g-1", email="ada@example.com", name="Ada")
    org = store.organisations.create(name="Acme", creator_id=profile.id)
    return SimpleNamespace(id=profile.id, org_id=org.id, email="ada@example.com", is_super_admin=False)


def _client(store: Store, member: SimpleNamespace, agent: Agent[ToolkitContext, Any]) -> TestClient:
    app = FastAPI()
    for error_type, handler in app_module._ERROR_HANDLERS.items():
        app.add_exception_handler(error_type, handler)
    app.include_router(agent_module.router)
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_catalog] = lambda: il.Catalog(components={})
    app.dependency_overrides[get_agent] = lambda: agent
    app.dependency_overrides[get_current_user] = lambda: member
    app.dependency_overrides[require_editor] = lambda: member
    app.dependency_overrides[require_viewer] = lambda: member
    app.dependency_overrides[get_org_id] = lambda: member.org_id
    return TestClient(app)


def _turn(text: str) -> dict[str, Any]:
    return {
        "trigger": "submit-message",
        "id": str(uuid4()),
        "messages": [{"id": str(uuid4()), "role": "user", "parts": [{"type": "text", "text": text}]}],
    }


def _echo_agent() -> Agent[ToolkitContext, Any]:
    agent = build_agent("google:gemini-2.5-flash")
    return agent  # the tests override its model per case


class TestConversationLifecycle:
    def test_create_list_read_and_delete(self, store: Store, member: SimpleNamespace):
        client = _client(store, member, _echo_agent())

        created = client.post("/agent/conversations")
        listed = client.get("/agent/conversations")
        detail = client.get(f"/agent/conversations/{created.json()['id']}")
        deleted = client.delete(f"/agent/conversations/{created.json()['id']}")

        assert created.status_code == 201
        assert [row["id"] for row in listed.json()] == [created.json()["id"]]
        assert detail.json()["messages"] == []
        assert deleted.status_code == 204
        assert client.get("/agent/conversations").json() == []

    def test_another_members_conversation_is_not_found(self, store: Store, member: SimpleNamespace):
        other = store.auth.upsert_profile(google_id="g-2", email="bob@example.com", name="Bob")
        theirs = store.conversations.create(member.org_id, other.id)
        client = _client(store, member, _echo_agent())

        assert client.get(f"/agent/conversations/{theirs.id}").status_code == 404
        assert client.delete(f"/agent/conversations/{theirs.id}").status_code == 404


class TestChat:
    def test_a_turn_streams_and_persists_the_history(self, store: Store, member: SimpleNamespace):
        agent = _echo_agent()
        client = _client(store, member, agent)
        conversation_id = client.post("/agent/conversations").json()["id"]

        with agent.override(model=TestModel(call_tools=[], custom_output_text="Hello Ada")):
            response = client.post(f"/agent/conversations/{conversation_id}/chat", json=_turn("hi there"))
            again = client.post(f"/agent/conversations/{conversation_id}/chat", json=_turn("and again"))

        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/event-stream")
        assert '"type":"text-delta"' in response.text and '"delta":"Ada"' in response.text
        assert again.status_code == 200
        row = store.conversations.get(UUID(conversation_id), org_id=member.org_id, user_id=member.id)
        assert row.title == "hi there"
        assert len(row.messages) == 4
        detail = client.get(f"/agent/conversations/{conversation_id}").json()
        assert [m["role"] for m in detail["messages"]] == ["user", "assistant", "user", "assistant"]
        assert detail["messages"][1]["parts"][0] == {"type": "text", "text": "Hello Ada", "state": "done"}

    def test_a_create_pauses_for_approval_and_runs_once_approved(self, store: Store, member: SimpleNamespace):
        async def model(messages: list[ModelMessage], info: AgentInfo) -> AsyncIterator[str | DeltaToolCalls]:
            if len(messages) == 1:
                args = {"name": "Daily", "cron": "0 6 * * *", "target_source_ids": [str(uuid4())]}
                yield {0: DeltaToolCall(name="create_job", json_args=json.dumps(args), tool_call_id="call-1")}
            else:
                yield "Job attempted"

        agent = Agent[ToolkitContext, str | DeferredToolRequests](
            FunctionModel(stream_function=model),
            deps_type=ToolkitContext,
            output_type=[str, DeferredToolRequests],
            toolsets=[toolset()],
        )
        client = _client(store, member, agent)
        conversation_id = client.post("/agent/conversations").json()["id"]

        paused = client.post(f"/agent/conversations/{conversation_id}/chat", json=_turn("schedule it"))
        approval = {
            "trigger": "submit-message",
            "id": str(uuid4()),
            "messages": [
                {
                    "id": "a1",
                    "role": "assistant",
                    "parts": [
                        {
                            "type": "tool-create_job",
                            "toolCallId": "call-1",
                            "state": "approval-responded",
                            "input": {"name": "Daily", "cron": "0 6 * * *", "target_source_ids": []},
                            "approval": {"id": "call-1", "approved": True},
                        }
                    ],
                }
            ],
        }
        resumed = client.post(f"/agent/conversations/{conversation_id}/chat", json=approval)

        assert paused.status_code == 200
        assert "tool-approval-request" in paused.text
        assert resumed.status_code == 200
        assert "Job attempted" in resumed.text
        assert "tool-output-available" in resumed.text

    def test_a_failing_turn_is_logged_server_side(
        self, store: Store, member: SimpleNamespace, caplog: pytest.LogCaptureFixture
    ):
        async def model(messages: list[ModelMessage], info: AgentInfo) -> AsyncIterator[str | DeltaToolCalls]:
            raise RuntimeError("provider exploded")
            yield ""

        agent = Agent[ToolkitContext, str | DeferredToolRequests](
            FunctionModel(stream_function=model), deps_type=ToolkitContext, output_type=[str, DeferredToolRequests]
        )
        client = _client(store, member, agent)
        conversation_id = client.post("/agent/conversations").json()["id"]

        with caplog.at_level("ERROR", logger="interloper_api.routes.agent"):
            response = client.post(f"/agent/conversations/{conversation_id}/chat", json=_turn("hi"))

        assert response.status_code == 200
        assert '"type":"error"' in response.text
        assert "Agent turn failed: provider exploded" in caplog.text
