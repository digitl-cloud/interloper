"""Tests for ``interloper_api.routes.agent``.

The ADK's in-memory session service is real here — it needs no network —
while the Runner is faked, since building the real one pulls in the whole
agent and its model client.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from google.adk.sessions.in_memory_session_service import InMemorySessionService

from interloper_api.dependencies import get_catalog, get_org_id, get_store, require_editor, require_viewer
from interloper_api.routes import agent as agent_module

_ORG_ID = uuid4()
_USER_ID = uuid4()


@pytest.fixture(autouse=True)
def reset_singletons() -> Iterator[None]:
    """Give each test a fresh runner and session service.

    Both are process-wide lazily-built singletons, so a session created by
    one test would otherwise be visible to the next.

    Yields:
        ``None``; the teardown restores the originals.
    """
    saved = (agent_module._runner, agent_module._session_service)
    agent_module._runner = None
    agent_module._session_service = None
    yield
    agent_module._runner, agent_module._session_service = saved


def _profile() -> SimpleNamespace:
    return SimpleNamespace(id=_USER_ID, email="ada@example.com", is_super_admin=False)


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """Mount the agent router with the runner stubbed out.

    Args:
        monkeypatch: Fixture used to keep ``_get_runner`` from building the
            real ADK Runner.

    Returns:
        A client for the probe app.
    """
    monkeypatch.setattr(agent_module, "_get_runner", lambda store, catalog: SimpleNamespace())
    app = FastAPI()
    app.include_router(agent_module.router)
    app.dependency_overrides[get_store] = lambda: SimpleNamespace()
    app.dependency_overrides[get_catalog] = lambda: SimpleNamespace()
    app.dependency_overrides[get_org_id] = lambda: _ORG_ID
    app.dependency_overrides[require_viewer] = _profile
    app.dependency_overrides[require_editor] = _profile
    return TestClient(app)


class TestSessionService:
    """The lazily-built in-memory session service is a process-wide singleton."""

    def test_it_is_created_once_and_reused(self) -> None:
        first = agent_module._get_session_service()

        assert agent_module._get_session_service() is first
        assert isinstance(first, InMemorySessionService)


class TestGetRunner:
    """The runner injects the API's store and catalog into the agent context."""

    def test_the_store_and_catalog_reach_the_agent_context(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        injected: dict[str, Any] = {}
        store, catalog = SimpleNamespace(name="store"), SimpleNamespace(name="catalog")

        import interloper_agent.context as agent_context

        monkeypatch.setattr("interloper_agent.agent.root_agent", SimpleNamespace(name="root"))
        monkeypatch.setattr(agent_context, "set_store", lambda value: injected.update(store=value))
        monkeypatch.setattr(agent_context, "set_catalog", lambda value: injected.update(catalog=value))
        monkeypatch.setattr(agent_module, "App", lambda name, root_agent: SimpleNamespace(name=name))
        monkeypatch.setattr(agent_module, "Runner", lambda **kwargs: SimpleNamespace(**kwargs))

        runner = agent_module._get_runner(store=store, catalog=catalog)  # ty: ignore[invalid-argument-type]

        assert injected == {"store": store, "catalog": catalog}
        assert runner is not None

    def test_it_is_built_once_and_reused(self, monkeypatch: pytest.MonkeyPatch) -> None:
        builds: list[int] = []

        import interloper_agent.context as agent_context

        monkeypatch.setattr("interloper_agent.agent.root_agent", SimpleNamespace(name="root"))
        monkeypatch.setattr(agent_context, "set_store", lambda value: None)
        monkeypatch.setattr(agent_context, "set_catalog", lambda value: None)
        monkeypatch.setattr(agent_module, "App", lambda name, root_agent: SimpleNamespace(name=name))
        monkeypatch.setattr(
            agent_module, "Runner", lambda **kwargs: builds.append(1) or SimpleNamespace(**kwargs)
        )
        store, catalog = SimpleNamespace(), SimpleNamespace()

        first = agent_module._get_runner(store=store, catalog=catalog)  # ty: ignore[invalid-argument-type]
        second = agent_module._get_runner(store=store, catalog=catalog)  # ty: ignore[invalid-argument-type]

        assert first is second
        assert builds == [1]

    def test_a_missing_store_or_catalog_is_not_injected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        injected: dict[str, Any] = {}

        import interloper_agent.context as agent_context

        monkeypatch.setattr("interloper_agent.agent.root_agent", SimpleNamespace(name="root"))
        monkeypatch.setattr(agent_context, "set_store", lambda value: injected.update(store=value))
        monkeypatch.setattr(agent_context, "set_catalog", lambda value: injected.update(catalog=value))
        monkeypatch.setattr(agent_module, "App", lambda name, root_agent: SimpleNamespace(name=name))
        monkeypatch.setattr(agent_module, "Runner", lambda **kwargs: SimpleNamespace(**kwargs))

        agent_module._get_runner(store=None, catalog=None)  # ty: ignore[invalid-argument-type]

        assert injected == {}


class TestCreateSession:
    """``POST /agent/sessions`` — the org is stamped into the session state."""

    def test_the_active_org_is_stamped_into_the_state(self, client: TestClient) -> None:
        # Agent tools read org_id off the session state to scope themselves.
        response = client.post("/agent/sessions")

        assert response.status_code == 200
        payload = response.json()
        assert payload["state"] == {"org_id": str(_ORG_ID)}
        assert payload["user_id"] == str(_USER_ID)
        assert payload["app_name"] == agent_module.APP_NAME
        assert payload["event_count"] == 0


class TestListSessions:
    """``GET /agent/sessions`` — scoped to the calling user."""

    def test_lists_the_users_sessions(self, client: TestClient) -> None:
        created = client.post("/agent/sessions").json()

        response = client.get("/agent/sessions")

        assert [session["id"] for session in response.json()] == [created["id"]]

    def test_no_sessions_is_an_empty_list(self, client: TestClient) -> None:
        assert client.get("/agent/sessions").json() == []

    def test_another_users_sessions_are_not_listed(self, client: TestClient) -> None:
        client.post("/agent/sessions")
        other = SimpleNamespace(id=uuid4(), email="bob@example.com", is_super_admin=False)
        client.app.dependency_overrides[require_viewer] = lambda: other

        assert client.get("/agent/sessions").json() == []


class TestGetSession:
    """``GET /agent/sessions/{id}`` — the ADK's own serialization, verbatim."""

    def test_returns_the_session_with_its_events(self, client: TestClient) -> None:
        created = client.post("/agent/sessions").json()

        response = client.get(f"/agent/sessions/{created['id']}")

        assert response.status_code == 200
        assert response.headers["content-type"] == "application/json"
        assert response.json()["id"] == created["id"]

    def test_an_unknown_session_is_a_404(self, client: TestClient) -> None:
        response = client.get("/agent/sessions/not-a-session")

        assert response.status_code == 404
        assert response.json()["detail"] == "Session not found"


class TestDeleteSession:
    """``DELETE /agent/sessions/{id}``."""

    def test_deletes_the_session(self, client: TestClient) -> None:
        created = client.post("/agent/sessions").json()

        response = client.delete(f"/agent/sessions/{created['id']}")

        assert response.json() == {"status": "deleted"}
        assert client.get("/agent/sessions").json() == []

    def test_an_unknown_session_is_a_404(self, client: TestClient) -> None:
        response = client.delete("/agent/sessions/not-a-session")

        assert response.status_code == 404
        assert response.json()["detail"] == "Session not found"


class TestChat:
    """``POST /agent/sessions/{id}/chat`` — one SSE ``data`` line per ADK event."""

    def test_each_event_becomes_an_sse_data_line(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        class FakeEvent:
            def __init__(self, text: str) -> None:
                self._text = text

            def model_dump_json(self, **kwargs: Any) -> str:
                return f'{{"text": "{self._text}"}}'

        captured: dict[str, Any] = {}

        async def run_async(**kwargs: Any) -> AsyncIterator[FakeEvent]:
            captured.update(kwargs)
            yield FakeEvent("hello")
            yield FakeEvent("world")

        monkeypatch.setattr(
            agent_module, "_get_runner", lambda store, catalog: SimpleNamespace(run_async=run_async)
        )

        response = client.post("/agent/sessions/s-1/chat", json={"message": "hi"})

        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/event-stream")
        assert response.text == 'data: {"text": "hello"}\n\ndata: {"text": "world"}\n\n'

    def test_the_message_and_session_reach_the_runner(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured: dict[str, Any] = {}

        async def run_async(**kwargs: Any) -> AsyncIterator[Any]:
            captured.update(kwargs)
            return
            yield  # pragma: no cover - makes this an async generator

        monkeypatch.setattr(
            agent_module, "_get_runner", lambda store, catalog: SimpleNamespace(run_async=run_async)
        )

        client.post("/agent/sessions/s-1/chat", json={"message": "what failed?"})

        assert captured["user_id"] == str(_USER_ID)
        assert captured["session_id"] == "s-1"
        assert captured["new_message"].role == "user"
        assert captured["new_message"].parts[0].text == "what failed?"

    def test_a_missing_message_is_rejected(self, client: TestClient) -> None:
        assert client.post("/agent/sessions/s-1/chat", json={}).status_code == 422


class TestSessionResponse:
    """``from_session`` flattens the ADK session onto the wire model."""

    def test_the_event_count_replaces_the_event_list(self) -> None:
        session = SimpleNamespace(
            id="s-1",
            user_id=str(_USER_ID),
            app_name=agent_module.APP_NAME,
            state={"org_id": str(_ORG_ID)},
            last_update_time=1234.5,
            events=[object(), object(), object()],
        )

        response = agent_module.SessionResponse.from_session(session)

        assert response.event_count == 3
        assert response.last_update_time == 1234.5
        assert response.state == {"org_id": str(_ORG_ID)}
