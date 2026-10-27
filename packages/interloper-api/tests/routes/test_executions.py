"""Tests for ``interloper_api.routes.executions``: operation verdicts across runs."""

from __future__ import annotations

from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from interloper_db import ExecutionQuery, Page

from interloper_api.app import ORG_PREFIX, install_error_handlers
from interloper_api.dependencies import get_caller, get_store
from interloper_api.routes import executions as executions_module

_ORG_ID = uuid4()
_BASE_URL = f"http://testserver/organisations/{_ORG_ID}"


def _execution(component_key: str | None = "demo.a") -> SimpleNamespace:
    return SimpleNamespace(
        run_id=uuid4(),
        org_id=_ORG_ID,
        component_id=uuid4(),
        component_key=component_key,
        status="success",
        started_at=None,
        completed_at=None,
        created_at=None,
    )


class ExecutionStore:
    """Fake store exposing ``executions.list``."""

    def __init__(self) -> None:
        """Set up the rows and the recorder."""
        self.rows: list[SimpleNamespace] = []
        self.listed: list[tuple[UUID, ExecutionQuery, UUID | None]] = []
        self.executions = SimpleNamespace(list=self._list)
        self.members = SimpleNamespace(role=lambda org_id, user_id: "viewer")
        self.organisations = SimpleNamespace(get=lambda org_id: SimpleNamespace(id=org_id))

    def _list(self, org_id: UUID, query: ExecutionQuery, *, run_id: UUID | None = None) -> Page:
        self.listed.append((org_id, query, run_id))
        return Page.window(self.rows, query)


@pytest.fixture
def store() -> ExecutionStore:
    """A fresh fake store.

    Returns:
        The fake store.
    """
    return ExecutionStore()


@pytest.fixture
def client(store: ExecutionStore) -> TestClient:
    """Mount the executions router as a viewer of the fixture organisation.

    Args:
        store: The fake store the route reads.

    Returns:
        A client for the probe app.
    """
    user = SimpleNamespace(id=uuid4(), is_super_admin=False)
    app = FastAPI()
    install_error_handlers(app)
    app.include_router(executions_module.org_router, prefix=ORG_PREFIX)
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_caller] = lambda: user
    return TestClient(app, base_url=_BASE_URL)


class TestListExecutions:
    """``GET /executions``, optionally each component's newest only."""

    def test_latest_executions_report_the_orgs_newest_per_component(
        self, client: TestClient, store: ExecutionStore
    ) -> None:
        store.rows = [_execution("demo.a")]

        response = client.get("/executions", params={"latest": "true"})

        assert response.status_code == 200
        body = response.json()
        assert [row["component_key"] for row in body["items"]] == ["demo.a"]
        assert body["total"] == 1
        ((org_id, query, run_id),) = store.listed
        assert org_id == _ORG_ID
        assert query == ExecutionQuery(latest=True)
        assert run_id is None

    def test_without_the_flag_every_execution_is_listed(self, client: TestClient, store: ExecutionStore) -> None:
        client.get("/executions")

        ((_, query, _),) = store.listed
        assert query.latest is False

    def test_a_components_executions_are_listed_in_the_asked_order(
        self, client: TestClient, store: ExecutionStore
    ) -> None:
        component_id = uuid4()

        client.get("/executions", params={"component_id": str(component_id), "sort": "-created_at"})

        ((_, query, _),) = store.listed
        assert (query.component_id, query.sort) == (component_id, "-created_at")

    def test_a_row_without_a_component_key_reads_as_empty(self, client: TestClient, store: ExecutionStore) -> None:
        store.rows = [_execution(component_key=None)]

        [row] = client.get("/executions").json()["items"]

        assert row["component_key"] == ""

    def test_the_page_window_is_forwarded(self, client: TestClient, store: ExecutionStore) -> None:
        store.rows = [_execution(f"demo.{i}") for i in range(4)]

        body = client.get("/executions", params={"limit": 2, "offset": 2}).json()

        ((_, query, _),) = store.listed
        assert (query.limit, query.offset) == (2, 2)
        assert [row["component_key"] for row in body["items"]] == ["demo.2", "demo.3"]
        assert body["total"] == 4

    def test_a_page_larger_than_the_cap_is_a_422(self, client: TestClient, store: ExecutionStore) -> None:
        assert client.get("/executions", params={"limit": 501}).status_code == 422
        assert store.listed == []

    def test_the_listing_requires_a_viewer(self, store: ExecutionStore) -> None:
        app = FastAPI()
        install_error_handlers(app)
        app.include_router(executions_module.org_router, prefix=ORG_PREFIX)
        app.dependency_overrides[get_store] = lambda: store

        assert TestClient(app, base_url=_BASE_URL).get("/executions").status_code == 401
