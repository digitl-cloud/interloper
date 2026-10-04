"""Tests for ``interloper_api.routes.backfills`` — cancel endpoint and org-membership scoping.

A lightweight fake store stands in for persistence so these stay pure unit
tests, matching the style of ``test_runs.py``.
"""

from __future__ import annotations

from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from interloper.errors import ConfigError, ConflictError, NotFoundError, QuotaExceededError
from interloper_db import BackfillQuery, Page

from interloper_api.app import install_error_handlers
from interloper_api.dependencies import get_current_user, get_org_id, get_store, require_viewer
from interloper_api.routes import backfills as backfills_module

_ORG_ID = uuid4()


def _fake_backfill(backfill_id: UUID, status: str = "running") -> SimpleNamespace:
    return SimpleNamespace(
        id=backfill_id,
        org_id=_ORG_ID,
        component_id=None,
        target=None,
        status=status,
        start_key="2026-01-01",
        end_key="2026-01-03",
        concurrency=1,
        fail_fast=False,
        partitions=3,
        started_at=None,
        completed_at=None,
        created_at=None,
    )


class FakeStore:
    """In-memory stand-in exposing only the store facets the backfill routes reach for."""

    def __init__(self) -> None:
        self.cancel_calls: list[UUID] = []
        self.list_calls: list[tuple[UUID, BackfillQuery]] = []
        self.listed_backfills: list[SimpleNamespace] = []
        self.raise_not_found = False
        self.raise_conflict: str | None = None
        self.role: str | None = "editor"
        self.members = SimpleNamespace(role=self._member_role)
        self.backfills = SimpleNamespace(
            get=self._get_backfill,
            cancel=self._cancel_backfill,
            list=self._list_backfills,
            run_counts=lambda backfill_ids: {},
        )
        self.components = SimpleNamespace()

    def _list_backfills(self, org_id: UUID, query: BackfillQuery) -> Page:
        self.list_calls.append((org_id, query))
        return Page.window(self.listed_backfills, query)

    def _get_backfill(self, backfill_id: UUID):
        if self.raise_not_found:
            raise NotFoundError(f"Backfill {backfill_id} not found")
        return _fake_backfill(backfill_id)

    def _member_role(self, org_id: UUID, user_id: UUID) -> str | None:
        return self.role

    def _cancel_backfill(self, backfill_id: UUID):
        self.cancel_calls.append(backfill_id)
        if self.raise_conflict is not None:
            raise ConflictError(self.raise_conflict)
        return _fake_backfill(backfill_id, status="canceled")


def _app(store: FakeStore) -> FastAPI:
    app = FastAPI()
    install_error_handlers(app)
    app.include_router(backfills_module.router)
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(id=uuid4())
    return app


def _client(store: FakeStore) -> TestClient:
    """A client for the probe app.

    Args:
        store: The fake store the routes resolve against.

    Returns:
        The client.
    """
    return TestClient(_app(store))


@pytest.fixture
def store() -> FakeStore:
    return FakeStore()


# -- Cancel ---------------------------------------------------------------------


def test_cancel_returns_the_canceled_backfill(store: FakeStore) -> None:
    backfill_id = uuid4()
    resp = _client(store).post(f"/backfills/{backfill_id}/cancel")
    assert resp.status_code == 200
    assert resp.json()["status"] == "canceled"
    assert store.cancel_calls == [backfill_id]


def test_cancel_reports_the_partitions_per_status(store: FakeStore) -> None:
    backfill_id = uuid4()
    store.backfills.run_counts = lambda backfill_ids: {backfill_id: {"success": 2, "canceled": 1}}

    resp = _client(store).post(f"/backfills/{backfill_id}/cancel")

    assert resp.json()["run_counts"] == {"success": 2, "canceled": 1}


def test_cancel_missing_backfill_returns_404(store: FakeStore) -> None:
    store.raise_not_found = True
    resp = _client(store).post(f"/backfills/{uuid4()}/cancel")
    assert resp.status_code == 404
    assert store.cancel_calls == []


def test_cancel_terminal_backfill_returns_409(store: FakeStore) -> None:
    store.raise_conflict = "Backfill is already canceled"
    resp = _client(store).post(f"/backfills/{uuid4()}/cancel")
    assert resp.status_code == 409
    assert "already canceled" in resp.json()["detail"]


def test_cancel_requires_editor_in_owning_org(store: FakeStore) -> None:
    store.role = "viewer"
    resp = _client(store).post(f"/backfills/{uuid4()}/cancel")
    assert resp.status_code == 403
    assert store.cancel_calls == []


def test_cancel_returns_404_for_non_member(store: FakeStore) -> None:
    store.role = None
    resp = _client(store).post(f"/backfills/{uuid4()}/cancel")
    assert resp.status_code == 404
    assert store.cancel_calls == []


def test_create_backfill_over_span_quota_returns_429(store: FakeStore) -> None:
    def _raise(org_id, **kwargs):
        raise QuotaExceededError(
            "Backfill spans 31 partitions, exceeding the limit of 30",
            quota="max_backfill_partitions",
            limit=30,
            used=31,
        )

    store.components.get = lambda component_id, kind=None: _fake_backfill(component_id)
    store.backfills.create = _raise

    resp = _client(store).post(
        "/backfills",
        json={"component_id": str(uuid4()), "start_key": "2026-01-01", "end_key": "2026-01-31"},
    )
    assert resp.status_code == 429
    assert resp.json()["detail"]["quota"] == "max_backfill_partitions"


# -- List / get -----------------------------------------------------------------


def _list_client(store: FakeStore) -> TestClient:
    """Mount the router with the viewer gate and active org satisfied.

    Args:
        store: The fake store the routes resolve against.

    Returns:
        A client for the probe app.
    """
    app = FastAPI()
    install_error_handlers(app)
    app.include_router(backfills_module.router)
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(id=uuid4())
    app.dependency_overrides[require_viewer] = lambda: SimpleNamespace(id=uuid4())
    app.dependency_overrides[get_org_id] = lambda: _ORG_ID
    return TestClient(app)


def test_list_backfills_returns_the_orgs_backfills(store: FakeStore) -> None:
    """The default listing covers every backfill, terminal ones included."""
    backfill_id = uuid4()
    store.listed_backfills = [_fake_backfill(backfill_id)]

    response = _list_client(store).get("/backfills")

    assert response.status_code == 200
    assert response.json()["total"] == 1
    assert [row["id"] for row in response.json()["items"]] == [str(backfill_id)]
    ((org_id, query),) = store.list_calls
    assert org_id == _ORG_ID
    assert query == BackfillQuery()
    assert query.status is None


def test_list_backfills_carries_each_ones_run_counts(store: FakeStore) -> None:
    """Every listed backfill reports its partitions per status, counted for the whole page at once."""
    counted, empty = uuid4(), uuid4()
    asked: list[list[UUID]] = []

    def count_backfill_runs(backfill_ids: list[UUID]) -> dict[UUID, dict[str, int]]:
        asked.append(list(backfill_ids))
        return {counted: {"success": 2, "queued": 1}}

    store.listed_backfills = [_fake_backfill(counted), _fake_backfill(empty)]
    store.backfills.run_counts = count_backfill_runs

    response = _list_client(store).get("/backfills")

    assert response.status_code == 200
    assert [row["run_counts"] for row in response.json()["items"]] == [{"success": 2, "queued": 1}, {}]
    assert asked == [[counted, empty]]


def test_get_backfill_carries_its_run_counts(store: FakeStore) -> None:
    store.backfills.run_counts = lambda backfill_ids: {backfill_ids[0]: {"failed": 3}}

    response = _client(store).get(f"/backfills/{uuid4()}")

    assert response.json()["run_counts"] == {"failed": 3}


def test_the_status_filter_is_forwarded_to_the_store(store: FakeStore) -> None:
    """``status=queued&status=running`` narrows in the store query, not a client-side filter."""
    response = _list_client(store).get("/backfills?status=queued&status=running")

    assert response.status_code == 200
    ((_, query),) = store.list_calls
    assert query.status == ["queued", "running"]


def test_list_backfills_forwards_the_page_window(store: FakeStore) -> None:
    store.listed_backfills = [_fake_backfill(uuid4()) for _ in range(3)]

    body = _list_client(store).get("/backfills", params={"limit": 1, "offset": 1}).json()

    ((_, query),) = store.list_calls
    assert (query.limit, query.offset) == (1, 1)
    assert [row["id"] for row in body["items"]] == [str(store.listed_backfills[1].id)]
    assert body["total"] == 3


def test_list_backfills_rejects_a_page_larger_than_the_cap(store: FakeStore) -> None:
    assert _list_client(store).get("/backfills", params={"limit": 501}).status_code == 422
    assert store.list_calls == []


def test_get_backfill_returns_it(store: FakeStore) -> None:
    """A member of the owning org can address a backfill by id."""
    backfill_id = uuid4()

    response = _client(store).get(f"/backfills/{backfill_id}")

    assert response.status_code == 200
    assert response.json()["id"] == str(backfill_id)


def test_get_missing_backfill_returns_404(store: FakeStore) -> None:
    """A missing backfill names itself in the detail."""
    store.raise_not_found = True
    backfill_id = uuid4()

    response = _client(store).get(f"/backfills/{backfill_id}")

    assert response.status_code == 404
    assert response.json()["detail"] == f"Backfill {backfill_id} not found"


def test_get_backfill_of_another_org_returns_404(store: FakeStore) -> None:
    """A non-member gets the same 404, so the id is not an existence oracle."""
    store.role = None
    backfill_id = uuid4()

    response = _client(store).get(f"/backfills/{backfill_id}")

    assert response.status_code == 404
    assert response.json()["detail"] == f"Backfill {backfill_id} not found"


def test_create_backfill_rejects_an_invalid_span(store: FakeStore) -> None:
    """A store-level ``ConfigError`` (bad keys, unpartitioned target) is a 400."""
    component_id = uuid4()
    store.components.get = lambda cid, kind=None: SimpleNamespace(id=cid, org_id=_ORG_ID)

    def create_backfill(org_id, **kwargs):
        raise ConfigError("end_key precedes start_key")

    store.backfills.create = create_backfill

    response = _client(store).post(
        "/backfills",
        json={"component_id": str(component_id), "start_key": "2026-01-03", "end_key": "2026-01-01"},
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "end_key precedes start_key"


def test_create_backfill_returns_the_created_row(store: FakeStore) -> None:
    """A successful create echoes the stored backfill back."""
    backfill_id = uuid4()
    store.components.get = lambda cid, kind=None: SimpleNamespace(id=cid, org_id=_ORG_ID)
    created: list[tuple[UUID, dict]] = []

    def create(org_id, **kwargs):
        created.append((org_id, kwargs))
        return _fake_backfill(backfill_id)

    store.backfills.create = create
    component_id = uuid4()

    response = _client(store).post(
        "/backfills",
        json={"component_id": str(component_id), "start_key": "2026-01-01", "end_key": "2026-01-03"},
    )

    assert response.status_code == 201
    assert response.json()["id"] == str(backfill_id)
    assert created == [
        (
            _ORG_ID,
            {
                "component_id": component_id,
                "start_key": "2026-01-01",
                "end_key": "2026-01-03",
                "concurrency": 1,
                "fail_fast": False,
            },
        )
    ]
