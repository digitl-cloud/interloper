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
from interloper.errors import NotFoundError

from interloper_api.dependencies import get_current_user, get_store
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
        self.raise_not_found = False
        self.raise_value_error: str | None = None
        #: Role the fake user holds in the backfill's org. None = not a member.
        self.role: str | None = "editor"
        self.organisations = SimpleNamespace(member_role=self._member_role)
        self.runs = SimpleNamespace(get_backfill=self._get_backfill, cancel_backfill=self._cancel_backfill)
        self.components = SimpleNamespace()

    def _get_backfill(self, backfill_id: UUID):
        if self.raise_not_found:
            raise NotFoundError(f"Backfill {backfill_id} not found")
        return _fake_backfill(backfill_id)

    def _member_role(self, user_id: UUID, org_id: UUID) -> str | None:
        return self.role

    def _cancel_backfill(self, backfill_id: UUID):
        self.cancel_calls.append(backfill_id)
        if self.raise_value_error is not None:
            raise ValueError(self.raise_value_error)
        return _fake_backfill(backfill_id, status="canceled")


def _client(store: FakeStore) -> TestClient:
    app = FastAPI()
    app.include_router(backfills_module.router)
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(id=uuid4())
    return TestClient(app)


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


def test_cancel_missing_backfill_returns_404(store: FakeStore) -> None:
    store.raise_not_found = True
    resp = _client(store).post(f"/backfills/{uuid4()}/cancel")
    assert resp.status_code == 404
    assert store.cancel_calls == []


def test_cancel_terminal_backfill_returns_409(store: FakeStore) -> None:
    store.raise_value_error = "Backfill is already canceled"
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
    from interloper.errors import QuotaExceededError

    def _raise(org_id, **kwargs):
        raise QuotaExceededError(
            "Backfill spans 31 partitions, exceeding the limit of 30",
            quota="max_backfill_partitions",
            limit=30,
            used=31,
        )

    store.components.get = lambda component_id, kind=None: _fake_backfill(component_id)
    store.runs.create_backfill = _raise
    client = _client(store)

    @client.app.exception_handler(QuotaExceededError)  # mirrors create_app's handler
    async def _quota_handler(_request, exc: QuotaExceededError):
        from fastapi.responses import JSONResponse

        return JSONResponse(
            status_code=429,
            content={"detail": {"message": str(exc), "quota": exc.quota, "limit": exc.limit, "used": exc.used}},
        )

    resp = client.post(
        "/backfills/",
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
    from interloper_api.dependencies import get_org_id, require_viewer

    app = FastAPI()
    app.include_router(backfills_module.router)
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(id=uuid4())
    app.dependency_overrides[require_viewer] = lambda: SimpleNamespace(id=uuid4())
    app.dependency_overrides[get_org_id] = lambda: _ORG_ID
    return TestClient(app)


def test_list_backfills_returns_the_orgs_backfills(store: FakeStore) -> None:
    """The default listing covers every backfill, terminal ones included."""
    listed: list[UUID] = []
    backfill_id = uuid4()
    store.runs.list_backfills = lambda org_id: listed.append(org_id) or [_fake_backfill(backfill_id)]
    store.runs.list_active_backfills = lambda org_id: []

    response = _list_client(store).get("/backfills/")

    assert response.status_code == 200
    assert [row["id"] for row in response.json()] == [str(backfill_id)]
    assert listed == [_ORG_ID]


def test_active_only_narrows_to_the_running_ones(store: FakeStore) -> None:
    """``active_only`` uses the dedicated store query, not a client-side filter."""
    active_id = uuid4()
    store.runs.list_backfills = lambda org_id: [_fake_backfill(uuid4())]
    store.runs.list_active_backfills = lambda org_id: [_fake_backfill(active_id)]

    response = _list_client(store).get("/backfills/?active_only=true")

    assert [row["id"] for row in response.json()] == [str(active_id)]


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
    """A store-level ``ValueError`` (bad keys, unpartitioned target) is a 400."""
    component_id = uuid4()
    store.components.get = lambda cid, kind=None: SimpleNamespace(id=cid, org_id=_ORG_ID)

    def create_backfill(org_id, **kwargs):
        raise ValueError("end_key precedes start_key")

    store.runs.create_backfill = create_backfill

    response = _client(store).post(
        "/backfills/",
        json={"component_id": str(component_id), "start_key": "2026-01-03", "end_key": "2026-01-01"},
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "end_key precedes start_key"


def test_create_backfill_returns_the_created_row(store: FakeStore) -> None:
    """A successful create echoes the stored backfill back."""
    backfill_id = uuid4()
    store.components.get = lambda cid, kind=None: SimpleNamespace(id=cid, org_id=_ORG_ID)
    store.runs.create_backfill = lambda org_id, **kwargs: _fake_backfill(backfill_id)

    response = _client(store).post(
        "/backfills/",
        json={"component_id": str(uuid4()), "start_key": "2026-01-01", "end_key": "2026-01-03"},
    )

    assert response.status_code == 201
    assert response.json()["id"] == str(backfill_id)
