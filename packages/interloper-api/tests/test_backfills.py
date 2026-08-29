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
        self.auth = SimpleNamespace(get_user_role=self._get_user_role)
        self.runs = SimpleNamespace(get_backfill=self._get_backfill, cancel_backfill=self._cancel_backfill)
        self.components = SimpleNamespace()

    def _get_backfill(self, backfill_id: UUID):
        if self.raise_not_found:
            raise NotFoundError(f"Backfill {backfill_id} not found")
        return _fake_backfill(backfill_id)

    def _get_user_role(self, user_id: UUID, org_id: UUID) -> str | None:
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
