"""Tests for ``interloper_api.routes.runs`` — retry endpoint and org-membership scoping.

A lightweight fake store stands in for persistence so these stay pure unit
tests, matching the style of ``test_admin.py``.
"""

from __future__ import annotations

from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from interloper.errors import NotFoundError

from interloper_api.dependencies import get_current_user, get_store
from interloper_api.routes import runs as runs_module

_ORG_ID = uuid4()


def _fake_run(run_id: UUID, org_id: UUID = _ORG_ID) -> SimpleNamespace:
    return SimpleNamespace(
        id=run_id,
        org_id=org_id,
        component_id=None,
        backfill_id=None,
        partition_date=None,
        status="failed",
        retry_of=None,
        attempt=1,
        retry_scope=None,
        started_at=None,
        completed_at=None,
        created_at=None,
    )


class FakeStore:
    """In-memory stand-in implementing only what the run routes call."""

    def __init__(self) -> None:
        self.retry_calls: list[tuple[UUID, str]] = []
        self.raise_not_found = False
        self.raise_value_error: str | None = None
        #: Role the fake user holds in the run's org. None = not a member.
        self.role: str | None = "editor"
        #: Org owning every run this store returns.
        self.run_org_id: UUID = _ORG_ID

    def get_run(self, run_id: UUID):
        if self.raise_not_found:
            raise NotFoundError(f"Run {run_id} not found")
        return _fake_run(run_id, self.run_org_id)

    def get_user_role(self, user_id: UUID, org_id: UUID) -> str | None:
        return self.role

    def retry_run(self, run_id: UUID, *, scope: str = "all"):
        self.retry_calls.append((run_id, scope))
        if self.raise_value_error is not None:
            raise ValueError(self.raise_value_error)
        return SimpleNamespace(id=uuid4())


def _client(store: FakeStore) -> TestClient:
    app = FastAPI()
    app.include_router(runs_module.router)
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(id=uuid4())
    return TestClient(app)


@pytest.fixture
def store() -> FakeStore:
    return FakeStore()


# -- Retry --------------------------------------------------------------------


def test_retry_defaults_to_all_scope(store: FakeStore) -> None:
    run_id = uuid4()
    resp = _client(store).post(f"/runs/{run_id}/retry")
    assert resp.status_code == 200
    assert resp.json()["status"] == "queued"
    assert store.retry_calls == [(run_id, "all")]


def test_retry_passes_failed_scope(store: FakeStore) -> None:
    run_id = uuid4()
    resp = _client(store).post(f"/runs/{run_id}/retry", json={"scope": "failed"})
    assert resp.status_code == 200
    assert store.retry_calls[0][1] == "failed"


def test_retry_rejects_unknown_scope(store: FakeStore) -> None:
    resp = _client(store).post(f"/runs/{uuid4()}/retry", json={"scope": "partial"})
    assert resp.status_code == 422
    assert store.retry_calls == []


def test_retry_missing_run_returns_404(store: FakeStore) -> None:
    store.raise_not_found = True
    resp = _client(store).post(f"/runs/{uuid4()}/retry")
    assert resp.status_code == 404


def test_retry_non_failed_run_returns_409(store: FakeStore) -> None:
    store.raise_value_error = "Run is not failed"
    resp = _client(store).post(f"/runs/{uuid4()}/retry")
    assert resp.status_code == 409
    assert "not failed" in resp.json()["detail"]


def test_retry_requires_editor_in_owning_org(store: FakeStore) -> None:
    store.role = "viewer"
    resp = _client(store).post(f"/runs/{uuid4()}/retry")
    assert resp.status_code == 403
    assert store.retry_calls == []


# -- Org-membership scoping ---------------------------------------------------


def test_get_run_allows_member_of_owning_org(store: FakeStore) -> None:
    run_id = uuid4()
    resp = _client(store).get(f"/runs/{run_id}")
    assert resp.status_code == 200
    assert resp.json()["org_id"] == str(_ORG_ID)


def test_get_run_returns_404_for_non_member(store: FakeStore) -> None:
    store.role = None
    run_id = uuid4()
    resp = _client(store).get(f"/runs/{run_id}")
    assert resp.status_code == 404
    # Identical detail to a missing run — IDs must not act as an existence oracle.
    assert resp.json()["detail"] == f"Run {run_id} not found"


def test_get_run_404_detail_matches_missing_run(store: FakeStore) -> None:
    run_id = uuid4()
    store.role = None
    non_member = _client(store).get(f"/runs/{run_id}").json()["detail"]
    store.role = "viewer"
    store.raise_not_found = True
    missing = _client(store).get(f"/runs/{run_id}").json()["detail"]
    assert non_member == missing


def test_run_events_return_404_for_non_member(store: FakeStore) -> None:
    store.role = None
    resp = _client(store).get(f"/runs/{uuid4()}/events")
    assert resp.status_code == 404


def test_asset_executions_return_404_for_non_member(store: FakeStore) -> None:
    store.role = None
    resp = _client(store).get(f"/runs/{uuid4()}/asset-executions")
    assert resp.status_code == 404
