"""Tests for ``interloper_api.routes.runs`` — retry endpoint and org-membership scoping.

A lightweight fake store stands in for persistence so these stay pure unit
tests, matching the style of ``test_admin.py``.
"""

from __future__ import annotations

import datetime as dt
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from interloper.errors import NotFoundError

from interloper_api.dependencies import get_current_user, get_org_id, get_store, require_viewer
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
        self.list_calls: list[dict[str, object]] = []
        self.count_calls: list[dict[str, object]] = []
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

    def list_runs(self, org_id: UUID, **kwargs):
        self.list_calls.append(kwargs)
        return []

    def count_runs(self, org_id: UUID, **kwargs):
        self.count_calls.append(kwargs)
        return 0

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


# -- Quota ---------------------------------------------------------------------


def test_quota_exceeded_maps_to_429(store: FakeStore) -> None:
    """The app-level handler turns QuotaExceededError into a structured 429."""
    from interloper.errors import QuotaExceededError

    def _raise(org_id, **kwargs):
        raise QuotaExceededError("quota exhausted (3/3)", quota="max_successful_runs_per_month", limit=3, used=3)

    store.get_component = lambda component_id: SimpleNamespace(  # ty: ignore[unresolved-attribute]
        id=component_id, org_id=_ORG_ID, kind="job"
    )
    store.create_run = _raise  # ty: ignore[unresolved-attribute]
    client = _client(store)

    @client.app.exception_handler(QuotaExceededError)  # mirrors create_app's handler
    async def _quota_handler(_request, exc: QuotaExceededError):
        from fastapi.responses import JSONResponse

        return JSONResponse(
            status_code=429,
            content={"detail": {"message": str(exc), "quota": exc.quota, "limit": exc.limit, "used": exc.used}},
        )

    resp = client.post("/runs/", json={"component_id": str(uuid4())})
    assert resp.status_code == 429
    detail = resp.json()["detail"]
    assert detail["quota"] == "max_successful_runs_per_month"
    assert (detail["limit"], detail["used"]) == (3, 3)


# -- Listing -------------------------------------------------------------------


def test_list_runs_forwards_the_time_window(store: FakeStore) -> None:
    """A timeline view asks for one window; both the listing and its count honour it."""
    client = _client(store)
    client.app.dependency_overrides[require_viewer] = lambda: SimpleNamespace(id=uuid4())
    client.app.dependency_overrides[get_org_id] = lambda: _ORG_ID

    resp = client.get("/runs/", params={"after": "2026-02-04T00:00:00Z", "before": "2026-02-05T00:00:00Z"})

    assert resp.status_code == 200
    assert resp.headers["X-Total-Count"] == "0"
    window = (
        dt.datetime(2026, 2, 4, tzinfo=dt.timezone.utc),
        dt.datetime(2026, 2, 5, tzinfo=dt.timezone.utc),
    )
    assert (store.list_calls[0]["after"], store.list_calls[0]["before"]) == window
    assert (store.count_calls[0]["after"], store.count_calls[0]["before"]) == window


def test_list_runs_without_window_passes_none(store: FakeStore) -> None:
    client = _client(store)
    client.app.dependency_overrides[require_viewer] = lambda: SimpleNamespace(id=uuid4())
    client.app.dependency_overrides[get_org_id] = lambda: _ORG_ID

    assert client.get("/runs/").status_code == 200
    assert (store.list_calls[0]["after"], store.list_calls[0]["before"]) == (None, None)
