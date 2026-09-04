"""Tests for ``interloper_api.routes.runs``.

Covers the retry endpoint, org-membership scoping, run creation, the
execution listing, and the event-pagination contract. A lightweight fake
store stands in for persistence so these stay pure unit tests, matching the
style of ``test_admin.py``.
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
from interloper_api.routes.runs import MAX_EVENTS_PAGE_SIZE

_ORG_ID = uuid4()
_RUN_ID = UUID("99c018d6-98fe-4de5-a867-1f1a9a545a38")


def _fake_run(run_id: UUID, org_id: UUID = _ORG_ID) -> SimpleNamespace:
    return SimpleNamespace(
        id=run_id,
        org_id=org_id,
        component_id=None,
        target=None,
        backfill_id=None,
        partition_key=None,
        status="failed",
        retry_of=None,
        attempt=1,
        retry_scope=None,
        started_at=None,
        completed_at=None,
        created_at=None,
    )


class FakeStore:
    """In-memory stand-in exposing only the store facets the run routes reach for."""

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
        self.organisations = SimpleNamespace(member_role=self._member_role)
        self.runs = SimpleNamespace(
            get=self._get_run,
            list_all=self._list_runs,
            count=self._count_runs,
            retry=self._retry_run,
        )
        self.components = SimpleNamespace()
        self.events = SimpleNamespace()

    def _get_run(self, run_id: UUID):
        if self.raise_not_found:
            raise NotFoundError(f"Run {run_id} not found")
        return _fake_run(run_id, self.run_org_id)

    def _member_role(self, user_id: UUID, org_id: UUID) -> str | None:
        return self.role

    def _list_runs(self, org_id: UUID, **kwargs):
        self.list_calls.append(kwargs)
        return []

    def _count_runs(self, org_id: UUID, **kwargs):
        self.count_calls.append(kwargs)
        return 0

    def _retry_run(self, run_id: UUID, *, scope: str = "all"):
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


def test_executions_return_404_for_non_member(store: FakeStore) -> None:
    store.role = None
    resp = _client(store).get(f"/runs/{uuid4()}/asset-executions")
    assert resp.status_code == 404


# -- Quota ---------------------------------------------------------------------


def test_quota_exceeded_maps_to_429(store: FakeStore) -> None:
    """The app-level handler turns QuotaExceededError into a structured 429."""
    from interloper.errors import QuotaExceededError

    def _raise(org_id, **kwargs):
        raise QuotaExceededError("quota exhausted (3/3)", quota="max_successful_runs_per_month", limit=3, used=3)

    store.components.get = lambda component_id: SimpleNamespace(id=component_id, org_id=_ORG_ID, kind="job")
    store.runs.create = _raise
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


# -- Create / executions --------------------------------------------------------


def test_create_run_rejects_an_invalid_partition(store: FakeStore) -> None:
    """A store-level ``ValueError`` (bad key, unpartitioned target) is a 400."""

    def create(org_id, **kwargs):
        raise ValueError("partition key '2026-13-01' is not a date")

    store.components.get = lambda cid, kind=None: SimpleNamespace(id=cid, org_id=_ORG_ID)
    store.runs.create = create

    response = _client(store).post(
        "/runs/", json={"component_id": str(uuid4()), "partition_key": "2026-13-01"}
    )

    assert response.status_code == 400
    assert "2026-13-01" in response.json()["detail"]


def test_list_executions_returns_the_runs_operations(store: FakeStore) -> None:
    """``GET /runs/{id}/executions`` reports one row per operation execution."""
    run_id = uuid4()
    component_id = uuid4()
    store.events = SimpleNamespace(
        list_executions=lambda rid: [
            SimpleNamespace(
                run_id=run_id,
                org_id=_ORG_ID,
                component_id=component_id,
                component_key="demo.a",
                status="completed",
                error=None,
                started_at=None,
                completed_at=None,
                created_at=None,
            )
        ]
    )

    response = _client(store).get(f"/runs/{run_id}/executions")

    assert response.status_code == 200
    assert [row["component_key"] for row in response.json()] == ["demo.a"]


def test_retry_a_missing_run_is_a_404(store: FakeStore) -> None:
    """A run that vanished between the load and the retry is a 404, not a 500."""
    run_id = uuid4()

    def retry(rid, scope):
        raise NotFoundError(f"Run {rid} not found")

    store.runs.retry = retry

    response = _client(store).post(f"/runs/{run_id}/retry", json={"scope": "all"})

    assert response.status_code == 404
    assert response.json()["detail"] == f"Run {run_id} not found"


def test_create_run_returns_the_queued_run(store: FakeStore) -> None:
    """A successful create echoes the stored run back."""
    run_id = uuid4()
    store.components.get = lambda cid, kind=None: SimpleNamespace(id=cid, org_id=_ORG_ID)
    store.runs.create = lambda org_id, **kwargs: _fake_run(run_id)

    response = _client(store).post("/runs/", json={"component_id": str(uuid4())})

    assert response.status_code in (200, 201)
    assert response.json()["id"] == str(run_id)


# -- Event pagination -----------------------------------------------------------


class EventsStore:
    """Records the pagination args it was called with and returns fakes."""

    def __init__(self, total: int = 777) -> None:
        self.total = total
        self.list_calls: list[tuple] = []
        self.count_calls: list[tuple] = []
        self.organisations = SimpleNamespace(member_role=self._member_role)
        self.runs = SimpleNamespace(get=self._get_run)
        self.events = SimpleNamespace(
            count=self._count_events,
            list_all=self._list_events,
        )

    def _get_run(self, run_id: UUID):
        return SimpleNamespace(id=run_id, org_id=_ORG_ID)

    def _member_role(self, user_id: UUID, org_id: UUID) -> str | None:
        return "viewer"

    def _count_events(
        self,
        *,
        run_id: UUID | None = None,
        org_id: UUID | None = None,
        component_ids: list[UUID] | None = None,
        event_types: list[str] | None = None,
    ) -> int:
        self.count_calls.append((component_ids, event_types))
        return self.total

    def _list_events(
        self,
        *,
        run_id: UUID | None = None,
        org_id: UUID | None = None,
        component_ids: list[UUID] | None = None,
        event_types: list[str] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list:
        self.list_calls.append((run_id, limit, offset, component_ids, event_types))
        # Return as many fake events as the page would hold, capped at the total.
        n = max(0, min(limit, self.total - offset))
        return [
            runs_module.Event(
                id=uuid4(),
                org_id=_ORG_ID,
                run_id=run_id,
                event_type="asset_completed",
                timestamp=dt.datetime.now(dt.timezone.utc),
            )
            for _ in range(n)
        ]


def _events_client(store: EventsStore) -> TestClient:
    app = FastAPI()
    app.include_router(runs_module.router)
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(id=uuid4())
    return TestClient(app)


@pytest.fixture
def events_store() -> EventsStore:
    return EventsStore()


def test_returns_total_count_header(events_store: EventsStore) -> None:
    resp = _events_client(events_store).get(f"/runs/{_RUN_ID}/events")
    assert resp.status_code == 200
    assert resp.headers["X-Total-Count"] == "777"


def test_forwards_limit_and_offset(events_store: EventsStore) -> None:
    resp = _events_client(events_store).get(f"/runs/{_RUN_ID}/events?limit=100&offset=200")
    assert resp.status_code == 200
    assert events_store.list_calls[-1] == (_RUN_ID, 100, 200, None, None)


def test_limit_is_clamped_to_max_page_size(events_store: EventsStore) -> None:
    _events_client(events_store).get(f"/runs/{_RUN_ID}/events?limit=1000000")
    assert events_store.list_calls[-1][1] == MAX_EVENTS_PAGE_SIZE


def test_forwards_component_filter_to_list_and_count(events_store: EventsStore) -> None:
    component_id = uuid4()
    resp = _events_client(events_store).get(f"/runs/{_RUN_ID}/events?component_id={component_id}")
    assert resp.status_code == 200
    # A single component_id arrives as a one-element list.
    assert events_store.list_calls[-1] == (_RUN_ID, 100, 0, [component_id], None)
    # X-Total-Count must reflect the same filter the listing used.
    assert events_store.count_calls[-1] == ([component_id], None)


def test_forwards_multiple_component_filters(events_store: EventsStore) -> None:
    a, b = uuid4(), uuid4()
    resp = _events_client(events_store).get(f"/runs/{_RUN_ID}/events?component_id={a}&component_id={b}")
    assert resp.status_code == 200
    # Repeated component_id params filter the listing to the whole set (e.g. one status).
    assert events_store.list_calls[-1] == (_RUN_ID, 100, 0, [a, b], None)
    assert events_store.count_calls[-1] == ([a, b], None)


def test_forwards_event_type_filter_to_list_and_count(events_store: EventsStore) -> None:
    resp = _events_client(events_store).get(f"/runs/{_RUN_ID}/events?event_type=log&event_type=asset_failed")
    assert resp.status_code == 200
    # Repeated event_type params filter to that set (e.g. a "Logs"/"Errors" tab).
    assert events_store.list_calls[-1] == (_RUN_ID, 100, 0, None, ["log", "asset_failed"])
    assert events_store.count_calls[-1] == (None, ["log", "asset_failed"])


def test_invalid_component_filter_is_rejected(events_store: EventsStore) -> None:
    resp = _events_client(events_store).get(f"/runs/{_RUN_ID}/events?component_id=not-a-uuid")
    assert resp.status_code == 422


def test_limit_and_offset_are_clamped_to_lower_bounds(events_store: EventsStore) -> None:
    _events_client(events_store).get(f"/runs/{_RUN_ID}/events?limit=0&offset=-5")
    _, limit, offset, _, _ = events_store.list_calls[-1]
    assert limit == 1
    assert offset == 0


def test_tail_page_reaches_terminal_events(events_store: EventsStore) -> None:
    # Paging to the final offset returns the outcome events that sort last.
    resp = _events_client(events_store).get(f"/runs/{_RUN_ID}/events?limit=100&offset=700")
    body = resp.json()
    assert len(body) == 77
    assert all(e["event_type"] == "asset_completed" for e in body)
