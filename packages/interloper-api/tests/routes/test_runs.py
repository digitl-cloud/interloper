"""Tests for ``interloper_api.routes.runs``.

Covers the retry and cancel endpoints, org-membership scoping, run creation, the
listing's query and page contract, the execution listing, and the
event-pagination contract. A lightweight fake
store stands in for persistence so these stay pure unit tests, matching the
style of ``test_admin.py``.
"""

from __future__ import annotations

import datetime as dt
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import interloper as il
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from interloper.dag import DAGSpec
from interloper.errors import ConfigError, PartitionError
from interloper.runner.spec import RunSpec
from interloper_db import (
    ConflictError,
    EventQuery,
    ExecutionQuery,
    NotFoundError,
    Page,
    QuotaExceededError,
    RunQuery,
    RunStatus,
)

from interloper_api.app import ORG_PREFIX, install_error_handlers
from interloper_api.dependencies import get_caller, get_store, require_run
from interloper_api.routes import runs as runs_module

_ORG_ID = uuid4()
_RUN_ID = UUID("99c018d6-98fe-4de5-a867-1f1a9a545a38")
_BASE_URL = f"http://testserver/organisations/{_ORG_ID}"


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
        root_run_id=run_id,
        attempt=1,
        retry_scope=None,
        scheduled_for=None,
        error=None,
        started_at=None,
        completed_at=None,
        created_at=None,
    )


class FakeStore:
    """In-memory stand-in exposing only the store facets the run routes reach for."""

    def __init__(self) -> None:
        self.retry_calls: list[tuple[UUID, str]] = []
        self.cancel_calls: list[UUID] = []
        self.list_calls: list[tuple[UUID, RunQuery]] = []
        self.execution_calls: list[tuple[UUID, ExecutionQuery, UUID | None]] = []
        self.raise_not_found = False
        self.raise_conflict: str | None = None
        self.listed_runs: list[SimpleNamespace] = []
        self.execution_rows: list[SimpleNamespace] = []
        self.role: str | None = "editor"
        self.run_org_id: UUID = _ORG_ID
        self.members = SimpleNamespace(role_of=self._role_of)
        self.ingested: list[tuple[UUID, il.EventBatch]] = []
        self.served_spec: RunSpec | None = None
        self.spec_error: Exception | None = None
        self.organisations = SimpleNamespace(get=lambda org_id: SimpleNamespace(id=org_id))
        self.runs = SimpleNamespace(
            get=self._get_run,
            list=self._list_runs,
            retry=self._retry_run,
            cancel=self._cancel_run,
            ingest=self._ingest,
            spec=self._spec,
        )
        self.components = SimpleNamespace()
        self.executions = SimpleNamespace(counts=lambda run_ids: {}, list=self._list_executions)

    def _get_run(self, run_id: UUID):
        if self.raise_not_found:
            raise NotFoundError(f"Run {run_id} not found")
        return _fake_run(run_id, self.run_org_id)

    def _role_of(self, org_id: UUID, user: Any) -> str | None:
        return self.role

    def _list_runs(self, org_id: UUID, query: RunQuery) -> Page:
        self.list_calls.append((org_id, query))
        return Page.window(self.listed_runs, query)

    def _list_executions(self, org_id: UUID, query: ExecutionQuery, *, run_id: UUID | None = None) -> Page:
        self.execution_calls.append((org_id, query, run_id))
        return Page.window(self.execution_rows, query)

    def _retry_run(self, run_id: UUID, *, scope: str = "all"):
        self.retry_calls.append((run_id, scope))
        if self.raise_conflict is not None:
            raise ConflictError(self.raise_conflict)
        retried = _fake_run(uuid4())
        retried.status = "queued"
        retried.retry_of = run_id
        retried.root_run_id = run_id
        retried.attempt = 2
        retried.retry_scope = scope
        return retried

    def _spec(self, run_id: UUID) -> RunSpec | None:
        if self.spec_error is not None:
            raise self.spec_error
        return self.served_spec

    def _ingest(self, run_id: UUID, batch: il.EventBatch) -> bool:
        self.ingested.append((run_id, batch))
        return True

    def _cancel_run(self, run_id: UUID):
        self.cancel_calls.append(run_id)
        if self.raise_conflict is not None:
            raise ConflictError(self.raise_conflict)
        return _fake_run(run_id)


def _any_run(run_id: UUID) -> SimpleNamespace:
    """Stand in for a live run token, whatever the run.

    Args:
        run_id: The run the path names.

    Returns:
        The run.
    """
    return SimpleNamespace(id=run_id)


def _app(store: FakeStore) -> FastAPI:
    app = FastAPI()
    install_error_handlers(app)
    app.include_router(runs_module.org_router, prefix=ORG_PREFIX)
    app.include_router(runs_module.router)
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_caller] = lambda: SimpleNamespace(id=uuid4(), is_super_admin=False)
    app.dependency_overrides[require_run] = _any_run
    return app


def _client(store: FakeStore) -> TestClient:
    """A client for the probe app.

    Args:
        store: The fake store the routes resolve against.

    Returns:
        The client.
    """
    return TestClient(_app(store), base_url=_BASE_URL)


def _ingest_client(store: FakeStore) -> TestClient:
    """A client at the API root, where a run's pod reports, outside any organisation.

    Args:
        store: The fake store the routes resolve against.

    Returns:
        The client.
    """
    return TestClient(_app(store))


@pytest.fixture
def store() -> FakeStore:
    return FakeStore()


# -- Retry --------------------------------------------------------------------


def test_retry_defaults_to_all_scope(store: FakeStore) -> None:
    run_id = uuid4()
    resp = _client(store).post(f"/runs/{run_id}/retry")
    assert resp.status_code == 201
    assert resp.json()["status"] == "queued"
    assert store.retry_calls == [(run_id, "all")]


def test_retry_returns_the_queued_attempt(store: FakeStore) -> None:
    run_id = uuid4()

    body = _client(store).post(f"/runs/{run_id}/retry").json()

    assert body["id"] != str(run_id)
    assert body["retry_of"] == str(run_id)
    assert body["root_run_id"] == str(run_id)
    assert body["attempt"] == 2


def test_retry_passes_failed_scope(store: FakeStore) -> None:
    run_id = uuid4()
    resp = _client(store).post(f"/runs/{run_id}/retry", json={"scope": "failed"})
    assert resp.status_code == 201
    assert store.retry_calls[0][1] == "failed"
    assert resp.json()["retry_scope"] == "failed"


def test_retry_rejects_unknown_scope(store: FakeStore) -> None:
    resp = _client(store).post(f"/runs/{uuid4()}/retry", json={"scope": "partial"})
    assert resp.status_code == 422
    assert store.retry_calls == []


def test_retry_missing_run_returns_404(store: FakeStore) -> None:
    store.raise_not_found = True
    resp = _client(store).post(f"/runs/{uuid4()}/retry")
    assert resp.status_code == 404


def test_retry_non_failed_run_returns_409(store: FakeStore) -> None:
    store.raise_conflict = "Run is not failed"
    resp = _client(store).post(f"/runs/{uuid4()}/retry")
    assert resp.status_code == 409
    assert "not failed" in resp.json()["detail"]


def test_retry_requires_editor_in_owning_org(store: FakeStore) -> None:
    store.role = "viewer"
    resp = _client(store).post(f"/runs/{uuid4()}/retry")
    assert resp.status_code == 403
    assert store.retry_calls == []


def test_retry_a_missing_run_is_a_404(store: FakeStore) -> None:
    """A run that vanished between the load and the retry is a 404, not a 500."""
    run_id = uuid4()

    def retry(rid, scope):
        raise NotFoundError(f"Run {rid} not found")

    store.runs.retry = retry

    response = _client(store).post(f"/runs/{run_id}/retry", json={"scope": "all"})

    assert response.status_code == 404
    assert response.json()["detail"] == f"Run {run_id} not found"


# -- Cancel -------------------------------------------------------------------


def test_cancel_cancels_the_run(store: FakeStore) -> None:
    run_id = uuid4()

    resp = _client(store).post(f"/runs/{run_id}/cancel")

    assert resp.status_code == 200
    assert store.cancel_calls == [run_id]
    assert resp.json()["id"] == str(run_id)


def test_cancel_an_ended_run_returns_409(store: FakeStore) -> None:
    store.raise_conflict = "Run is already success"

    resp = _client(store).post(f"/runs/{uuid4()}/cancel")

    assert resp.status_code == 409


def test_cancel_requires_editor_in_owning_org(store: FakeStore) -> None:
    store.role = "viewer"

    resp = _client(store).post(f"/runs/{uuid4()}/cancel")

    assert resp.status_code == 403
    assert store.cancel_calls == []


# -- Org-membership scoping ---------------------------------------------------


def test_get_run_allows_member_of_owning_org(store: FakeStore) -> None:
    run_id = uuid4()
    resp = _client(store).get(f"/runs/{run_id}")
    assert resp.status_code == 200
    assert resp.json()["org_id"] == str(_ORG_ID)


def test_get_run_returns_404_for_non_member(store: FakeStore) -> None:
    store.role = None
    resp = _client(store).get(f"/runs/{uuid4()}")
    assert resp.status_code == 404
    assert resp.json()["detail"] == f"Organisation {_ORG_ID} not found"


def test_detail_of_other_org_404(store: FakeStore) -> None:
    # A run of organisation A, read under organisation B where the caller is
    # an editor, is missing there: identical to a run that does not exist.
    run_id = uuid4()
    store.run_org_id = uuid4()
    other_org = _client(store).get(f"/runs/{run_id}")
    store.raise_not_found = True
    missing = _client(store).get(f"/runs/{run_id}")

    assert other_org.status_code == missing.status_code == 404
    assert other_org.json()["detail"] == missing.json()["detail"] == f"Run {run_id} not found"


def test_a_write_on_a_run_of_another_org_is_refused(store: FakeStore) -> None:
    store.run_org_id = uuid4()

    resp = _client(store).post(f"/runs/{uuid4()}/cancel")

    assert resp.status_code == 404
    assert store.cancel_calls == []


def test_run_events_return_404_for_non_member(store: FakeStore) -> None:
    store.role = None
    resp = _client(store).get(f"/runs/{uuid4()}/events")
    assert resp.status_code == 404


def test_executions_return_404_for_non_member(store: FakeStore) -> None:
    store.role = None
    resp = _client(store).get(f"/runs/{uuid4()}/executions")
    assert resp.status_code == 404
    assert store.execution_calls == []


# -- Quota ---------------------------------------------------------------------


def test_quota_exceeded_maps_to_429(store: FakeStore) -> None:
    """The app-level handler turns QuotaExceededError into a structured 429."""

    def _raise(org_id, **kwargs):
        raise QuotaExceededError("quota exhausted (3/3)", quota="max_successful_runs_per_month", limit=3, used=3)

    store.components.get = lambda component_id: SimpleNamespace(id=component_id, org_id=_ORG_ID, kind="job")
    store.runs.create = _raise

    resp = _client(store).post("/runs", json={"component_id": str(uuid4())})

    assert resp.status_code == 429
    detail = resp.json()["detail"]
    assert detail["message"] == "quota exhausted (3/3)"
    assert detail["quota"] == "max_successful_runs_per_month"
    assert (detail["limit"], detail["used"]) == (3, 3)


# -- Listing -------------------------------------------------------------------


def _viewer_client(store: FakeStore) -> TestClient:
    """A client authenticated as a viewer of the fixture organisation.

    Args:
        store: The fake store the routes resolve against.

    Returns:
        The client.
    """
    store.role = "viewer"
    return _client(store)


def test_list_runs_is_a_page(store: FakeStore) -> None:
    store.listed_runs = [_fake_run(uuid4()), _fake_run(uuid4())]

    resp = _viewer_client(store).get("/runs")

    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 2
    assert [row["id"] for row in body["items"]] == [str(run.id) for run in store.listed_runs]
    assert "X-Total-Count" not in resp.headers


def test_list_runs_forwards_the_time_window(store: FakeStore) -> None:
    """A timeline view asks for one window."""
    resp = _viewer_client(store).get(
        "/runs", params={"after": "2026-02-04T00:00:00Z", "before": "2026-02-05T00:00:00Z"}
    )

    assert resp.status_code == 200
    assert resp.json() == {"items": [], "total": 0}
    ((org_id, query),) = store.list_calls
    assert org_id == _ORG_ID
    assert (query.after, query.before) == (
        dt.datetime(2026, 2, 4, tzinfo=dt.timezone.utc),
        dt.datetime(2026, 2, 5, tzinfo=dt.timezone.utc),
    )


def test_list_runs_forwards_every_query_field(store: FakeStore) -> None:
    component_id, backfill_id, root_run_id = uuid4(), uuid4(), uuid4()
    params = {
        "component_id": str(component_id),
        "backfill_id": str(backfill_id),
        "root_run_id": str(root_run_id),
        "status": "failed",
        "after": "2026-02-04T00:00:00Z",
        "before": "2026-02-05T00:00:00Z",
        "completed_after": "2026-02-04T06:00:00Z",
        "completed_before": "2026-02-04T18:00:00Z",
        "q": "swaro",
        "component_kind": "job",
        "component_key": "facebook_ads",
        "all_attempts": "true",
        "sort": "-partition_key",
        "limit": "25",
        "offset": "75",
    }

    resp = _viewer_client(store).get("/runs", params=params)

    assert resp.status_code == 200
    ((_, query),) = store.list_calls
    assert query == RunQuery(
        component_id=component_id,
        backfill_id=backfill_id,
        root_run_id=root_run_id,
        status=[RunStatus.FAILED],
        after=dt.datetime(2026, 2, 4, tzinfo=dt.timezone.utc),
        before=dt.datetime(2026, 2, 5, tzinfo=dt.timezone.utc),
        completed_after=dt.datetime(2026, 2, 4, 6, tzinfo=dt.timezone.utc),
        completed_before=dt.datetime(2026, 2, 4, 18, tzinfo=dt.timezone.utc),
        q="swaro",
        component_kind="job",
        component_key="facebook_ads",
        all_attempts=True,
        sort="-partition_key",
        limit=25,
        offset=75,
    )
    assert set(params) == set(RunQuery.model_fields)


def test_list_runs_defaults_to_one_row_per_stack(store: FakeStore) -> None:
    """Without filters the store is asked for stacks, not attempts, in the default window."""
    resp = _viewer_client(store).get("/runs")

    assert resp.status_code == 200
    ((_, query),) = store.list_calls
    assert query == RunQuery()
    assert query.root_run_id is None
    assert query.all_attempts is False
    assert (query.after, query.before) == (None, None)
    assert (query.limit, query.offset) == (50, 0)


def test_list_runs_rejects_an_unknown_sort(store: FakeStore) -> None:
    resp = _viewer_client(store).get("/runs", params={"sort": "org_id"})

    assert resp.status_code == 422
    assert store.list_calls == []


@pytest.mark.parametrize("params", [{"limit": 501}, {"limit": 0}, {"offset": -1}])
def test_list_runs_rejects_a_window_out_of_bounds(store: FakeStore, params: dict[str, int]) -> None:
    assert _viewer_client(store).get("/runs", params=params).status_code == 422
    assert store.list_calls == []


def test_list_runs_accepts_the_largest_page(store: FakeStore) -> None:
    assert _viewer_client(store).get("/runs", params={"limit": 500}).status_code == 200
    assert store.list_calls[0][1].limit == 500


def test_a_run_response_carries_its_stack(store: FakeStore) -> None:
    resp = _client(store).get(f"/runs/{_RUN_ID}")

    assert resp.status_code == 200
    body = resp.json()
    assert body["root_run_id"] == str(_RUN_ID)
    assert body["attempt"] == 1
    assert body["scheduled_for"] is None


def test_list_runs_carries_each_attempts_execution_counts(store: FakeStore) -> None:
    """Every listed run reports its own executions per status, counted for the whole page at once."""
    counted, pending = uuid4(), uuid4()
    asked: list[list[UUID]] = []

    def counts(run_ids: list[UUID]) -> dict[UUID, dict[str, int]]:
        asked.append(list(run_ids))
        return {counted: {"success": 2, "failed": 1}}

    store.listed_runs = [_fake_run(counted), _fake_run(pending)]
    store.executions.counts = counts

    resp = _viewer_client(store).get("/runs")

    assert resp.status_code == 200
    assert [row["execution_counts"] for row in resp.json()["items"]] == [{"success": 2, "failed": 1}, {}]
    assert asked == [[counted, pending]]


def test_get_run_carries_its_execution_counts(store: FakeStore) -> None:
    store.executions.counts = lambda run_ids: {run_ids[0]: {"running": 3}}

    resp = _client(store).get(f"/runs/{_RUN_ID}")

    assert resp.json()["execution_counts"] == {"running": 3}


# -- Create / executions --------------------------------------------------------


def test_create_run_rejects_an_invalid_partition(store: FakeStore) -> None:
    """A store-level ``ConfigError`` (bad key, unpartitioned target) is a 400."""

    def create(org_id, **kwargs):
        raise ConfigError("partition key '2026-13-01' is not a date")

    store.components.get = lambda cid, kind=None: SimpleNamespace(id=cid, org_id=_ORG_ID)
    store.runs.create = create

    response = _client(store).post("/runs", json={"component_id": str(uuid4()), "partition_key": "2026-13-01"})

    assert response.status_code == 400
    assert "2026-13-01" in response.json()["detail"]


def test_create_run_returns_the_queued_run(store: FakeStore) -> None:
    """A successful create echoes the stored run back."""
    run_id = uuid4()
    component_id = uuid4()
    created: list[tuple[UUID, dict]] = []

    def create(org_id, **kwargs):
        created.append((org_id, kwargs))
        return _fake_run(run_id)

    store.components.get = lambda cid, kind=None: SimpleNamespace(id=cid, org_id=_ORG_ID)
    store.runs.create = create

    response = _client(store).post("/runs", json={"component_id": str(component_id), "partition_key": "2026-01-01"})

    assert response.status_code == 201
    assert response.json()["id"] == str(run_id)
    assert created == [(_ORG_ID, {"component_id": component_id, "partition_key": "2026-01-01"})]


def test_list_executions_returns_the_runs_operations(store: FakeStore) -> None:
    """``GET /runs/{id}/executions`` reports one row per operation execution."""
    run_id = uuid4()
    store.execution_rows = [
        SimpleNamespace(
            run_id=run_id,
            org_id=_ORG_ID,
            component_id=uuid4(),
            component_key="demo.a",
            status="completed",
            error=None,
            started_at=None,
            completed_at=None,
            created_at=None,
        )
    ]

    response = _client(store).get(f"/runs/{run_id}/executions", params={"limit": 10, "offset": 0})

    assert response.status_code == 200
    body = response.json()
    assert [row["component_key"] for row in body["items"]] == ["demo.a"]
    assert body["total"] == 1
    ((org_id, query, listed_run_id),) = store.execution_calls
    assert org_id == _ORG_ID
    assert listed_run_id == run_id
    assert query == ExecutionQuery(limit=10, offset=0)


# -- Event pagination -----------------------------------------------------------


class EventsStore:
    """Records the queries it was called with and returns fakes."""

    def __init__(self, total: int = 777) -> None:
        self.total = total
        self.list_calls: list[tuple[UUID, EventQuery, UUID | None]] = []
        self.members = SimpleNamespace(role_of=lambda org_id, user: "viewer")
        self.organisations = SimpleNamespace(get=lambda org_id: SimpleNamespace(id=org_id))
        self.runs = SimpleNamespace(get=lambda run_id: SimpleNamespace(id=run_id, org_id=_ORG_ID))
        self.events = SimpleNamespace(list=self._list_events)

    def _list_events(self, org_id: UUID, query: EventQuery, *, run_id: UUID | None = None) -> Page:
        self.list_calls.append((org_id, query, run_id))
        # Return as many fake events as the page would hold, capped at the total.
        assert query.limit is not None
        n = max(0, min(query.limit, self.total - query.offset))
        events = [
            runs_module.Event(
                id=uuid4(),
                org_id=_ORG_ID,
                run_id=run_id,
                event_type="asset_completed",
                timestamp=dt.datetime.now(dt.timezone.utc),
            )
            for _ in range(n)
        ]
        return Page(items=events, total=self.total)


def _events_client(store: EventsStore) -> TestClient:
    app = FastAPI()
    install_error_handlers(app)
    app.include_router(runs_module.org_router, prefix=ORG_PREFIX)
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_caller] = lambda: SimpleNamespace(id=uuid4(), is_super_admin=False)
    return TestClient(app, base_url=_BASE_URL)


@pytest.fixture
def events_store() -> EventsStore:
    return EventsStore()


def test_returns_the_total_in_the_page(events_store: EventsStore) -> None:
    resp = _events_client(events_store).get(f"/runs/{_RUN_ID}/events")
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 777
    assert len(body["items"]) == 50
    assert "X-Total-Count" not in resp.headers


def test_reads_the_runs_org_and_the_run(events_store: EventsStore) -> None:
    _events_client(events_store).get(f"/runs/{_RUN_ID}/events")
    ((org_id, query, run_id),) = events_store.list_calls
    assert (org_id, run_id) == (_ORG_ID, _RUN_ID)
    assert query == EventQuery()


def test_forwards_limit_and_offset(events_store: EventsStore) -> None:
    resp = _events_client(events_store).get(f"/runs/{_RUN_ID}/events?limit=100&offset=200")
    assert resp.status_code == 200
    query = events_store.list_calls[-1][1]
    assert (query.limit, query.offset, query.component_id, query.event_type) == (100, 200, None, None)


@pytest.mark.parametrize("params", ["limit=1000000", "limit=501", "limit=0", "offset=-5"])
def test_a_window_out_of_bounds_is_a_422(events_store: EventsStore, params: str) -> None:
    resp = _events_client(events_store).get(f"/runs/{_RUN_ID}/events?{params}")
    assert resp.status_code == 422
    assert events_store.list_calls == []


def test_forwards_component_filter(events_store: EventsStore) -> None:
    component_id = uuid4()
    resp = _events_client(events_store).get(f"/runs/{_RUN_ID}/events?component_id={component_id}")
    assert resp.status_code == 200
    # A single component_id arrives as a one-element list.
    assert events_store.list_calls[-1][1].component_id == [component_id]


def test_forwards_multiple_component_filters(events_store: EventsStore) -> None:
    a, b = uuid4(), uuid4()
    resp = _events_client(events_store).get(f"/runs/{_RUN_ID}/events?component_id={a}&component_id={b}")
    assert resp.status_code == 200
    # Repeated component_id params filter the listing to the whole set (e.g. one status).
    assert events_store.list_calls[-1][1].component_id == [a, b]


def test_forwards_event_type_and_error_filters(events_store: EventsStore) -> None:
    resp = _events_client(events_store).get(
        f"/runs/{_RUN_ID}/events?event_type=log&event_type=asset_failed&has_error=true"
    )
    assert resp.status_code == 200
    # Repeated event_type params filter to that set (e.g. a "Logs"/"Errors" tab).
    query = events_store.list_calls[-1][1]
    assert query.event_type == ["log", "asset_failed"]
    assert query.has_error is True


def test_invalid_component_filter_is_rejected(events_store: EventsStore) -> None:
    resp = _events_client(events_store).get(f"/runs/{_RUN_ID}/events?component_id=not-a-uuid")
    assert resp.status_code == 422


def test_tail_page_reaches_terminal_events(events_store: EventsStore) -> None:
    # Paging to the final offset returns the outcome events that sort last.
    resp = _events_client(events_store).get(f"/runs/{_RUN_ID}/events?limit=100&offset=700")
    items = resp.json()["items"]
    assert len(items) == 77
    assert all(e["event_type"] == "asset_completed" for e in items)


# -- Ingest -------------------------------------------------------------------


def test_ingest_hands_the_batch_to_the_store_and_reports_whether_the_run_was_live(store: FakeStore) -> None:
    event_id = str(uuid4())
    event = {"event_id": event_id, "type": "log", "timestamp": "2026-10-26T10:00:00+00:00", "message": "hi"}
    effects = {"a": {"config": {"k": 1}, "state": {}}}

    response = _ingest_client(store).post(f"/runs/{_RUN_ID}/events", json={"events": [event], "effects": effects})

    assert response.status_code == 200 and response.json() == {"live": True}
    ((run_id, batch),) = store.ingested
    assert run_id == _RUN_ID
    assert [(item.id, item.metadata["message"]) for item in batch.events] == [(event_id, "hi")]
    assert batch.effects == {"a": il.Effect(config={"k": 1})}


def test_ingest_rejects_a_malformed_event(store: FakeStore) -> None:
    response = _ingest_client(store).post(f"/runs/{_RUN_ID}/events", json={"events": [{"type": "nope"}]})

    assert response.status_code == 422
    assert store.ingested == []


def test_ingest_rejects_a_malformed_effect(store: FakeStore) -> None:
    response = _ingest_client(store).post(
        f"/runs/{_RUN_ID}/events", json={"events": [], "effects": {"a": {"config": "rotated"}}}
    )

    assert response.status_code == 422
    assert store.ingested == []


def test_ingest_is_not_served_under_the_organisation(store: FakeStore) -> None:
    response = _client(store).post(f"/runs/{_RUN_ID}/events", json={"events": []})

    assert response.status_code == 405
    assert store.ingested == []


# -- Spec ---------------------------------------------------------------------


def _spec_client(store: FakeStore, status: str = "running") -> TestClient:
    """A client at the API root, as a run's process, whose token stands for a run in *status*.

    Args:
        store: The fake store the routes resolve against.
        status: The status of the run the token stands for.

    Returns:
        The client.
    """
    app = _app(store)
    app.dependency_overrides[require_run] = lambda run_id: SimpleNamespace(id=run_id, status=status)
    return TestClient(app)


@pytest.mark.parametrize("status", ["dispatched", "running"])
def test_a_live_run_is_served_its_spec(store: FakeStore, status: str) -> None:
    store.served_spec = RunSpec(dag=DAGSpec(), partition_key="2026-10-25", metadata={"run_id": str(_RUN_ID)})

    response = _spec_client(store, status).get(f"/runs/{_RUN_ID}/spec")

    assert response.status_code == 200
    assert RunSpec.model_validate(response.json()) == store.served_spec


@pytest.mark.parametrize("status", ["success", "failed", "canceled"])
def test_a_run_no_longer_live_is_not_served(store: FakeStore, status: str) -> None:
    store.served_spec = RunSpec(dag=DAGSpec())

    response = _spec_client(store, status).get(f"/runs/{_RUN_ID}/spec")

    assert response.status_code == 409
    assert "not live" in response.json()["detail"]


@pytest.mark.parametrize(
    ("error", "served"),
    [
        (NotFoundError("Run targets a component that was deleted"), None),
        (PartitionError("The run's window is not valid for every operation"), None),
        (None, None),
    ],
)
def test_a_run_that_cannot_be_built_is_refused_with_the_reason(
    store: FakeStore, error: Exception | None, served: RunSpec | None
) -> None:
    store.spec_error = error
    store.served_spec = served

    response = _spec_client(store).get(f"/runs/{_RUN_ID}/spec")

    assert response.status_code == 409
    assert response.json()["detail"] == (str(error) if error else f"Run {_RUN_ID} selects no operations")


def test_the_spec_is_not_served_under_the_organisation(store: FakeStore) -> None:
    assert _client(store).get(f"/runs/{_RUN_ID}/spec").status_code in (404, 405)
