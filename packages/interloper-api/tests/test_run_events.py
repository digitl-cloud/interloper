"""Tests for the ``GET /{run_id}/events`` pagination contract.

A lightweight fake store stands in for persistence so these stay pure unit
tests. The properties under test:

- ``limit``/``offset`` are forwarded to the store,
- ``limit`` is clamped to ``[1, MAX_EVENTS_PAGE_SIZE]`` and ``offset`` to ``>= 0``,
- the total event count is surfaced in the ``X-Total-Count`` header so the
  client can page through every event (including terminal/outcome events).
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from interloper_api.dependencies import get_current_user, get_store
from interloper_api.routes import runs as runs_module
from interloper_api.routes.runs import MAX_EVENTS_PAGE_SIZE

_RUN_ID = UUID("99c018d6-98fe-4de5-a867-1f1a9a545a38")
_ORG_ID = uuid4()


class FakeStore:
    """Records the pagination args it was called with and returns fakes."""

    def __init__(self, total: int = 777) -> None:
        self.total = total
        self.list_calls: list[tuple] = []
        self.count_calls: list[tuple] = []

    def get_run(self, run_id: UUID):
        return SimpleNamespace(id=run_id, org_id=_ORG_ID)

    def get_user_role(self, user_id: UUID, org_id: UUID) -> str | None:
        return "viewer"

    def count_events(
        self,
        *,
        run_id: UUID | None = None,
        org_id: UUID | None = None,
        asset_ids: list[UUID] | None = None,
        event_types: list[str] | None = None,
    ) -> int:
        self.count_calls.append((asset_ids, event_types))
        return self.total

    def list_events(
        self,
        *,
        run_id: UUID | None = None,
        org_id: UUID | None = None,
        asset_ids: list[UUID] | None = None,
        event_types: list[str] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list:
        self.list_calls.append((run_id, limit, offset, asset_ids, event_types))
        # Return as many fake events as the page would hold, capped at the total.
        n = max(0, min(limit, self.total - offset))
        return [
            runs_module.Event(
                id=uuid4(),
                org_id=_ORG_ID,
                run_id=run_id,
                event_type="asset_completed",
                timestamp=datetime.now(timezone.utc),
            )
            for _ in range(n)
        ]


def _client(store: FakeStore) -> TestClient:
    app = FastAPI()
    app.include_router(runs_module.router)
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(id=uuid4())
    return TestClient(app)


@pytest.fixture
def store() -> FakeStore:
    return FakeStore()


def test_returns_total_count_header(store: FakeStore) -> None:
    resp = _client(store).get(f"/runs/{_RUN_ID}/events")
    assert resp.status_code == 200
    assert resp.headers["X-Total-Count"] == "777"


def test_forwards_limit_and_offset(store: FakeStore) -> None:
    resp = _client(store).get(f"/runs/{_RUN_ID}/events?limit=100&offset=200")
    assert resp.status_code == 200
    assert store.list_calls[-1] == (_RUN_ID, 100, 200, None, None)


def test_limit_is_clamped_to_max_page_size(store: FakeStore) -> None:
    _client(store).get(f"/runs/{_RUN_ID}/events?limit=1000000")
    assert store.list_calls[-1][1] == MAX_EVENTS_PAGE_SIZE


def test_forwards_asset_filter_to_list_and_count(store: FakeStore) -> None:
    asset_id = uuid4()
    resp = _client(store).get(f"/runs/{_RUN_ID}/events?asset_id={asset_id}")
    assert resp.status_code == 200
    # A single asset_id arrives as a one-element list.
    assert store.list_calls[-1] == (_RUN_ID, 100, 0, [asset_id], None)
    # X-Total-Count must reflect the same filter the listing used.
    assert store.count_calls[-1] == ([asset_id], None)


def test_forwards_multiple_asset_filters(store: FakeStore) -> None:
    a, b = uuid4(), uuid4()
    resp = _client(store).get(f"/runs/{_RUN_ID}/events?asset_id={a}&asset_id={b}")
    assert resp.status_code == 200
    # Repeated asset_id params filter the listing to the whole set (e.g. one status).
    assert store.list_calls[-1] == (_RUN_ID, 100, 0, [a, b], None)
    assert store.count_calls[-1] == ([a, b], None)


def test_forwards_event_type_filter_to_list_and_count(store: FakeStore) -> None:
    resp = _client(store).get(f"/runs/{_RUN_ID}/events?event_type=log&event_type=asset_failed")
    assert resp.status_code == 200
    # Repeated event_type params filter to that set (e.g. a "Logs"/"Errors" tab).
    assert store.list_calls[-1] == (_RUN_ID, 100, 0, None, ["log", "asset_failed"])
    assert store.count_calls[-1] == (None, ["log", "asset_failed"])


def test_invalid_asset_filter_is_rejected(store: FakeStore) -> None:
    resp = _client(store).get(f"/runs/{_RUN_ID}/events?asset_id=not-a-uuid")
    assert resp.status_code == 422


def test_limit_and_offset_are_clamped_to_lower_bounds(store: FakeStore) -> None:
    _client(store).get(f"/runs/{_RUN_ID}/events?limit=0&offset=-5")
    _, limit, offset, _, _ = store.list_calls[-1]
    assert limit == 1
    assert offset == 0


def test_tail_page_reaches_terminal_events(store: FakeStore) -> None:
    # Paging to the final offset returns the outcome events that sort last.
    resp = _client(store).get(f"/runs/{_RUN_ID}/events?limit=100&offset=700")
    body = resp.json()
    assert len(body) == 77
    assert all(e["event_type"] == "asset_completed" for e in body)
