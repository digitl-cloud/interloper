"""Tests for the internal event-ingest endpoint (``interloper_api.routes.internal``).

The endpoint is machine-to-machine: authenticated by a shared service token
(not a user session) and backed by ``Store.save_event`` (idempotent). A
lightweight fake store keeps these as pure unit tests.
"""

from __future__ import annotations

from types import SimpleNamespace
from uuid import UUID, uuid4

import interloper as il
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from interloper.errors import NotFoundError

from interloper_api.dependencies import get_store, set_ingest_token
from interloper_api.routes import internal as internal_module

TOKEN = "s3cret-token"


class FakeStore:
    """In-memory stand-in implementing only what the ingest route calls."""

    def __init__(self) -> None:
        self.org_id = uuid4()
        self.run_id = uuid4()
        self.saved: list[tuple[il.Event, UUID, UUID | None]] = []

    def get_run(self, run_id: UUID):
        if run_id != self.run_id:
            raise NotFoundError(f"Run {run_id} not found")
        return SimpleNamespace(id=run_id, org_id=self.org_id)

    def save_event(self, event: il.Event, org_id: UUID, run_id: UUID | None = None):
        self.saved.append((event, org_id, run_id))
        return SimpleNamespace(id=event.id)


def _client(store: FakeStore, *, token: str | None = TOKEN) -> TestClient:
    set_ingest_token(token)
    app = FastAPI()
    app.include_router(internal_module.router)
    app.dependency_overrides[get_store] = lambda: store
    return TestClient(app)


def _event(message: str = "hi") -> dict:
    return il.Event(type=il.EventType.LOG, metadata={"message": message}).to_dict()


@pytest.fixture
def store() -> FakeStore:
    return FakeStore()


@pytest.fixture(autouse=True)
def _reset_token():
    yield
    set_ingest_token(None)


def test_ingest_persists_events(store: FakeStore) -> None:
    client = _client(store)
    resp = client.post(
        f"/runs/{store.run_id}/events",
        json={"events": [_event("a"), _event("b")]},
        headers={"Authorization": f"Bearer {TOKEN}"},
    )
    assert resp.status_code == 200
    assert resp.json() == {"accepted": 2, "rejected": 0}
    assert len(store.saved) == 2
    # org_id is resolved server-side from the run; run_id comes from the path.
    assert all(org == store.org_id and run == store.run_id for _, org, run in store.saved)


def test_ingest_requires_valid_token(store: FakeStore) -> None:
    client = _client(store)
    # Missing header.
    assert client.post(f"/runs/{store.run_id}/events", json={"events": []}).status_code == 401
    # Wrong token.
    resp = client.post(
        f"/runs/{store.run_id}/events",
        json={"events": []},
        headers={"Authorization": "Bearer wrong"},
    )
    assert resp.status_code == 401
    assert store.saved == []


def test_ingest_disabled_when_unconfigured(store: FakeStore) -> None:
    client = _client(store, token=None)
    resp = client.post(
        f"/runs/{store.run_id}/events",
        json={"events": [_event()]},
        headers={"Authorization": f"Bearer {TOKEN}"},
    )
    assert resp.status_code == 503


def test_ingest_unknown_run_is_404(store: FakeStore) -> None:
    client = _client(store)
    resp = client.post(
        f"/runs/{uuid4()}/events",
        json={"events": [_event()]},
        headers={"Authorization": f"Bearer {TOKEN}"},
    )
    assert resp.status_code == 404
    assert store.saved == []


def test_ingest_skips_malformed_events(store: FakeStore) -> None:
    client = _client(store)
    resp = client.post(
        f"/runs/{store.run_id}/events",
        json={"events": [_event("ok"), {"type": "not_a_real_event", "timestamp": "nope"}]},
        headers={"Authorization": f"Bearer {TOKEN}"},
    )
    assert resp.status_code == 200
    assert resp.json() == {"accepted": 1, "rejected": 1}
    assert len(store.saved) == 1
