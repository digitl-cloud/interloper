"""Tests for ``interloper_api.routes.runs`` — focused on the retry endpoint.

A lightweight fake store stands in for persistence so these stay pure unit
tests, matching the style of ``test_admin.py``.
"""

from __future__ import annotations

from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from interloper.errors import RunNotFoundError

from interloper_api.dependencies import get_store, require_editor
from interloper_api.routes import runs as runs_module


class FakeStore:
    """In-memory stand-in implementing only what the retry route calls."""

    def __init__(self) -> None:
        self.retry_calls: list[tuple[UUID, str]] = []
        self.raise_not_found = False
        self.raise_value_error: str | None = None

    def retry_run(self, run_id: UUID, *, scope: str = "all"):
        self.retry_calls.append((run_id, scope))
        if self.raise_not_found:
            raise RunNotFoundError(f"Run {run_id} not found")
        if self.raise_value_error is not None:
            raise ValueError(self.raise_value_error)
        return SimpleNamespace(id=uuid4())


def _client(store: FakeStore) -> TestClient:
    app = FastAPI()
    app.include_router(runs_module.router)
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[require_editor] = lambda: SimpleNamespace(id=uuid4())
    return TestClient(app)


@pytest.fixture
def store() -> FakeStore:
    return FakeStore()


def test_retry_defaults_to_all_scope(store: FakeStore) -> None:
    run_id = uuid4()
    resp = _client(store).post(f"/{run_id}/retry")
    assert resp.status_code == 200
    assert resp.json()["status"] == "queued"
    assert store.retry_calls == [(run_id, "all")]


def test_retry_passes_failed_scope(store: FakeStore) -> None:
    run_id = uuid4()
    resp = _client(store).post(f"/{run_id}/retry", json={"scope": "failed"})
    assert resp.status_code == 200
    assert store.retry_calls[0][1] == "failed"


def test_retry_rejects_unknown_scope(store: FakeStore) -> None:
    resp = _client(store).post(f"/{uuid4()}/retry", json={"scope": "partial"})
    assert resp.status_code == 422
    assert store.retry_calls == []


def test_retry_missing_run_returns_404(store: FakeStore) -> None:
    store.raise_not_found = True
    resp = _client(store).post(f"/{uuid4()}/retry")
    assert resp.status_code == 404


def test_retry_non_failed_run_returns_409(store: FakeStore) -> None:
    store.raise_value_error = "Run is not failed"
    resp = _client(store).post(f"/{uuid4()}/retry")
    assert resp.status_code == 409
    assert "not failed" in resp.json()["detail"]
