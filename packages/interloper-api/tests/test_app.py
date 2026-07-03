"""Tests for the app factory — optional agent routes and feature flags."""

from types import SimpleNamespace

from fastapi.testclient import TestClient

from interloper_api.app import create_app
from interloper_api.dependencies import get_features, get_store


def test_agent_routes_absent_when_disabled():
    app = create_app(agent_config=SimpleNamespace(enabled=False))
    client = TestClient(app)

    assert client.post("/api/agent/sessions").status_code == 404
    assert get_features() == {"agent": False}


def test_agent_routes_mounted_when_enabled():
    app = create_app(agent_config=SimpleNamespace(enabled=True))
    app.dependency_overrides[get_store] = lambda: None
    client = TestClient(app)

    # 401 (not 404): the router is mounted and the auth guard answers first.
    assert client.post("/api/agent/sessions").status_code == 401
    assert get_features() == {"agent": True}
