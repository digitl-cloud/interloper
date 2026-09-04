"""Tests for ``interloper_api.routes.health``."""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from interloper_api.routes import health as health_module


def test_the_liveness_probe_reports_ok() -> None:
    """Container and load-balancer probes get a body, not just a status."""
    app = FastAPI()
    app.include_router(health_module.router)

    response = TestClient(app).get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_it_needs_no_authentication() -> None:
    """The probe runs before any session exists, so it must not be gated."""
    app = FastAPI()
    app.include_router(health_module.router)

    # No dependency overrides installed at all.
    assert TestClient(app).get("/health").status_code == 200
