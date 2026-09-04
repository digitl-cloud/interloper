"""Tests for the app factory — error handlers, optional agent routes, feature flags."""

from __future__ import annotations

import sys
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from interloper.errors import ComponentDriftError, HydrationError, NotFoundError, QuotaExceededError

from interloper_api import app as app_module
from interloper_api.app import create_app
from interloper_api.dependencies import get_features, get_store


def test_agent_routes_absent_when_disabled(fake_settings: SimpleNamespace):
    fake_settings.agent.enabled = False
    app = create_app(settings=fake_settings)
    client = TestClient(app)

    assert client.post("/api/agent/sessions").status_code == 404
    assert get_features() == {"agent": False}


def test_agent_routes_mounted_when_enabled(fake_settings: SimpleNamespace):
    app = create_app(settings=fake_settings)
    app.dependency_overrides[get_store] = lambda: None
    client = TestClient(app)

    # 401 (not 404): the router is mounted and the auth guard answers first.
    assert client.post("/api/agent/sessions").status_code == 401
    assert get_features() == {"agent": True}


class TestErrorHandlers:
    """Domain errors become the status the UI can act on, never an opaque 500."""

    @staticmethod
    def _client(raise_error: Exception) -> TestClient:
        """Mount one route that raises, behind the app's handler table.

        Args:
            raise_error: The exception the probe route raises.

        Returns:
            A client for the probe app.
        """
        app = FastAPI()
        for error_type, handler in app_module._ERROR_HANDLERS.items():
            app.add_exception_handler(error_type, handler)

        @app.get("/probe")
        def probe() -> dict[str, str]:
            raise raise_error

        return TestClient(app, raise_server_exceptions=False)

    def test_a_missing_record_is_a_404(self) -> None:
        response = self._client(NotFoundError("Run 1 not found")).get("/probe")

        assert response.status_code == 404
        # NotFoundError subclasses KeyError, so str() wraps the message in
        # quotes — the detail carries them through to the client.
        assert response.json() == {"detail": "'Run 1 not found'"}

    def test_catalog_drift_is_a_409(self) -> None:
        # A drifted component stays broken until the user resolves it, so it
        # is a conflict rather than a server fault.
        response = self._client(ComponentDriftError("source 'fb' drifted")).get("/probe")

        assert response.status_code == 409
        assert response.json() == {"detail": "source 'fb' drifted"}

    def test_an_unreadable_record_is_a_409(self) -> None:
        response = self._client(HydrationError("cannot decrypt payload")).get("/probe")

        assert response.status_code == 409
        assert response.json() == {"detail": "cannot decrypt payload"}

    def test_a_quota_refusal_is_a_429_with_its_numbers(self) -> None:
        error = QuotaExceededError("Too many sources", quota="max_sources", limit=10, used=10)

        response = self._client(error).get("/probe")

        assert response.status_code == 429
        assert response.json() == {
            "detail": {"message": "Too many sources", "quota": "max_sources", "limit": 10, "used": 10}
        }


class TestCreateApp:
    """Wiring ``create_app`` does beyond mounting the routers."""

    def test_cors_origins_are_installed_when_given(self, fake_settings: SimpleNamespace) -> None:
        app = create_app(settings=fake_settings, cors_origins=["http://localhost:3000"])

        assert any("CORSMiddleware" in getattr(m.cls, "__name__", "") for m in app.user_middleware)

    def test_no_cors_middleware_without_origins(self, fake_settings: SimpleNamespace) -> None:
        app = create_app(settings=fake_settings)

        assert not any("CORSMiddleware" in getattr(m.cls, "__name__", "") for m in app.user_middleware)

    def test_a_missing_agent_extra_is_reported_and_survived(
        self, fake_settings: SimpleNamespace, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        # An API-only image is built without the extra on purpose. The
        # submodule is already imported, so the parent package's attribute has
        # to go too — `from X import Y` reads that before sys.modules.
        import interloper_api.routes as routes_package

        monkeypatch.delattr(routes_package, "agent", raising=False)
        monkeypatch.setitem(sys.modules, "interloper_api.routes.agent", None)

        with caplog.at_level("WARNING", logger="interloper_api.app"):
            app = create_app(settings=fake_settings)

        assert "the 'agent' extra is not installed" in caplog.text
        assert TestClient(app).post("/api/agent/sessions").status_code == 404

    def test_the_agent_feature_flag_follows_the_mount(self, fake_settings: SimpleNamespace) -> None:
        from interloper_api.dependencies import get_features

        create_app(settings=fake_settings)

        assert get_features() == {"agent": True}

    def test_state_slots_are_left_alone_when_not_supplied(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # ``create_app()`` with no store or catalog must not clobber whatever
        # a caller installed beforehand.
        from interloper_api.dependencies import state as state_module

        sentinel_store, sentinel_catalog = SimpleNamespace(name="s"), SimpleNamespace(name="c")
        monkeypatch.setattr(state_module, "_store", sentinel_store)
        monkeypatch.setattr(state_module, "_catalog", sentinel_catalog)

        create_app()

        assert state_module._store is sentinel_store
        assert state_module._catalog is sentinel_catalog

    def test_a_supplied_store_and_catalog_are_installed(self, fake_settings: SimpleNamespace) -> None:
        import interloper as il

        from interloper_api.dependencies import get_catalog

        store, catalog = SimpleNamespace(name="s"), il.Catalog(components={})

        create_app(store=store, catalog=catalog, settings=fake_settings)  # ty: ignore[invalid-argument-type]

        assert get_store() is store
        assert get_catalog() is catalog
