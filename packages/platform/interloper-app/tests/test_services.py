"""Tests for ``interloper_app.services``."""

from __future__ import annotations

import os
import socket
import threading
from pathlib import Path
from typing import Any

import interloper_api.server
import pytest
import sqlalchemy
from fastapi import APIRouter
from fastapi.testclient import TestClient
from interloper.catalog import Catalog
from interloper.settings import ServerSettings
from interloper_api import ApiServer
from interloper_api.settings import ApiSettings
from interloper_db import Store
from interloper_scheduler.settings import SchedulerSettings

import interloper_app.services as services_module
from interloper_app import SpaFallback
from interloper_app.services import App, ParentWatchdog


def _free_port() -> int:
    """An OS-assigned port nothing listens on.

    Returns:
        The port.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


@pytest.fixture
def stub_roles(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Build a bare FastAPI app instead of the real API, and named stand-ins for the scheduler.

    Args:
        monkeypatch: Fixture used to swap the collaborators.

    Returns:
        What the API factory and the scheduler builder were handed.
    """
    from fastapi import FastAPI

    recorded: dict[str, Any] = {}

    def create_app(**kwargs: Any) -> FastAPI:
        recorded["create_app"] = kwargs
        return FastAPI()

    def scheduler_services(settings: SchedulerSettings, **kwargs: Any) -> dict[str, object]:
        recorded["scheduler"] = kwargs
        enabled = [name for name in ("cron", "worker", "reaper") if kwargs[name]]
        return {name: object() for name in enabled}

    monkeypatch.setattr(interloper_api.server, "create_app", create_app)
    monkeypatch.setattr(services_module, "scheduler_services", scheduler_services)
    monkeypatch.setattr("atexit.register", lambda *args: None)
    return recorded


def _app(**overrides: Any) -> App:
    """Build an ``App`` over the active settings, with stand-in collaborators.

    Args:
        **overrides: Any ``from_settings`` toggle to change; every service is off unless asked for.

    Returns:
        The composed application.
    """
    toggles: dict[str, Any] = {"api": False, "cron": False, "worker": False, "reaper": False}
    toggles.update(overrides)
    catalog = Catalog()
    store = Store(catalog, engine=sqlalchemy.create_engine("sqlite://"))
    return App.from_settings(ApiSettings.get(), SchedulerSettings.get(), store=store, catalog=catalog, **toggles)


class TestFromSettings:
    """The services composed for the enabled roles."""

    def test_names_every_enabled_service(self, stub_roles: dict[str, Any]) -> None:
        app = _app(api=True, cron=True, worker=True, reaper=True)

        assert set(app.services) == {"api", "cron", "worker", "reaper"}

    def test_the_api_binds_the_resolved_port(self, stub_roles: dict[str, Any]) -> None:
        api = _app(api=True).services["api"]

        assert isinstance(api, ApiServer)
        assert api._server.config.port == ServerSettings.get().port

    def test_an_explicit_port_is_where_the_api_binds(self, stub_roles: dict[str, Any]) -> None:
        api = _app(api=True, port=45678).services["api"]

        assert isinstance(api, ApiServer)
        assert api._server.config.port == 45678

    def test_behind_the_nuxt_dev_server_the_api_skips_the_spa(
        self, stub_roles: dict[str, Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(services_module, "static_dir", lambda: pytest.fail("the built SPA must not be mounted"))

        assert "api" in _app(api=True, spa=False).services

    def test_production_mounts_the_built_spa(
        self, stub_roles: dict[str, Any], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        (tmp_path / "index.html").write_text("<html>spa</html>")
        monkeypatch.setattr(services_module, "static_dir", lambda: tmp_path)

        api = _app(api=True).services["api"]

        assert isinstance(api, ApiServer)
        assert [middleware.cls for middleware in api.app.user_middleware] == [SpaFallback]

    def test_interloper_dev_mounts_its_routes_and_names_its_dev_servers(
        self, stub_roles: dict[str, Any], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        (tmp_path / "index.html").write_text("<html>spa</html>")
        monkeypatch.setattr(services_module, "static_dir", lambda: tmp_path)
        router = APIRouter()
        router.add_api_route("/api/dev/ping", lambda: {"pong": True}, methods=["GET"])

        api = _app(api=True, routers=[router], dev_origins=["http://127.0.0.1:5173"]).services["api"]

        assert isinstance(api, ApiServer)
        assert TestClient(api.app).get("/api/dev/ping").json() == {"pong": True}
        (middleware,) = api.app.user_middleware
        assert middleware.kwargs == {"directory": tmp_path, "dev_origins": ["http://127.0.0.1:5173"]}

    def test_a_missing_build_is_a_warning(
        self, stub_roles: dict[str, Any], monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        def not_built() -> Path:
            raise FileNotFoundError("not built")

        monkeypatch.setattr(services_module, "static_dir", not_built)

        with caplog.at_level("WARNING"):
            app = _app(api=True)

        assert "api" in app.services
        assert "Frontend not available" in caplog.text

    def test_the_scheduler_services_follow_their_toggles(self, stub_roles: dict[str, Any]) -> None:
        _app(cron=True, reaper=True)

        assert (stub_roles["scheduler"]["cron"], stub_roles["scheduler"]["worker"]) == (True, False)
        assert stub_roles["scheduler"]["reaper"] is True

    def test_nothing_enabled_is_refused(self, stub_roles: dict[str, Any]) -> None:
        with pytest.raises(SystemExit, match="No services enabled"):
            _app()


class TestParentWatchdog:
    """The dev-mode orphan watchdog."""

    def test_reparenting_returns(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # `make dev-up` runs us under make/uv; if that chain dies we get
        # reparented and must not run on as an orphan.
        parent_pids = iter([os.getppid(), 1, 1])
        monkeypatch.setattr(os, "getppid", lambda: next(parent_pids))
        watchdog = ParentWatchdog(interval=0.01)

        thread = threading.Thread(target=watchdog.start, daemon=True)
        thread.start()
        thread.join(timeout=5)

        assert not thread.is_alive()

    def test_a_parent_gone_before_the_watchdog_was_built_is_noticed(self) -> None:
        watchdog = ParentWatchdog(interval=0.01, parent=os.getppid() + 100_000)

        thread = threading.Thread(target=watchdog.start, daemon=True)
        thread.start()
        thread.join(timeout=5)

        assert not thread.is_alive()

    def test_a_stable_parent_keeps_running_until_stopped(self) -> None:
        watchdog = ParentWatchdog(interval=0.01)
        thread = threading.Thread(target=watchdog.start, daemon=True)
        thread.start()
        try:
            thread.join(timeout=0.1)
            assert thread.is_alive()
        finally:
            watchdog.stop()
            thread.join(timeout=5)

        assert not thread.is_alive()
