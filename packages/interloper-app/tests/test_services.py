"""Tests for ``interloper_app.services``."""

from __future__ import annotations

import os
import signal
import socket
import subprocess
import threading
import time
from pathlib import Path
from typing import Any

import interloper_api.server
import pytest
from interloper.catalog import Catalog
from interloper.settings import AppSettings
from interloper_api import ApiServer

import interloper_app.services as services_module
from interloper_app import SpaFallback
from interloper_app.services import App, DevServer, ParentWatchdog


def _free_port() -> int:
    """An OS-assigned port nothing listens on.

    Returns:
        The port.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def _with_port(port: int) -> AppSettings:
    """Default settings with ``server.port`` moved.

    Args:
        port: The server port.

    Returns:
        The settings.
    """
    settings = AppSettings()
    return settings.model_copy(update={"server": settings.server.model_copy(update={"port": port})})


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

    def scheduler_services(settings: AppSettings, **kwargs: Any) -> dict[str, object]:
        recorded["scheduler"] = kwargs
        enabled = [name for name in ("cron", "worker", "reaper") if kwargs[name]]
        return {name: object() for name in enabled}

    monkeypatch.setattr(interloper_api.server, "create_app", create_app)
    monkeypatch.setattr(services_module, "scheduler_services", scheduler_services)
    monkeypatch.setattr("atexit.register", lambda *args: None)
    return recorded


def _app(settings: AppSettings | None = None, **overrides: Any) -> App:
    """Build an ``App`` with stand-in collaborators.

    Args:
        settings: The settings; defaults apply when ``None``.
        **overrides: Any ``from_settings`` toggle to change; every service is off unless asked for.

    Returns:
        The composed application.
    """
    toggles: dict[str, Any] = {"api": False, "cron": False, "worker": False, "reaper": False}
    toggles |= {"dev": False, "port": None}
    toggles.update(overrides)
    return App.from_settings(settings or AppSettings(), store=object(), catalog=Catalog(), **toggles)  # ty: ignore[invalid-argument-type]


class TestApiPort:
    """The API moves off the configured port only when Nuxt needs it."""

    def test_production_uses_the_configured_port(self) -> None:
        settings = AppSettings()

        assert App._api_port(settings, dev=False, port=None) == settings.server.port

    def test_dev_mode_picks_a_free_port_for_the_api(self) -> None:
        settings = AppSettings()

        port = App._api_port(settings, dev=True, port=None)

        assert port != settings.server.port
        assert DevServer._port_in_use(port) is False

    def test_an_explicit_port_is_honoured_in_dev_mode(self) -> None:
        # --port already moved server.port; the API must land there rather
        # than on a random one Nuxt would not proxy to.
        assert App._api_port(_with_port(4567), dev=True, port=4567) == 4567


class TestFromSettings:
    """The services composed for the enabled roles."""

    def test_names_every_enabled_service(self, stub_roles: dict[str, Any]) -> None:
        app = _app(api=True, cron=True, worker=True, reaper=True)

        assert set(app.services) == {"api", "cron", "worker", "reaper"}

    def test_dev_mode_adds_the_nuxt_server_and_the_watchdog(self, stub_roles: dict[str, Any]) -> None:
        app = _app(api=True, dev=True)

        assert set(app.services) == {"api", "nuxt-dev", "parent-watchdog"}
        assert isinstance(app.services["nuxt-dev"], DevServer)
        assert isinstance(app.services["parent-watchdog"], ParentWatchdog)

    def test_the_api_binds_the_resolved_port(self, stub_roles: dict[str, Any]) -> None:
        api = _app(api=True).services["api"]

        assert isinstance(api, ApiServer)
        assert api._server.config.port == AppSettings().server.port

    def test_dev_mode_allows_the_nuxt_origin_and_skips_the_spa(
        self, stub_roles: dict[str, Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(services_module, "static_dir", lambda: pytest.fail("dev mode must not mount the built SPA"))
        settings = AppSettings()

        _app(settings, api=True, dev=True)

        assert stub_roles["create_app"]["cors_origins"] == [f"http://localhost:{settings.server.port}"]

    def test_production_mounts_the_built_spa(
        self, stub_roles: dict[str, Any], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        (tmp_path / "index.html").write_text("<html>spa</html>")
        monkeypatch.setattr(services_module, "static_dir", lambda: tmp_path)

        api = _app(api=True).services["api"]

        assert isinstance(api, ApiServer)
        assert [middleware.cls for middleware in api.app.user_middleware] == [SpaFallback]

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

    def test_dev_without_the_api_is_refused(self, stub_roles: dict[str, Any]) -> None:
        with pytest.raises(SystemExit, match="--dev"):
            _app(cron=True, dev=True)

    def test_nothing_enabled_is_refused(self, stub_roles: dict[str, Any]) -> None:
        with pytest.raises(SystemExit, match="No services enabled"):
            _app()


class TestDevServer:
    """The Nuxt dev server, as a supervised service."""

    def test_an_occupied_port_fails_fast(self) -> None:
        # Nuxt would silently fall back to another port, stacking a
        # half-broken instance on top of the running one.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.bind(("127.0.0.1", 0))
            server.listen(1)
            port = server.getsockname()[1]

            with pytest.raises(SystemExit, match=f"Port {port} is already in use"):
                DevServer.check_port(port)

    def test_a_free_port_passes(self) -> None:
        DevServer.check_port(_free_port())

    def test_passes_both_ports_into_the_environment(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        launched: dict[str, Any] = {}

        class FakePopen:
            def __init__(self, command: Any, **kwargs: Any) -> None:
                launched["command"] = command
                launched.update(kwargs)

            def poll(self) -> int | None:
                return 0

            def wait(self) -> int:
                return 0

        monkeypatch.setattr(services_module, "source_dir", lambda: tmp_path)
        monkeypatch.setattr(subprocess, "Popen", FakePopen)
        monkeypatch.setattr("atexit.register", lambda *args: None)

        DevServer(server_port=3000, api_port=5555).start()

        assert launched["command"] == ["pnpm", "dev"]
        assert launched["cwd"] == tmp_path
        assert launched["env"]["INTERLOPER_API_PORT"] == "5555"
        assert launched["env"]["INTERLOPER_SERVER_PORT"] == "3000"
        # One process group so shutdown can kill the whole pnpm → nuxt tree.
        assert launched["start_new_session"] is True

    def test_does_not_start_after_stop(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(subprocess, "Popen", lambda *a, **kw: pytest.fail("Nuxt started after shutdown began"))
        monkeypatch.setattr("atexit.register", lambda *args: None)
        server = DevServer(server_port=3000, api_port=5555)
        server.stop()

        server.start()

        assert server._process is None

    def test_a_race_with_stop_signals_the_new_process(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        signalled: list[int] = []
        monkeypatch.setattr("atexit.register", lambda *args: None)
        server = DevServer(server_port=3000, api_port=5555)

        class RacingPopen:
            def __init__(self, command: Any, **kwargs: Any) -> None:
                # The stop lands between the guard and Popen returning.
                server._stopping.set()

            def poll(self) -> int | None:
                return None

            def wait(self) -> int:
                return -15

        monkeypatch.setattr(services_module, "source_dir", lambda: tmp_path)
        monkeypatch.setattr(subprocess, "Popen", RacingPopen)
        monkeypatch.setattr(
            DevServer, "_kill_process_group", staticmethod(lambda process, number: signalled.append(number))
        )

        server.start()

        assert signalled == [signal.SIGTERM]

    def test_nuxt_dying_first_returns_and_is_logged(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        class DyingPopen:
            def __init__(self, command: Any, **kwargs: Any) -> None:
                pass

            def poll(self) -> int | None:
                return 1

            def wait(self) -> int:
                return 1

        monkeypatch.setattr(services_module, "source_dir", lambda: tmp_path)
        monkeypatch.setattr(subprocess, "Popen", DyingPopen)
        monkeypatch.setattr("atexit.register", lambda *args: None)

        with caplog.at_level("ERROR"):
            DevServer(server_port=3000, api_port=5555).start()

        # Returning on its own is a death: the supervisor stops the process.
        assert "exited unexpectedly" in caplog.text

    def test_a_surviving_group_is_killed_on_exit(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        # SIGTERM-immune stand-in for a pnpm → nuxt tree that outlived the stop.
        monkeypatch.setattr("atexit.register", lambda *args: None)
        process = subprocess.Popen(["/bin/sh", "-c", "trap '' TERM; sleep 30"], start_new_session=True)
        server = DevServer(server_port=3000, api_port=5555)
        server._process = process
        # Mark the stop without signalling: a SIGTERM racing the shell's
        # `trap` would kill the stand-in before it is immune.
        server._stopping.set()

        with caplog.at_level("WARNING"):
            server._kill_survivor()

        assert process.wait(timeout=5) != 0
        assert "still running" in caplog.text

    def test_kill_process_group_kills_children(self) -> None:
        """The group kill reaches grandchildren that terminate() would orphan.

        Raises:
            AssertionError: If the grandchild survives the group kill.
        """
        process = subprocess.Popen(
            ["/bin/sh", "-c", "sleep 30 & echo $!; wait"], stdout=subprocess.PIPE, start_new_session=True
        )
        assert process.stdout is not None
        child_pid = int(process.stdout.readline())

        DevServer._kill_process_group(process, signal.SIGKILL)
        process.wait(timeout=5)

        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                os.kill(child_pid, 0)
            except ProcessLookupError:
                return
            time.sleep(0.05)
        raise AssertionError(f"grandchild {child_pid} still alive after killpg")

    def test_kill_process_group_ignores_an_exited_process(self) -> None:
        process = subprocess.Popen(["/bin/sh", "-c", "exit 0"], start_new_session=True)
        process.wait(timeout=5)

        DevServer._kill_process_group(process, signal.SIGTERM)

    def test_kill_process_group_tolerates_a_vanished_group(self) -> None:
        class VanishedProcess:
            pid = 2**31 - 1

            def poll(self) -> int | None:
                return None

        DevServer._kill_process_group(VanishedProcess(), signal.SIGTERM)  # ty: ignore[invalid-argument-type]

    def test_port_in_use_detects_an_ipv4_listener(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.bind(("127.0.0.1", 0))
            server.listen(1)

            assert DevServer._port_in_use(server.getsockname()[1]) is True

    def test_port_in_use_detects_an_ipv6_only_listener(self) -> None:
        """A listener bound only to ``::1`` (how Nuxt dev binds on macOS) is detected."""
        with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as server:
            server.bind(("::1", 0))
            server.listen(1)

            assert DevServer._port_in_use(server.getsockname()[1]) is True

    def test_port_in_use_free_port(self) -> None:
        assert DevServer._port_in_use(_free_port()) is False

    def test_port_in_use_survives_an_unavailable_stack(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A loopback family the host does not support is skipped, not raised."""

        def unsupported(family: int, kind: int) -> Any:
            raise OSError("address family not supported")

        monkeypatch.setattr(socket, "socket", unsupported)

        assert DevServer._port_in_use(1) is False


class TestParentWatchdog:
    """The dev-mode orphan watchdog."""

    def test_reparenting_returns(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # `make dev-up` runs us under make/uv; if that chain dies we get
        # reparented and must not run on as an orphan.
        parent_pids = iter([os.getppid(), 1, 1])
        monkeypatch.setattr(os, "getppid", lambda: next(parent_pids))
        watchdog = ParentWatchdog()

        thread = threading.Thread(target=watchdog.start, daemon=True)
        thread.start()
        thread.join(timeout=5)

        assert not thread.is_alive()

    def test_a_stable_parent_keeps_running_until_stopped(self) -> None:
        watchdog = ParentWatchdog()
        thread = threading.Thread(target=watchdog.start, daemon=True)
        thread.start()
        try:
            thread.join(timeout=1.5)
            assert thread.is_alive()
        finally:
            watchdog.stop()
            thread.join(timeout=5)

        assert not thread.is_alive()
