"""Tests for ``interloper.cli.runtime``."""

from __future__ import annotations

import argparse
import os
import signal
import socket
import subprocess
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import interloper_api
import interloper_app
import interloper_scheduler
import pytest

from interloper.catalog import Catalog
from interloper.cli.runtime import Services, _find_free_port, apply_cli_overrides, resolve_api_port
from interloper.settings import AppSettings


def test_port_in_use_detects_listener() -> None:
    """A live listener on the port is detected."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        port = server.getsockname()[1]
        assert Services._port_in_use(port) is True


def test_port_in_use_detects_ipv6_only_listener() -> None:
    """A listener bound only to ``::1`` (how Nuxt dev binds on macOS) is detected."""
    with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as server:
        server.bind(("::1", 0))
        server.listen(1)
        port = server.getsockname()[1]
        assert Services._port_in_use(port) is True


def test_port_in_use_free_port() -> None:
    """A free port reports not in use."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    assert Services._port_in_use(port) is False


def test_kill_process_group_kills_children() -> None:
    """The group kill reaches grandchildren that terminate() would orphan.

    Raises:
        AssertionError: If the grandchild survives the group kill.
    """
    # Mimics pnpm → nuxt: a shell whose child would survive a plain terminate().
    proc = subprocess.Popen(
        ["/bin/sh", "-c", "sleep 30 & echo $!; wait"],
        stdout=subprocess.PIPE,
        start_new_session=True,
    )
    assert proc.stdout is not None
    child_pid = int(proc.stdout.readline())

    Services._kill_process_group(proc, signal.SIGKILL)
    proc.wait(timeout=5)

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        try:
            os.kill(child_pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.05)
    raise AssertionError(f"grandchild {child_pid} still alive after killpg")


def test_kill_process_group_ignores_exited_process() -> None:
    """Killing an already-exited process is a no-op."""
    proc = subprocess.Popen(["/bin/sh", "-c", "exit 0"], start_new_session=True)
    proc.wait(timeout=5)
    Services._kill_process_group(proc, signal.SIGTERM)


def test_kill_process_group_tolerates_a_vanished_group() -> None:
    """A group that exits between the poll and the killpg does not raise."""

    class VanishedProcess:
        pid = 2**31 - 1

        def poll(self) -> int | None:
            return None

    Services._kill_process_group(VanishedProcess(), signal.SIGTERM)  # ty: ignore[invalid-argument-type]


# -- CLI overrides ------------------------------------------------------------


def _app_args(**overrides: Any) -> argparse.Namespace:
    base: dict[str, Any] = {
        "host": None,
        "port": None,
        "api": None,
        "cron": None,
        "worker": None,
        "reaper": None,
    }
    base.update(overrides)
    return argparse.Namespace(**base)


class TestApplyCliOverrides:
    """``interloper app`` flags fold into the loaded settings."""

    def test_omitted_flags_leave_settings_untouched(self) -> None:
        settings = AppSettings()

        result = apply_cli_overrides(_app_args(), settings)

        assert result.server.host == settings.server.host
        assert result.server.port == settings.server.port
        assert result.server.enabled == settings.server.enabled
        assert result.cron.enabled == settings.cron.enabled
        assert result.worker.enabled == settings.worker.enabled
        assert result.reaper.enabled == settings.reaper.enabled

    def test_host_and_port_override_the_server_block(self) -> None:
        result = apply_cli_overrides(_app_args(host="0.0.0.0", port=9999), AppSettings())

        assert (result.server.host, result.server.port) == ("0.0.0.0", 9999)

    @pytest.mark.parametrize("enabled", [True, False])
    def test_every_service_toggle_is_applied(self, enabled: bool) -> None:
        result = apply_cli_overrides(
            _app_args(api=enabled, cron=enabled, worker=enabled, reaper=enabled), AppSettings()
        )

        assert result.server.enabled is enabled
        assert result.cron.enabled is enabled
        assert result.worker.enabled is enabled
        assert result.reaper.enabled is enabled

    def test_the_original_settings_are_not_mutated(self) -> None:
        settings = AppSettings()
        original_port = settings.server.port

        apply_cli_overrides(_app_args(port=original_port + 1), settings)

        assert settings.server.port == original_port


# -- API port resolution ------------------------------------------------------


class TestResolveApiPort:
    """The API moves off the configured port only when Nuxt needs it."""

    def test_production_uses_the_configured_port(self) -> None:
        settings = AppSettings()

        port = resolve_api_port(settings=settings, run_api=True, dev_mode=False, explicit_port_arg=None)

        assert port == settings.server.port

    def test_dev_mode_picks_a_free_port_for_the_api(self) -> None:
        settings = AppSettings()

        port = resolve_api_port(settings=settings, run_api=True, dev_mode=True, explicit_port_arg=None)

        assert port != settings.server.port
        assert Services._port_in_use(port) is False

    def test_an_explicit_port_is_honoured_in_dev_mode(self) -> None:
        # --port already moved settings.server.port; the API must land there
        # rather than on a random one Nuxt would not proxy to.
        settings = apply_cli_overrides(_app_args(port=4567), AppSettings())

        port = resolve_api_port(settings=settings, run_api=True, dev_mode=True, explicit_port_arg=4567)

        assert port == 4567

    def test_without_the_api_the_configured_port_stands(self) -> None:
        settings = AppSettings()

        port = resolve_api_port(settings=settings, run_api=False, dev_mode=True, explicit_port_arg=None)

        assert port == settings.server.port


def test_find_free_port_returns_an_unused_port() -> None:
    """The OS-assigned port is genuinely free."""
    port = _find_free_port()

    assert 1024 < port <= 65535
    assert Services._port_in_use(port) is False


# -- Services -----------------------------------------------------------------


class FakeController:
    """Controller that blocks in ``start`` until ``stop`` releases it."""

    def __init__(self, **kwargs: Any) -> None:
        """Record the construction kwargs and arm the release latch.

        Args:
            kwargs: Whatever the runtime passed to the real controller.
        """
        self.kwargs = kwargs
        self.started = threading.Event()
        self.stopped = threading.Event()

    def start(self) -> None:
        """Block until stopped, mimicking a controller's poll loop."""
        self.started.set()
        self.stopped.wait(timeout=10)

    def stop(self) -> None:
        """Release the ``start`` loop."""
        self.stopped.set()


@pytest.fixture
def restore_signal_handlers() -> Iterator[None]:
    """Put pytest's own signal handlers back after ``_install_signal_handlers``.

    Yields:
        ``None``; the teardown restores the saved handlers.
    """
    names = ["SIGINT", "SIGTERM", "SIGHUP"]
    saved = {name: signal.getsignal(getattr(signal, name)) for name in names if hasattr(signal, name)}
    yield
    for name, handler in saved.items():
        signal.signal(getattr(signal, name), handler)


def _services(**overrides: Any) -> Services:
    """Build a ``Services`` with stub collaborators.

    Args:
        overrides: Any constructor argument to replace.

    Returns:
        The configured instance; no service is enabled unless asked for.
    """
    settings = overrides.pop("settings", None) or AppSettings()
    kwargs: dict[str, Any] = {
        "settings": settings,
        "store": object(),
        "catalog": Catalog(),
        "run_api": False,
        "run_cron": False,
        "run_worker": False,
        "run_reaper": False,
        "dev_mode": False,
        "api_port": settings.server.port,
    }
    kwargs.update(overrides)
    return Services(**kwargs)


class TestBuildApi:
    """The uvicorn server, and the SPA it serves outside dev mode."""

    def test_binds_the_resolved_api_port(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from fastapi import FastAPI

        monkeypatch.setattr(interloper_api, "create_app", lambda **kwargs: FastAPI())
        services = _services(run_api=True, api_port=4711)

        services._build_api()

        assert services._api_server.config.port == 4711
        assert services._api_server.config.host == services.settings.server.host

    def test_dev_mode_allows_the_nuxt_origin_and_skips_the_spa(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from fastapi import FastAPI

        seen: dict[str, Any] = {}

        def create_app(**kwargs: Any) -> FastAPI:
            seen.update(kwargs)
            return FastAPI()

        monkeypatch.setattr(interloper_api, "create_app", create_app)
        monkeypatch.setattr(
            interloper_app, "static_dir", lambda: pytest.fail("dev mode must not mount the built SPA")
        )
        services = _services(run_api=True, dev_mode=True)

        services._build_api()

        assert seen["cors_origins"] == [f"http://localhost:{services.settings.server.port}"]

    def test_production_mounts_the_built_spa(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        from fastapi import FastAPI

        (tmp_path / "index.html").write_text("<html>spa</html>")
        monkeypatch.setattr(interloper_api, "create_app", lambda **kwargs: FastAPI())
        monkeypatch.setattr(interloper_app, "static_dir", lambda: tmp_path)
        services = _services(run_api=True)

        services._build_api()

        middleware = services._api_server.config.app.user_middleware
        assert [m.cls.__name__ for m in middleware] == ["SPAFallbackMiddleware"]

    def test_an_api_only_image_without_the_frontend_still_builds(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        from fastapi import FastAPI

        # API-only images are built without interloper_app on purpose.
        monkeypatch.setattr(interloper_api, "create_app", lambda **kwargs: FastAPI())
        monkeypatch.setattr(
            interloper_app, "static_dir", lambda: (_ for _ in ()).throw(FileNotFoundError("not built"))
        )
        services = _services(run_api=True)

        with caplog.at_level("WARNING"):
            services._build_api()

        assert services._api_server is not None
        assert "Frontend not available" in caplog.text


class TestBuildCron:
    """Cron rides with the hook and renewal controllers."""

    def test_builds_cron_hooks_and_renewal(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(interloper_scheduler, "CronController", FakeController)
        monkeypatch.setattr(interloper_scheduler, "HookController", FakeController)
        monkeypatch.setattr(interloper_scheduler, "RenewalController", FakeController)
        services = _services(run_cron=True)

        services._build_cron()

        assert services._cron_controller.kwargs["batch_size"] == services.settings.cron.batch_size
        assert services._hook_controller is not None
        assert services._renewal_controller is not None

    def test_renewal_is_skipped_when_disabled(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(interloper_scheduler, "CronController", FakeController)
        monkeypatch.setattr(interloper_scheduler, "HookController", FakeController)
        settings = AppSettings()
        settings = settings.model_copy(update={"renewal": settings.renewal.model_copy(update={"enabled": False})})
        services = _services(run_cron=True, settings=settings)

        services._build_cron()

        assert services._renewal_controller is None


class TestBuildLauncherServices:
    """The worker and reaper share one launcher."""

    @pytest.fixture(autouse=True)
    def _fake_launcher(self, monkeypatch: pytest.MonkeyPatch) -> Any:
        launcher = object()
        monkeypatch.setattr(
            interloper_scheduler.Launcher, "from_settings", classmethod(lambda cls, settings, **kw: launcher)
        )
        monkeypatch.setattr(interloper_scheduler, "QueueController", FakeController)
        monkeypatch.setattr(interloper_scheduler, "Reaper", FakeController)
        return launcher

    def test_worker_only(self) -> None:
        services = _services(run_worker=True)

        services._build_launcher_services()

        assert services._queue_controller is not None
        assert services._reaper is None

    def test_reaper_only(self) -> None:
        services = _services(run_reaper=True)

        services._build_launcher_services()

        assert services._queue_controller is None
        assert services._reaper is not None

    def test_both_share_the_same_launcher(self, _fake_launcher: Any) -> None:
        services = _services(run_worker=True, run_reaper=True)

        services._build_launcher_services()

        assert services._queue_controller.kwargs["launcher"] is _fake_launcher
        assert services._reaper.kwargs["launcher"] is _fake_launcher


class TestPrepareNuxt:
    """Dev-mode preflight on the Nuxt port."""

    def test_an_occupied_port_fails_fast(self) -> None:
        # Nuxt would silently fall back to another port, stacking a
        # half-broken instance on top of the running one.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.bind(("127.0.0.1", 0))
            server.listen(1)
            port = server.getsockname()[1]
            settings = AppSettings()
            settings = settings.model_copy(update={"server": settings.server.model_copy(update={"port": port})})

            with pytest.raises(SystemExit, match=f"Port {port} is already in use"):
                _services(dev_mode=True, settings=settings)._prepare_nuxt()

    def test_a_free_port_arms_the_last_resort_kill(self, monkeypatch: pytest.MonkeyPatch) -> None:
        registered: list[Any] = []
        monkeypatch.setattr("atexit.register", lambda fn, *a: registered.append((fn, a)))
        settings = AppSettings()
        settings = settings.model_copy(
            update={"server": settings.server.model_copy(update={"port": _find_free_port()})}
        )
        services = _services(dev_mode=True, settings=settings)

        services._prepare_nuxt()

        assert registered == [(services._signal_nuxt, (signal.SIGKILL,))]


class TestStartThreads:
    """One daemon thread per built service."""

    def test_names_every_enabled_service(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from fastapi import FastAPI

        monkeypatch.setattr(interloper_api, "create_app", lambda **kwargs: FastAPI())
        monkeypatch.setattr(interloper_scheduler, "CronController", FakeController)
        monkeypatch.setattr(interloper_scheduler, "HookController", FakeController)
        monkeypatch.setattr(interloper_scheduler, "RenewalController", FakeController)
        monkeypatch.setattr(
            interloper_scheduler.Launcher, "from_settings", classmethod(lambda cls, settings, **kw: object())
        )
        monkeypatch.setattr(interloper_scheduler, "QueueController", FakeController)
        monkeypatch.setattr(interloper_scheduler, "Reaper", FakeController)
        services = _services(run_api=True, run_cron=True, run_worker=True, run_reaper=True)
        services._build_api()
        services._build_cron()
        services._build_launcher_services()
        # Never actually serve: only the thread wiring is under test.
        services._api_server.run = lambda: None

        services._start_threads()
        try:
            assert {t.name for t in services._threads} == {"cron", "hooks", "renewal", "worker", "reaper", "api"}
            assert all(t.daemon for t in services._threads)
        finally:
            services._shutdown("test teardown")
            for thread in services._threads:
                thread.join(timeout=5)

    def test_dev_mode_adds_the_nuxt_and_watchdog_threads(self, monkeypatch: pytest.MonkeyPatch) -> None:
        services = _services(dev_mode=True)
        services._stop_event.set()  # keep both dev threads from doing any work

        services._start_threads()

        assert {t.name for t in services._threads} == {"nuxt-dev", "parent-watchdog"}
        for thread in services._threads:
            thread.join(timeout=5)

    def test_nothing_enabled_starts_no_threads(self) -> None:
        services = _services()

        services._start_threads()

        assert services._threads == []


class TestRun:
    """The blocking run loop and its shutdown path."""

    def test_returns_once_the_stop_event_is_set(self, restore_signal_handlers: None) -> None:
        services = _services()
        services._stop_event.set()

        services.run()

    def test_blocks_until_a_concurrent_shutdown_lands(self, restore_signal_handlers: None) -> None:
        services = _services()
        threading.Timer(0.05, services._shutdown, args=("test",)).start()

        services.run()

        assert services._stop_event.is_set()

    def test_builds_only_the_enabled_services(
        self, monkeypatch: pytest.MonkeyPatch, restore_signal_handlers: None
    ) -> None:
        built: list[str] = []
        for name in ("_build_api", "_build_cron", "_build_launcher_services", "_prepare_nuxt"):
            monkeypatch.setattr(Services, name, lambda self, _name=name: built.append(_name))
        services = _services(run_cron=True, run_reaper=True)
        services._stop_event.set()

        services.run()

        assert built == ["_build_cron", "_build_launcher_services"]

    def test_dev_mode_builds_every_service_and_the_nuxt_preflight(
        self, monkeypatch: pytest.MonkeyPatch, restore_signal_handlers: None
    ) -> None:
        built: list[str] = []
        for name in ("_build_api", "_build_cron", "_build_launcher_services", "_prepare_nuxt"):
            monkeypatch.setattr(Services, name, lambda self, _name=name: built.append(_name))
        services = _services(run_api=True, run_cron=True, run_worker=True, dev_mode=True)
        services._stop_event.set()

        services.run()

        assert built == ["_build_api", "_build_cron", "_build_launcher_services", "_prepare_nuxt"]

    def test_shutdown_stops_every_built_service(self, monkeypatch: pytest.MonkeyPatch) -> None:
        services = _services()
        cron, hooks, renewal, worker, reaper = (FakeController() for _ in range(5))
        services._cron_controller = cron
        services._hook_controller = hooks
        services._renewal_controller = renewal
        services._queue_controller = worker
        services._reaper = reaper

        class FakeApiServer:
            should_exit = False

        services._api_server = FakeApiServer()

        services._shutdown("test")

        assert all(c.stopped.is_set() for c in (cron, hooks, renewal, worker, reaper))
        assert services._api_server.should_exit is True
        assert services._stop_event.is_set()

    def test_shutdown_is_idempotent(self) -> None:
        services = _services()
        cron = FakeController()
        services._shutdown("first")
        services._cron_controller = cron

        services._shutdown("second")

        # The second call is ignored, so the newly attached controller is
        # never asked to stop.
        assert cron.stopped.is_set() is False

    def test_a_signal_names_itself_as_the_shutdown_reason(self, caplog: pytest.LogCaptureFixture) -> None:
        services = _services()

        with caplog.at_level("INFO"):
            services._on_signal(signal.SIGTERM, None)

        assert "signal SIGTERM" in caplog.text
        assert services._stop_event.is_set()

    def test_installs_handlers_for_every_shutdown_signal(self, restore_signal_handlers: None) -> None:
        services = _services()

        services._install_signal_handlers()

        assert signal.getsignal(signal.SIGINT) == services._on_signal
        assert signal.getsignal(signal.SIGTERM) == services._on_signal
        assert signal.getsignal(signal.SIGHUP) == services._on_signal

    def test_a_surviving_nuxt_group_is_killed_after_the_grace_period(
        self, restore_signal_handlers: None, caplog: pytest.LogCaptureFixture
    ) -> None:
        # SIGTERM-immune stand-in for a pnpm → nuxt tree that outlived shutdown.
        proc = subprocess.Popen(
            ["/bin/sh", "-c", "trap '' TERM; sleep 30"],
            start_new_session=True,
        )
        services = _services()
        services._nuxt_process = proc
        services._stop_event.set()

        with caplog.at_level("WARNING"):
            services.run()

        assert proc.wait(timeout=5) != 0
        assert "still running after grace period" in caplog.text

    def test_signalling_nuxt_before_it_starts_is_a_no_op(self) -> None:
        _services()._signal_nuxt(signal.SIGKILL)


class TestRunNuxtDev:
    """The Nuxt dev-server thread."""

    def test_passes_both_ports_into_the_environment(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        launched: dict[str, Any] = {}

        class FakePopen:
            def __init__(self, cmd: Any, **kwargs: Any) -> None:
                launched["cmd"] = cmd
                launched.update(kwargs)

            def poll(self) -> int | None:
                return 0

            def wait(self) -> int:
                return 0

        monkeypatch.setattr(interloper_app, "source_dir", lambda: tmp_path)
        monkeypatch.setattr(subprocess, "Popen", FakePopen)
        services = _services(dev_mode=True, api_port=5555)

        services._run_nuxt_dev()

        assert launched["cmd"] == ["pnpm", "dev"]
        assert launched["cwd"] == tmp_path
        assert launched["env"]["INTERLOPER_API_PORT"] == "5555"
        assert launched["env"]["INTERLOPER_SERVER_PORT"] == str(services.settings.server.port)
        # One process group so shutdown can kill the whole pnpm → nuxt tree.
        assert launched["start_new_session"] is True

    def test_does_not_start_after_shutdown(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            subprocess, "Popen", lambda *a, **kw: pytest.fail("Nuxt started after shutdown began")
        )
        services = _services(dev_mode=True)
        services._stop_event.set()

        services._run_nuxt_dev()

        assert services._nuxt_process is None

    def test_a_race_with_shutdown_signals_the_new_process(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        signalled: list[int] = []
        services = _services(dev_mode=True)

        class RacingPopen:
            def __init__(self, cmd: Any, **kwargs: Any) -> None:
                # Shutdown lands between the guard above and Popen returning.
                services._stop_event.set()

            def poll(self) -> int | None:
                return None

            def wait(self) -> int:
                return -15

        monkeypatch.setattr(interloper_app, "source_dir", lambda: tmp_path)
        monkeypatch.setattr(subprocess, "Popen", RacingPopen)
        monkeypatch.setattr(Services, "_kill_process_group", staticmethod(lambda p, s: signalled.append(s)))

        services._run_nuxt_dev()

        assert signalled == [signal.SIGTERM]

    def test_nuxt_dying_first_shuts_the_whole_process_down(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        class DyingPopen:
            def __init__(self, cmd: Any, **kwargs: Any) -> None:
                pass

            def poll(self) -> int | None:
                return 1

            def wait(self) -> int:
                return 1

        monkeypatch.setattr(interloper_app, "source_dir", lambda: tmp_path)
        monkeypatch.setattr(subprocess, "Popen", DyingPopen)
        services = _services(dev_mode=True)

        with caplog.at_level("ERROR"):
            services._run_nuxt_dev()

        assert "exited unexpectedly" in caplog.text
        assert services._stop_event.is_set()


class TestWatchParent:
    """The dev-mode orphan watchdog."""

    def test_reparenting_triggers_shutdown(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # `make dev-up` runs us under make/uv; if that chain dies we get
        # reparented and must not run on as an orphan.
        parent_pids = iter([os.getppid(), 1, 1])
        monkeypatch.setattr(os, "getppid", lambda: next(parent_pids))
        services = _services(dev_mode=True)

        thread = threading.Thread(target=services._watch_parent, daemon=True)
        thread.start()

        assert services._stop_event.wait(timeout=5) is True
        thread.join(timeout=5)

    def test_a_stable_parent_keeps_the_process_alive(self) -> None:
        services = _services(dev_mode=True)

        thread = threading.Thread(target=services._watch_parent, daemon=True)
        thread.start()
        try:
            assert services._stop_event.wait(timeout=1.5) is False
        finally:
            services._shutdown("test teardown")
            thread.join(timeout=5)


class TestMountSpa:
    """The SPA fallback only takes over requests FastAPI did not match."""

    @pytest.fixture
    def client(self, tmp_path: Path) -> Any:
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        (tmp_path / "index.html").write_text("<html>spa</html>")
        (tmp_path / "favicon.ico").write_bytes(b"icon-bytes")

        app = FastAPI()

        @app.get("/api/health")
        def health() -> dict[str, str]:
            return {"status": "ok"}

        Services._mount_spa(app, tmp_path)
        return TestClient(app)

    def test_api_routes_are_served_by_fastapi(self, client: Any) -> None:
        response = client.get("/api/health")

        assert response.json() == {"status": "ok"}

    def test_unmatched_api_routes_keep_their_404(self, client: Any) -> None:
        # Falling back to index.html here would turn a bad API call into a
        # 200 page and hide the error from the frontend.
        assert client.get("/api/does-not-exist").status_code == 404

    def test_existing_files_are_served_from_disk(self, client: Any) -> None:
        response = client.get("/favicon.ico")

        assert response.content == b"icon-bytes"

    def test_client_side_routes_fall_back_to_index(self, client: Any) -> None:
        response = client.get("/organisations/1/jobs")

        assert response.status_code == 200
        assert "spa" in response.text

    def test_the_root_serves_index(self, client: Any) -> None:
        assert "spa" in client.get("/").text


def test_port_in_use_survives_an_unavailable_stack(monkeypatch: pytest.MonkeyPatch) -> None:
    """A loopback family the host does not support is skipped, not raised."""

    def unsupported(family: int, kind: int) -> Any:
        raise OSError("address family not supported")

    monkeypatch.setattr(socket, "socket", unsupported)

    assert Services._port_in_use(1) is False

    def test_websocket_traffic_is_left_to_the_application(self, tmp_path: Path) -> None:
        from fastapi import FastAPI

        (tmp_path / "index.html").write_text("<html>spa</html>")
        app = FastAPI()
        Services._mount_spa(app, tmp_path)
        middleware_cls = app.user_middleware[0].cls
        delegated: list[str] = []

        async def wrapped(scope: dict[str, Any], receive: Any, send: Any) -> None:
            delegated.append(scope["type"])

        import asyncio

        asyncio.run(
            middleware_cls(wrapped)({"type": "websocket", "path": "/events"}, None, None)  # ty: ignore[invalid-argument-type]
        )

        assert delegated == ["websocket"]
