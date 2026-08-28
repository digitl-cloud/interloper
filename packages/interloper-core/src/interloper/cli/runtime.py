"""Runtime for ``interloper app``: CLI overrides, port selection, and service orchestration."""

from __future__ import annotations

import argparse
import atexit
import logging
import os
import signal
import socket
import subprocess
import threading
from typing import TYPE_CHECKING, Any

from interloper.telemetry import instrument_fastapi

if TYPE_CHECKING:
    from interloper_db import Store

    from interloper.catalog.base import Catalog
    from interloper.settings import AppSettings

logger = logging.getLogger(__name__)


def apply_cli_overrides(
    args: argparse.Namespace,
    settings: AppSettings,
) -> AppSettings:
    """Apply CLI-provided overrides into settings.

    Args:
        args: Parsed CLI arguments.
        settings: Loaded AppSettings instance.

    Returns:
        New AppSettings with CLI overrides applied.
    """
    server = settings.server
    if getattr(args, "host", None) is not None:
        server = server.model_copy(update={"host": args.host})
    if getattr(args, "port", None) is not None:
        server = server.model_copy(update={"port": args.port})
    if getattr(args, "api", None) is not None:
        server = server.model_copy(update={"enabled": bool(args.api)})

    cron = settings.cron
    if getattr(args, "cron", None) is not None:
        cron = cron.model_copy(update={"enabled": bool(args.cron)})

    worker = settings.worker
    if getattr(args, "worker", None) is not None:
        worker = worker.model_copy(update={"enabled": bool(args.worker)})

    reaper = settings.reaper
    if getattr(args, "reaper", None) is not None:
        reaper = reaper.model_copy(update={"enabled": bool(args.reaper)})

    return settings.model_copy(
        update={
            "server": server,
            "cron": cron,
            "worker": worker,
            "reaper": reaper,
        }
    )


def resolve_api_port(
    *,
    settings: AppSettings,
    run_api: bool,
    dev_mode: bool,
    explicit_port_arg: int | None,
) -> int:
    """Resolve effective API port for service startup.

    In dev mode, when no explicit ``--port`` is provided, the API binds to a
    random free port to avoid clashing with Nuxt on ``settings.server.port``.

    Returns:
        Effective API port for the API server.
    """
    if run_api and dev_mode and explicit_port_arg is None:
        return _find_free_port()
    return settings.server.port


def _find_free_port() -> int:
    """Bind to port 0 and let the OS assign a free port.

    Returns:
        An available TCP port number.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("", 0))
        return sock.getsockname()[1]


class Services:
    """Builds the enabled services, runs them in threads, and blocks until shutdown.

    The four runtime services (api, cron, worker, reaper) toggle
    independently so each role can run in its own pod. Cron and reaper
    are singletons (one per cluster); api and worker scale horizontally.
    In dev mode a Nuxt dev server and a parent-death watchdog run alongside.

    Args:
        store: The Store instance.
        catalog: Catalog instance.
        settings: Loaded AppSettings with CLI overrides applied.
        run_api: Whether to start the API.
        run_cron: Whether to start the cron controller.
        run_worker: Whether to start the queue worker.
        run_reaper: Whether to start the reaper (timed-out run cleanup).
        dev_mode: Whether to run Nuxt in development mode.
        api_port: Effective API port to bind.
    """

    def __init__(
        self,
        *,
        settings: AppSettings,
        store: Store,
        catalog: Catalog,
        run_api: bool,
        run_cron: bool,
        run_worker: bool,
        run_reaper: bool,
        dev_mode: bool,
        api_port: int,
    ) -> None:
        """Store the configuration; services are built when :meth:`run` starts."""
        self.settings = settings
        self.store = store
        self.catalog = catalog
        self.run_api = run_api
        self.run_cron = run_cron
        self.run_worker = run_worker
        self.run_reaper = run_reaper
        self.dev_mode = dev_mode
        self.api_port = api_port

        self._stop_event = threading.Event()
        self._threads: list[threading.Thread] = []
        self._api_server: Any = None
        self._cron_controller: Any = None
        self._hook_controller: Any = None
        self._queue_controller: Any = None
        self._reaper: Any = None
        self._nuxt_process: subprocess.Popen[bytes] | None = None

    def run(self) -> None:
        """Build the enabled services, start them in threads, and block until shutdown."""
        if self.run_api:
            self._build_api()
        if self.run_cron:
            self._build_cron()
        if self.run_worker or self.run_reaper:
            self._build_launcher_services()
        if self.dev_mode:
            self._prepare_nuxt()
        self._install_signal_handlers()
        self._start_threads()

        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=1)

        for thread in self._threads:
            thread.join(timeout=5)

        # Nuxt got SIGTERM when shutdown started; if its group survived the grace
        # period, kill it so nothing outlives this process.
        if self._nuxt_process is not None and self._nuxt_process.poll() is None:
            logger.warning("Nuxt dev server still running after grace period; killing its process group.")
            self._signal_nuxt(signal.SIGKILL)

        logger.info("Shutdown complete.")

    # -- Service construction ----------------------------------------------------

    def _build_api(self) -> None:
        import uvicorn
        from interloper_api import create_app

        cors_origins = [f"http://localhost:{self.settings.server.port}"] if self.dev_mode else None

        app = create_app(
            store=self.store,
            catalog=self.catalog,
            settings=self.settings,
            cors_origins=cors_origins,
        )
        instrument_fastapi(app)

        # In production mode, serve the built SPA as a fallback for non-API routes.
        # The interloper_app package is optional: API-only images (built without it)
        # raise ImportError here, which is fine — they just don't serve the SPA.
        if not self.dev_mode:
            try:
                from interloper_app import static_dir

                self._mount_spa(app, static_dir())
                logger.info("Serving frontend from %s", static_dir())
            except (FileNotFoundError, ImportError) as e:
                logger.warning("Frontend not available: %s", e)

        uvi_config = uvicorn.Config(
            app,
            host=self.settings.server.host,
            port=self.api_port,
            log_level="info",
            log_config=None,
            ws="wsproto",
        )
        self._api_server = uvicorn.Server(uvi_config)

    def _build_cron(self) -> None:
        # The hook evaluator rides the cron service: both are cluster singletons
        # that turn declarative component intent into runs/side effects.
        from interloper_scheduler import CronController, HookController

        self._cron_controller = CronController(
            store=self.store,
            reconcile_interval=self.settings.cron.reconcile_interval,
            max_execution_delay=self.settings.cron.max_execution_delay,
            batch_size=self.settings.cron.batch_size,
        )
        self._hook_controller = HookController(store=self.store)

    def _build_launcher_services(self) -> None:
        # Both need a Launcher; build it once if either is enabled.
        from interloper_scheduler import Launcher

        launcher = Launcher.from_settings(
            self.settings.launcher,
            postgres=self.settings.postgres,
            runner=self.settings.runner,
            catalog=self.catalog,
            store=self.store,
        )

        if self.run_worker:
            from interloper_scheduler import QueueController

            self._queue_controller = QueueController(
                launcher=launcher,
                store=self.store,
                poll_interval=self.settings.worker.poll_interval,
            )

        if self.run_reaper:
            from interloper_scheduler import Reaper

            self._reaper = Reaper(
                store=self.store,
                launcher=launcher,
                timeout=self.settings.reaper.timeout,
                poll_interval=self.settings.reaper.poll_interval,
            )

    def _prepare_nuxt(self) -> None:
        """Fail fast on an occupied dev port and arm the last-resort Nuxt kill.

        Raises:
            SystemExit: When the server port is already in use.
        """
        # Fail fast if the port is taken: Nuxt would silently fall back to
        # :3001, stacking a half-broken instance (OAuth and CORS are pinned to
        # settings.server.port) on top of the one already running.
        if self._port_in_use(self.settings.server.port):
            raise SystemExit(
                f"Port {self.settings.server.port} is already in use — a previous dev instance is likely still "
                f"running (check `lsof -nP -iTCP:{self.settings.server.port} -sTCP:LISTEN`). "
                "Stop it, or set INTERLOPER_SERVER_PORT to a free port."
            )

        # Last resort if run() unwinds via an unhandled exception.
        atexit.register(self._signal_nuxt, signal.SIGKILL)

    # -- Threads & lifecycle -------------------------------------------------------

    def _start_threads(self) -> None:
        if self._cron_controller:
            self._threads.append(threading.Thread(target=self._cron_controller.start, name="cron", daemon=True))
        if self._hook_controller:
            self._threads.append(threading.Thread(target=self._hook_controller.start, name="hooks", daemon=True))
        if self._queue_controller:
            self._threads.append(threading.Thread(target=self._queue_controller.start, name="worker", daemon=True))
        if self._reaper:
            self._threads.append(threading.Thread(target=self._reaper.start, name="reaper", daemon=True))
        if self._api_server:
            self._threads.append(threading.Thread(target=self._api_server.run, name="api", daemon=True))
        if self.dev_mode:
            self._threads.append(threading.Thread(target=self._run_nuxt_dev, name="nuxt-dev", daemon=True))
            self._threads.append(threading.Thread(target=self._watch_parent, name="parent-watchdog", daemon=True))

        for thread in self._threads:
            thread.start()

        services = ", ".join(thread.name or "?" for thread in self._threads)
        logger.info("Started services: %s", services)

    def _run_nuxt_dev(self) -> None:
        from interloper_app import source_dir

        if self._stop_event.is_set():
            return
        logger.info("Starting Nuxt dev server from %s", source_dir())
        # New session: the whole pnpm → nuxt → dev-worker tree lands in one
        # process group that shutdown can kill atomically; terminate()-ing
        # just pnpm leaves the rest of the tree orphaned on the port.
        self._nuxt_process = subprocess.Popen(
            ["pnpm", "dev"],
            cwd=source_dir(),
            env={
                **os.environ,
                "INTERLOPER_SERVER_PORT": str(self.settings.server.port),
                "INTERLOPER_API_PORT": str(self.api_port),
            },
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        )
        if self._stop_event.is_set():
            # Shutdown ran between the check above and Popen returning.
            self._signal_nuxt(signal.SIGTERM)
        returncode = self._nuxt_process.wait()
        if not self._stop_event.is_set():
            logger.error("Nuxt dev server exited unexpectedly (code %s).", returncode)
            self._shutdown("nuxt dev server died")

    def _watch_parent(self) -> None:
        # `make dev-up` runs us under make/uv. If that chain dies without
        # signaling us (terminal killed, agent session reaped), we get
        # reparented — shut down instead of running on as an orphan.
        parent_pid = os.getppid()
        while not self._stop_event.wait(1.0):
            if os.getppid() != parent_pid:
                self._shutdown("parent process died")
                return

    # -- Shutdown --------------------------------------------------------------

    def _install_signal_handlers(self) -> None:
        signal.signal(signal.SIGINT, self._on_signal)
        signal.signal(signal.SIGTERM, self._on_signal)
        if hasattr(signal, "SIGHUP"):
            # Terminal/session closed — without this the default handler kills us
            # before shutdown runs and the Nuxt tree is orphaned.
            signal.signal(signal.SIGHUP, self._on_signal)

    def _on_signal(self, signal_number: int, frame: object) -> None:
        self._shutdown(f"signal {signal.Signals(signal_number).name}")

    def _shutdown(self, reason: str) -> None:
        if self._stop_event.is_set():
            return
        logger.info("Shutting down (%s)...", reason)
        self._stop_event.set()
        self._signal_nuxt(signal.SIGTERM)
        if self._cron_controller:
            self._cron_controller.stop()
        if self._hook_controller:
            self._hook_controller.stop()
        if self._queue_controller:
            self._queue_controller.stop()
        if self._reaper:
            self._reaper.stop()
        if self._api_server:
            self._api_server.should_exit = True

    def _signal_nuxt(self, signal_number: int) -> None:
        if self._nuxt_process is not None:
            self._kill_process_group(self._nuxt_process, signal_number)

    # -- Internals ---------------------------------------------------------------

    @staticmethod
    def _kill_process_group(process: subprocess.Popen[bytes], signal_number: int) -> None:
        """Send a signal to a child's entire process group.

        The child must have been started with ``start_new_session=True`` so its
        pid is also its process-group id.
        """
        if process.poll() is not None:
            return
        try:
            os.killpg(process.pid, signal_number)
        except ProcessLookupError:
            pass

    @staticmethod
    def _port_in_use(port: int) -> bool:
        """Check whether something is already listening on localhost:``port``.

        Probes both loopback stacks: Nuxt dev binds only ``::1`` on macOS, so an
        IPv4-only probe misses it — and Nuxt's own port check doesn't, sending it
        to its silent fall-back-to-3000 path instead of our fail-fast.

        Returns:
            True if a connection to the port succeeds on either loopback.
        """
        for family, addr in ((socket.AF_INET, "127.0.0.1"), (socket.AF_INET6, "::1")):
            try:
                with socket.socket(family, socket.SOCK_STREAM) as sock:
                    sock.settimeout(0.5)
                    if sock.connect_ex((addr, port)) == 0:
                        return True
            except OSError:
                continue
        return False

    @staticmethod
    def _mount_spa(app: Any, directory: Any) -> None:
        """Mount a SPA as a fallback that only serves non-API requests.

        Uses ASGI middleware so that ``/api/*`` routes are handled by FastAPI
        first, and only unmatched non-API paths fall through to static files.
        """
        from pathlib import Path

        from starlette.responses import FileResponse
        from starlette.staticfiles import StaticFiles
        from starlette.types import ASGIApp, Receive, Scope, Send

        static_app = StaticFiles(directory=directory)
        static_directory = Path(directory)
        index = static_directory / "index.html"

        class SPAFallbackMiddleware:
            """Serve static files for non-API paths, falling back to index.html for SPA routes."""

            def __init__(self, wrapped_app: ASGIApp) -> None:
                self.app = wrapped_app

            async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
                if scope["type"] != "http" or scope["path"].startswith("/api"):
                    await self.app(scope, receive, send)
                    return

                path = scope["path"].lstrip("/")
                file_path = static_directory / path
                if path and file_path.is_file():
                    await static_app(scope, receive, send)
                    return

                # No matching file — serve index.html for SPA client-side routing
                response = FileResponse(index, media_type="text/html")
                await response(scope, receive, send)

        app.add_middleware(SPAFallbackMiddleware)
