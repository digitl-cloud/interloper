"""Service orchestration: start API, cron, worker, and dev server in threads."""

from __future__ import annotations

import atexit
import logging
import os
import signal
import socket
import subprocess
import threading
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from interloper_db import Store

    from interloper.catalog.base import Catalog
    from interloper.settings import AppSettings

logger = logging.getLogger(__name__)


def run_services(
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
    """Start services in threads and block until shutdown.

    The four runtime services (api, cron, worker, reaper) toggle
    independently so each role can run in its own pod. Cron and reaper
    are singletons (one per cluster); api and worker scale horizontally.

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

    Raises:
        SystemExit: In dev mode, when the server port is already in use.
    """
    stop_event = threading.Event()

    api_server = None
    cron_controller = None
    queue_controller = None
    reaper = None
    nuxt_process: subprocess.Popen[bytes] | None = None

    # -- API server -----------------------------------------------------------
    if run_api:
        import uvicorn
        from interloper_api import create_app

        cors_origins = [f"http://localhost:{settings.server.port}"] if dev_mode else None

        app = create_app(
            store=store,
            catalog=catalog,
            auth_config=settings.auth,
            smtp_config=settings.smtp,
            agent_config=settings.agent,
            cors_origins=cors_origins,
        )

        # In production mode, serve the built SPA as a fallback for non-API routes.
        # The interloper_app package is optional: API-only images (built without it)
        # raise ImportError here, which is fine — they just don't serve the SPA.
        if run_api and not dev_mode:
            try:
                from interloper_app import static_dir

                _mount_spa(app, static_dir())
                logger.info("Serving frontend from %s", static_dir())
            except (FileNotFoundError, ImportError) as e:
                logger.warning("Frontend not available: %s", e)

        uvi_config = uvicorn.Config(
            app,
            host=settings.server.host,
            port=api_port,
            log_level="info",
            log_config=None,
            ws="wsproto",
        )
        api_server = uvicorn.Server(uvi_config)

    # -- Cron controller + hook evaluator ---------------------------------------
    # The hook evaluator rides the cron service: both are cluster singletons
    # that turn declarative component intent into runs/side effects.
    hook_controller = None
    if run_cron:
        from interloper_scheduler import CronController, HookController

        cron_controller = CronController(
            store=store,
            reconcile_interval=settings.cron.reconcile_interval,
            max_execution_delay=settings.cron.max_execution_delay,
            batch_size=settings.cron.batch_size,
        )
        hook_controller = HookController(store=store)

    # -- Queue worker / reaper ------------------------------------------------
    # Both need a Launcher; build it once if either is enabled.
    if run_worker or run_reaper:
        from interloper_scheduler import Launcher

        launcher = Launcher.from_settings(
            settings.launcher,
            postgres=settings.postgres,
            runner=settings.runner,
            catalog=catalog,
            store=store,
        )

        if run_worker:
            from interloper_scheduler import QueueController

            queue_controller = QueueController(
                launcher=launcher,
                poll_interval=settings.worker.poll_interval,
            )

        if run_reaper:
            from interloper_scheduler import Reaper

            reaper = Reaper(
                store=store,
                launcher=launcher,
                timeout=settings.reaper.timeout,
                poll_interval=settings.reaper.poll_interval,
            )

    # -- Shutdown -------------------------------------------------------------
    def signal_nuxt(sig: int) -> None:
        if nuxt_process is not None:
            _kill_process_group(nuxt_process, sig)

    def shutdown_all(reason: str) -> None:
        if stop_event.is_set():
            return
        logger.info("Shutting down (%s)...", reason)
        stop_event.set()
        signal_nuxt(signal.SIGTERM)
        if cron_controller:
            cron_controller.stop()
        if hook_controller:
            hook_controller.stop()
        if queue_controller:
            queue_controller.stop()
        if reaper:
            reaper.stop()
        if api_server:
            api_server.should_exit = True

    # -- Nuxt dev server (dev mode only) ----------------------------------------
    if dev_mode:
        from interloper_app import source_dir

        # Fail fast if the port is taken: Nuxt would silently fall back to
        # :3001, stacking a half-broken instance (OAuth and CORS are pinned to
        # settings.server.port) on top of the one already running.
        if _port_in_use(settings.server.port):
            raise SystemExit(
                f"Port {settings.server.port} is already in use — a previous dev instance is likely still "
                f"running (check `lsof -nP -iTCP:{settings.server.port} -sTCP:LISTEN`). "
                "Stop it, or set INTERLOPER_SERVER_PORT to a free port."
            )

        nuxt_env = {
            **os.environ,
            "INTERLOPER_SERVER_PORT": str(settings.server.port),
            "INTERLOPER_API_PORT": str(api_port),
        }

        def run_nuxt_dev() -> None:
            nonlocal nuxt_process
            if stop_event.is_set():
                return
            # New session: the whole pnpm → nuxt → dev-worker tree lands in one
            # process group that shutdown can kill atomically; terminate()-ing
            # just pnpm leaves the rest of the tree orphaned on the port.
            nuxt_process = subprocess.Popen(
                ["pnpm", "dev"],
                cwd=source_dir(),
                env=nuxt_env,
                stdin=subprocess.DEVNULL,
                start_new_session=True,
            )
            if stop_event.is_set():
                # Shutdown ran between the check above and Popen returning.
                signal_nuxt(signal.SIGTERM)
            returncode = nuxt_process.wait()
            if not stop_event.is_set():
                logger.error("Nuxt dev server exited unexpectedly (code %s).", returncode)
                shutdown_all("nuxt dev server died")

        # Last resort if run_services unwinds via an unhandled exception.
        atexit.register(signal_nuxt, signal.SIGKILL)
        logger.info("Starting Nuxt dev server from %s", source_dir())

    # -- Signal handling ------------------------------------------------------
    def on_signal(sig: int, frame: object) -> None:
        shutdown_all(f"signal {signal.Signals(sig).name}")

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)
    if hasattr(signal, "SIGHUP"):
        # Terminal/session closed — without this the default handler kills us
        # before shutdown runs and the Nuxt tree is orphaned.
        signal.signal(signal.SIGHUP, on_signal)

    # -- Start threads --------------------------------------------------------
    threads: list[threading.Thread] = []
    if cron_controller:
        threads.append(threading.Thread(target=cron_controller.start, name="cron", daemon=True))
    if hook_controller:
        threads.append(threading.Thread(target=hook_controller.start, name="hooks", daemon=True))
    if queue_controller:
        threads.append(threading.Thread(target=queue_controller.start, name="worker", daemon=True))
    if reaper:
        threads.append(threading.Thread(target=reaper.start, name="reaper", daemon=True))
    if api_server:
        threads.append(threading.Thread(target=api_server.run, name="api", daemon=True))
    if dev_mode:
        threads.append(threading.Thread(target=run_nuxt_dev, name="nuxt-dev", daemon=True))

        parent_pid = os.getppid()

        def watch_parent() -> None:
            # `make dev-up` runs us under make/uv. If that chain dies without
            # signaling us (terminal killed, agent session reaped), we get
            # reparented — shut down instead of running on as an orphan.
            while not stop_event.wait(1.0):
                if os.getppid() != parent_pid:
                    shutdown_all("parent process died")
                    return

        threads.append(threading.Thread(target=watch_parent, name="parent-watchdog", daemon=True))

    for t in threads:
        t.start()

    services = ", ".join(t.name or "?" for t in threads)
    logger.info("Started services: %s", services)

    # Block until shutdown signal
    while not stop_event.is_set():
        stop_event.wait(timeout=1)

    for t in threads:
        t.join(timeout=5)

    # Nuxt got SIGTERM when shutdown started; if its group survived the grace
    # period, kill it so nothing outlives this process.
    if nuxt_process is not None and nuxt_process.poll() is None:
        logger.warning("Nuxt dev server still running after grace period; killing its process group.")
        signal_nuxt(signal.SIGKILL)

    logger.info("Shutdown complete.")


# ------------------------------------------------------------------
# Internals
# ------------------------------------------------------------------


def _kill_process_group(process: subprocess.Popen[bytes], sig: int) -> None:
    """Send a signal to a child's entire process group.

    The child must have been started with ``start_new_session=True`` so its
    pid is also its process-group id.
    """
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, sig)
    except ProcessLookupError:
        pass


def _port_in_use(port: int) -> bool:
    """Check whether something is already listening on localhost:``port``.

    Returns:
        True if a connection to the port succeeds.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.5)
        return sock.connect_ex(("127.0.0.1", port)) == 0


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

            # Check if the request matches an actual static file on disk
            path = scope["path"].lstrip("/")
            file_path = static_directory / path
            if path and file_path.is_file():
                await static_app(scope, receive, send)
                return

            # No matching file — serve index.html for SPA client-side routing
            response = FileResponse(index, media_type="text/html")
            await response(scope, receive, send)

    app.add_middleware(SPAFallbackMiddleware)
