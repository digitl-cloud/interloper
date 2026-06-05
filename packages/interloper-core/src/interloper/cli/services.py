"""Service orchestration: start API, cron, worker, and dev server in threads."""

from __future__ import annotations

import logging
import signal
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
    """
    stop_event = threading.Event()

    api_server = None
    cron_controller = None
    queue_controller = None
    reaper = None
    nuxt_process = None

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
            event_ingest_token=settings.events.ingest_token,
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

    # -- Cron controller ------------------------------------------------------
    if run_cron:
        from interloper_scheduler import CronController

        cron_controller = CronController(
            store=store,
            reconcile_interval=settings.cron.reconcile_interval,
            max_execution_delay=settings.cron.max_execution_delay,
            batch_size=settings.cron.batch_size,
        )

    # -- Queue worker / reaper ------------------------------------------------
    # Both need a Launcher; build it once if either is enabled.
    if run_worker or run_reaper:
        from interloper_scheduler import build_launcher

        launcher = build_launcher(
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

    # -- Nuxt dev server (dev mode only) ----------------------------------------
    if dev_mode:
        import os
        import subprocess

        from interloper_app import source_dir

        nuxt_env = {
            **os.environ,
            "INTERLOPER_SERVER_PORT": str(settings.server.port),
            "INTERLOPER_API_PORT": str(api_port),
        }

        def run_nuxt_dev() -> None:
            nonlocal nuxt_process
            nuxt_process = subprocess.Popen(
                ["pnpm", "dev"],
                cwd=source_dir(),
                env=nuxt_env,
            )
            nuxt_process.wait()

        logger.info("Starting Nuxt dev server from %s", source_dir())

    # -- Signal handling ------------------------------------------------------
    def shutdown(sig: int, frame: object) -> None:
        logger.info("Shutting down (signal %s)...", sig)
        if nuxt_process:
            nuxt_process.terminate()
        if cron_controller:
            cron_controller.stop()
        if queue_controller:
            queue_controller.stop()
        if reaper:
            reaper.stop()
        if api_server:
            api_server.should_exit = True
        stop_event.set()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # -- Start threads --------------------------------------------------------
    threads: list[threading.Thread] = []
    if cron_controller:
        threads.append(threading.Thread(target=cron_controller.start, name="cron", daemon=True))
    if queue_controller:
        threads.append(threading.Thread(target=queue_controller.start, name="worker", daemon=True))
    if reaper:
        threads.append(threading.Thread(target=reaper.start, name="reaper", daemon=True))
    if api_server:
        threads.append(threading.Thread(target=api_server.run, name="api", daemon=True))
    if dev_mode:
        threads.append(threading.Thread(target=run_nuxt_dev, name="nuxt-dev", daemon=True))

    for t in threads:
        t.start()

    services = ", ".join(t.name or "?" for t in threads)
    logger.info("Started services: %s", services)

    # Block until shutdown signal
    while not stop_event.is_set():
        stop_event.wait(timeout=1)

    for t in threads:
        t.join(timeout=5)

    logger.info("Shutdown complete.")


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
