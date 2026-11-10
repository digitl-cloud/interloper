"""The all-in-one application: the API, the scheduler's services and, in dev, the Nuxt dev server."""

from __future__ import annotations

import atexit
import logging
import os
import signal
import socket
import subprocess
import threading
from typing import TYPE_CHECKING

from interloper.utils import Service, Supervisor
from interloper_api import ApiServer
from interloper_scheduler import scheduler_services

from interloper_app.assets import SpaFallback, source_dir, static_dir

if TYPE_CHECKING:
    from interloper.catalog import Catalog
    from interloper.settings import AppSettings
    from interloper_db import Store

logger = logging.getLogger(__name__)


class App:
    """The services ``interloper app`` runs together, under one supervisor.

    Attributes:
        services: The services, by the name their thread and log lines carry.
    """

    def __init__(self, services: dict[str, Service], *, dev_port: int | None = None) -> None:
        """Hold the composed services.

        Args:
            services: The services, by name.
            dev_port: The Nuxt dev server's port, checked free before anything starts; ``None`` outside dev mode.
        """
        self.services = services
        self._dev_port = dev_port

    @classmethod
    def from_settings(
        cls,
        settings: AppSettings,
        *,
        store: Store,
        catalog: Catalog,
        api: bool,
        cron: bool,
        worker: bool,
        reaper: bool,
        dev: bool,
        port: int | None,
    ) -> App:
        """Compose the enabled services.

        Outside dev mode the API serves the built SPA too. In dev mode the
        Nuxt dev server serves it on ``server.port`` and proxies to the API,
        which moves to a free port unless one was given, and a watchdog stops
        everything when the parent process dies.

        Args:
            settings: The active settings, the command's flags already applied.
            store: The store every service reads and writes through.
            catalog: The catalog the services resolve components against.
            api: Whether to serve the API.
            cron: Whether to run the cron, hook and renewal controllers.
            worker: Whether to run the queue worker.
            reaper: Whether to run the reaper.
            dev: Whether to run the Nuxt dev server instead of serving the built SPA.
            port: The explicit ``--port``, ``None`` when it was not given.

        Returns:
            The composition, not yet started.

        Raises:
            SystemExit: When no service is enabled, or dev mode is asked for without the API.
        """
        if not (api or cron or worker or reaper):
            raise SystemExit("Error: No services enabled. Enable at least one of: --api, --cron, --worker, --reaper.")
        if dev and not api:
            raise SystemExit("Error: --dev runs the Nuxt dev server against the API; it cannot be used with --no-api.")
        services: dict[str, Service] = {}
        api_port = cls._api_port(settings, dev=dev, port=port)
        if api:
            services["api"] = cls._api(settings, store=store, catalog=catalog, port=api_port, dev=dev)
        services.update(
            scheduler_services(settings, store=store, catalog=catalog, cron=cron, worker=worker, reaper=reaper)
        )
        if dev:
            services["nuxt-dev"] = DevServer(server_port=settings.server.port, api_port=api_port)
            services["parent-watchdog"] = ParentWatchdog()
        return cls(services, dev_port=settings.server.port if dev else None)

    def run(self) -> None:
        """Run every service until a signal or a service's death."""
        if self._dev_port is not None:
            DevServer.check_port(self._dev_port)
        Supervisor(self.services).run()

    # -- Internals -------------------------------------------------------------

    @staticmethod
    def _api_port(settings: AppSettings, *, dev: bool, port: int | None) -> int:
        """The port the API binds.

        In dev mode with no explicit port, the API moves to a free port so the
        Nuxt dev server can take ``server.port`` and proxy to it.

        Args:
            settings: The active settings, ``--port`` already folded into ``server.port``.
            dev: Whether the Nuxt dev server runs alongside.
            port: The explicit ``--port``, ``None`` when it was not given.

        Returns:
            The port.
        """
        if dev and port is None:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
                probe.bind(("", 0))
                return probe.getsockname()[1]
        return settings.server.port

    @staticmethod
    def _api(settings: AppSettings, *, store: Store, catalog: Catalog, port: int, dev: bool) -> ApiServer:
        """Build the API, serving the built SPA outside dev mode.

        Args:
            settings: The active settings.
            store: The store the routes read and write through.
            catalog: The catalog the routes describe.
            port: The port to bind.
            dev: Whether the Nuxt dev server serves the SPA instead.

        Returns:
            The API server.
        """
        cors_origins = [f"http://localhost:{settings.server.port}"] if dev else None
        server = ApiServer.from_settings(settings, store=store, catalog=catalog, port=port, cors_origins=cors_origins)
        if not dev:
            try:
                directory = static_dir()
            except FileNotFoundError as error:
                logger.warning("Frontend not available: %s", error)
            else:
                server.app.add_middleware(SpaFallback, directory=directory)
                logger.info("Serving frontend from %s", directory)
        return server


class DevServer:
    """The Nuxt dev server, run from the source checkout until stopped.

    Its process tree (pnpm, nuxt, the dev workers) runs in its own process
    group, so stopping it signals the whole group; a group that ignored the
    stop is killed when this process exits.
    """

    def __init__(self, *, server_port: int, api_port: int) -> None:
        """Prepare the server; nothing starts until ``start``.

        Args:
            server_port: The port Nuxt serves the app on.
            api_port: The port of the API Nuxt proxies ``/api`` to.
        """
        self.server_port = server_port
        self.api_port = api_port
        self._process: subprocess.Popen[bytes] | None = None
        self._stopping = threading.Event()
        atexit.register(self._kill_survivor)

    @classmethod
    def check_port(cls, port: int) -> None:
        """Refuse a dev port something already listens on.

        Nuxt would otherwise fall back to another port silently, stacking a
        half-broken instance (OAuth and CORS are pinned to ``server.port``) on
        top of the one already running.

        Args:
            port: The dev server's port.

        Raises:
            SystemExit: When the port is in use.
        """
        if cls._port_in_use(port):
            raise SystemExit(
                f"Port {port} is already in use: a previous dev instance is likely still running "
                f"(check `lsof -nP -iTCP:{port} -sTCP:LISTEN`). Stop it, or set INTERLOPER_SERVER_PORT to a free port."
            )

    def start(self) -> None:
        """Run the dev server until it exits; an exit nobody asked for is logged."""
        if self._stopping.is_set():
            return
        logger.info("Starting Nuxt dev server from %s", source_dir())
        self._process = subprocess.Popen(
            ["pnpm", "dev"],
            cwd=source_dir(),
            env={
                **os.environ,
                "INTERLOPER_SERVER_PORT": str(self.server_port),
                "INTERLOPER_API_PORT": str(self.api_port),
            },
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        )
        if self._stopping.is_set():
            # The stop landed between the check above and Popen returning.
            self._kill_process_group(self._process, signal.SIGTERM)
        returncode = self._process.wait()
        if not self._stopping.is_set():
            logger.error("Nuxt dev server exited unexpectedly (code %s).", returncode)

    def stop(self) -> None:
        """Ask the dev server's process group to stop."""
        self._stopping.set()
        if self._process is not None:
            self._kill_process_group(self._process, signal.SIGTERM)

    # -- Internals -------------------------------------------------------------

    def _kill_survivor(self) -> None:
        """Kill a process group that outlived the stop, so nothing outlives this process."""
        if self._process is not None and self._process.poll() is None:
            logger.warning("Nuxt dev server still running after the stop; killing its process group.")
            self._kill_process_group(self._process, signal.SIGKILL)

    @staticmethod
    def _kill_process_group(process: subprocess.Popen[bytes], signal_number: int) -> None:
        """Send a signal to a child's entire process group.

        The child must have been started with ``start_new_session=True`` so its
        pid is also its process-group id.

        Args:
            process: The child process whose group is signalled; already-exited
                processes are skipped.
            signal_number: The signal to send to the whole group.
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
        IPv4-only probe misses it, and Nuxt's own port check does not, sending
        it to its silent fall-back path instead of this fail-fast.

        Args:
            port: TCP port to probe on loopback.

        Returns:
            True if a connection to the port succeeds on either loopback.
        """
        for family, address in ((socket.AF_INET, "127.0.0.1"), (socket.AF_INET6, "::1")):
            try:
                with socket.socket(family, socket.SOCK_STREAM) as probe:
                    probe.settimeout(0.5)
                    if probe.connect_ex((address, port)) == 0:
                        return True
            except OSError:
                continue
        return False


class ParentWatchdog:
    """Return once this process is reparented, i.e. the process that started it died.

    ``make dev-up`` runs ``interloper app`` under make and uv. If that chain
    dies without signalling it (a terminal killed, an agent session reaped),
    the reparenting is the only sign; returning is a death the supervisor
    turns into a shutdown, instead of running on as an orphan.
    """

    def __init__(self, interval: float = 1.0) -> None:
        """Remember the parent this process started under.

        Args:
            interval: Seconds between two checks of the parent.
        """
        self._parent = os.getppid()
        self._interval = interval
        self._stopping = threading.Event()

    def start(self) -> None:
        """Poll the parent every ``interval`` seconds until stopped or reparented."""
        while not self._stopping.wait(self._interval):
            if os.getppid() != self._parent:
                logger.info("Parent process died; shutting down.")
                return

    def stop(self) -> None:
        """Stop polling."""
        self._stopping.set()
