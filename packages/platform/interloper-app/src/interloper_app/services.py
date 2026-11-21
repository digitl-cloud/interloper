"""The all-in-one application: the API and the scheduler's services, under one supervisor."""

from __future__ import annotations

import logging
import os
import threading
from collections.abc import Sequence
from typing import TYPE_CHECKING

from interloper.utils import Service, Supervisor
from interloper_api import ApiServer
from interloper_scheduler import scheduler_services

from interloper_app.assets import SpaFallback, static_dir

if TYPE_CHECKING:
    from fastapi import APIRouter
    from interloper.catalog import Catalog
    from interloper_api.settings import ApiSettings
    from interloper_db import Store
    from interloper_scheduler.settings import SchedulerSettings

logger = logging.getLogger(__name__)


class App:
    """The services ``interloper app`` runs together, under one supervisor.

    Attributes:
        services: The services, by the name their thread and log lines carry.
    """

    def __init__(self, services: dict[str, Service]) -> None:
        """Hold the composed services.

        Args:
            services: The services, by name.
        """
        self.services = services

    @classmethod
    def from_settings(
        cls,
        api_settings: ApiSettings,
        scheduler_settings: SchedulerSettings,
        *,
        store: Store,
        catalog: Catalog,
        api: bool,
        cron: bool,
        worker: bool,
        reaper: bool,
        port: int | None = None,
        spa: bool = True,
        routers: Sequence[APIRouter] = (),
        dev_origins: Sequence[str] = (),
    ) -> App:
        """Compose the enabled services.

        Args:
            api_settings: The API's settings, the command's flags already applied.
            scheduler_settings: The scheduler's settings, the command's flags already applied.
            store: The store every service reads and writes through.
            catalog: The catalog the services resolve components against.
            api: Whether to serve the API.
            cron: Whether to run the cron, hook and renewal controllers.
            worker: Whether to run the queue worker.
            reaper: Whether to run the reaper.
            port: The port the API binds; ``None`` binds ``server.port``.
            spa: Whether the API serves the built SPA too. ``interloper dev``
                turns it off when the Nuxt dev server serves the app.
            routers: Routes mounted on the API beside its own: ``interloper
                dev``'s sign-in, which no other caller passes.
            dev_origins: The extension UIs' Vite dev servers the built page
                imports from, under ``interloper dev``; none otherwise.

        Returns:
            The composition, not yet started.

        Raises:
            SystemExit: When no service is enabled.
        """
        if not (api or cron or worker or reaper):
            raise SystemExit("Error: No services enabled. Enable at least one of: --api, --cron, --worker, --reaper.")
        services: dict[str, Service] = {}
        if api:
            services["api"] = cls._api(
                api_settings,
                store=store,
                catalog=catalog,
                port=port or api_settings.server.port,
                spa=spa,
                routers=routers,
                dev_origins=dev_origins,
            )
        services.update(
            scheduler_services(
                scheduler_settings, store=store, catalog=catalog, cron=cron, worker=worker, reaper=reaper
            )
        )
        return cls(services)

    def run(self) -> None:
        """Run every service until a signal or a service's death."""
        Supervisor(self.services).run()

    # -- Internals -------------------------------------------------------------

    @staticmethod
    def _api(
        settings: ApiSettings,
        *,
        store: Store,
        catalog: Catalog,
        port: int,
        spa: bool,
        routers: Sequence[APIRouter],
        dev_origins: Sequence[str],
    ) -> ApiServer:
        """Build the API, serving the built SPA when asked.

        Args:
            settings: The API's settings.
            store: The store the routes read and write through.
            catalog: The catalog the routes describe.
            port: The port to bind.
            spa: Whether to serve the built SPA too.
            routers: Routes mounted beside the API's own.
            dev_origins: The Vite dev servers the built page's import map names.

        Returns:
            The API server.
        """
        server = ApiServer.from_settings(settings, store=store, catalog=catalog, port=port)
        for router in routers:
            server.app.include_router(router)
        if spa:
            try:
                directory = static_dir()
            except FileNotFoundError as error:
                logger.warning("Frontend not available: %s", error)
            else:
                server.app.add_middleware(SpaFallback, directory=directory, dev_origins=dev_origins)
                logger.info("Serving frontend from %s", directory)
        return server


class ParentWatchdog:
    """Return once this process is reparented, i.e. the process that started it died.

    ``interloper dev`` runs the platform in a process of its own. If ``dev``
    dies without signalling it (a terminal killed, an agent session reaped),
    the reparenting is the only sign; returning is a death the supervisor
    turns into a shutdown, instead of running on as an orphan.
    """

    def __init__(self, interval: float = 1.0, parent: int | None = None) -> None:
        """Remember the parent this process started under.

        Args:
            interval: Seconds between two checks of the parent.
            parent: The parent's pid, as the parent itself gave it, so a
                parent that died before this process got here is noticed;
                ``None`` reads it now.
        """
        self._parent = os.getppid() if parent is None else parent
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
