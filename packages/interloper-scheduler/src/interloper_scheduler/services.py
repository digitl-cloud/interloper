"""The scheduler's long-lived services, built from settings."""

from __future__ import annotations

from typing import TYPE_CHECKING

from interloper_scheduler.cron import CronController
from interloper_scheduler.hooks import HookController
from interloper_scheduler.launcher import Launcher
from interloper_scheduler.queue import QueueController
from interloper_scheduler.reaper import Reaper
from interloper_scheduler.renewal import RenewalController

if TYPE_CHECKING:
    from interloper.catalog import Catalog
    from interloper.settings import AppSettings
    from interloper.utils import Service
    from interloper_db import Store


def scheduler_services(
    settings: AppSettings, *, store: Store, catalog: Catalog, cron: bool, worker: bool, reaper: bool
) -> dict[str, Service]:
    """Build the enabled scheduler services, by name.

    Cron brings the hook and renewal controllers, the cluster singletons that
    turn declared intent into runs; renewal also follows ``renewal.enabled``.
    The worker and the reaper share one launcher.

    Args:
        settings: The active settings.
        store: The store every service reads and writes through.
        catalog: The catalog the renewal controller and the launcher resolve components against.
        cron: Whether to run the cron, hook and renewal controllers.
        worker: Whether to run the queue worker.
        reaper: Whether to run the reaper.

    Returns:
        The services, keyed ``cron``, ``hooks``, ``renewal``, ``worker`` and ``reaper``, each present when enabled.
    """
    services: dict[str, Service] = {}
    if cron:
        services["cron"] = CronController(
            store=store,
            reconcile_interval=settings.cron.reconcile_interval,
            max_execution_delay=settings.cron.max_execution_delay,
            batch_size=settings.cron.batch_size,
        )
        services["hooks"] = HookController(store=store)
        if settings.renewal.enabled:
            services["renewal"] = RenewalController(
                catalog=catalog,
                store=store,
                reconcile_interval=settings.renewal.reconcile_interval,
                batch_size=settings.renewal.batch_size,
            )
    if worker or reaper:
        launcher = Launcher.from_settings(
            settings.launcher,
            server=settings.server,
            runner=settings.runner,
            reaper=settings.reaper,
            catalog=catalog,
            store=store,
        )
        if worker:
            services["worker"] = QueueController(
                launcher=launcher, store=store, poll_interval=settings.worker.poll_interval
            )
        if reaper:
            services["reaper"] = Reaper(
                store=store,
                launcher=launcher,
                startup_timeout=settings.reaper.startup_timeout,
                heartbeat_timeout=settings.reaper.heartbeat_timeout,
                run_timeout=settings.reaper.run_timeout,
                poll_interval=settings.reaper.poll_interval,
            )
    return services
