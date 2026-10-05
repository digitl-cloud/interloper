"""Launcher interface, in-process implementation, and the launcher registry."""

from __future__ import annotations

import logging
import threading
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any
from uuid import UUID

from interloper.errors import ConfigError
from interloper.registry import Registry
from interloper_db import Store
from opentelemetry import context as otel_context

if TYPE_CHECKING:
    from interloper.catalog.base import Catalog
    from interloper.settings import LauncherSettings, PostgresSettings, ReaperSettings, RunnerSettings

logger = logging.getLogger(__name__)


#: Launcher classes by type key (``LauncherSettings.type``), fed by the
#: ``interloper.launchers`` entry-point group — every launcher, the
#: built-in in-process one included, registers through it. Installed means
#: discovered: a new launcher is one new package with one entry point.
LAUNCHERS: Registry[type[Launcher]] = Registry("interloper.launchers")


class Launcher(ABC):
    """Abstract base for run launchers.

    A launcher decides *where* a run executes: in-process, Docker, Kubernetes, etc.
    Every launcher carries a runner configuration that determines *how* the
    DAG is executed once it reaches the execution environment. Whether a run
    is still alive is not the launcher's to say: the run's heartbeat says it
    (see :mod:`~interloper_scheduler.heartbeat`).
    """

    @classmethod
    def from_settings(
        cls,
        settings: LauncherSettings,
        *,
        postgres: PostgresSettings,
        runner: RunnerSettings,
        reaper: ReaperSettings,
        catalog: Catalog | None,
        store: Any | None = None,
    ) -> Launcher:
        """Construct the launcher the settings describe.

        Called on ``Launcher``, resolves ``settings.type`` in ``LAUNCHERS``
        and delegates to that class's own ``from_settings`` — each launcher
        owns its recipe (which settings it consumes, how ``settings.config``
        maps to its constructor). Concrete launchers must override this
        with theirs.

        Args:
            settings: Launcher settings (type + type-specific config).
            postgres: Postgres settings forwarded to launchers that spawn
                isolated processes (e.g. Docker).
            runner: Runner settings forwarded to every launcher.
            reaper: Run liveness settings, whose heartbeat every run's
                executor follows.
            catalog: Catalog forwarded to launchers that spawn isolated
                processes so they can reproduce an identical catalog
                (``None`` is accepted by launchers that don't consume it).
            store: Optional Store instance shared with in-process launchers.

        Returns:
            The configured launcher instance.

        Raises:
            ConfigError: If no launcher is registered under ``settings.type``.
            NotImplementedError: If the resolved launcher class does not
                implement its own ``from_settings``.
        """
        if cls is not Launcher:
            raise NotImplementedError(f"{cls.__name__} must implement from_settings")
        launcher_cls = LAUNCHERS.get(settings.type)
        if launcher_cls is None:
            raise ConfigError(
                f"Unknown launcher: {settings.type!r} (available: {list(LAUNCHERS.keys())}). "
                f"Is the matching interloper package installed?"
            )
        return launcher_cls.from_settings(
            settings, postgres=postgres, runner=runner, reaper=reaper, catalog=catalog, store=store
        )

    def __init__(
        self,
        runner_type: str = "async",
        runner_config: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the launcher.

        Args:
            runner_type: Runner type name (``async``, ``serial``, ``multi_process``).
            runner_config: Runner-specific kwargs forwarded to the runner constructor.
        """
        self._runner_type = runner_type
        self._runner_config = runner_config or {}

    @abstractmethod
    def launch(self, run_id: UUID) -> None:
        """Launch a run for execution.

        Args:
            run_id: The run UUID to execute.
        """

    def diagnose(self, run_id: UUID) -> str | None:
        """Say why a launched run's workload stopped, when the launcher can tell.

        The reaper has already concluded the run is dead from its silent
        heartbeat; this only adds what the infrastructure saw (an OOM kill,
        an exit code) to the reason it records. It decides nothing.

        Args:
            run_id: The run UUID.

        Returns:
            A one-line reason, or ``None`` when the workload has not stopped,
            is gone, or the launcher cannot introspect its runs.
        """
        return None


class InProcessLauncher(Launcher):
    """Launches runs in a detached thread using ``RunExecutor``.

    Accepts an optional ``store`` so all runs share the same persistence
    layer (encryption keys, etc.) rather than creating a fresh default.
    """

    def __init__(
        self,
        runner_type: str = "async",
        runner_config: dict[str, Any] | None = None,
        store: Store | None = None,
        reaper: ReaperSettings | None = None,
    ) -> None:
        """Initialize the launcher.

        Args:
            runner_type: Runner type name (``async``, ``serial``, ``multi_process``).
            runner_config: Runner-specific kwargs forwarded to the runner constructor.
            store: Optional Store instance to share with executors.
            reaper: Run liveness settings the executors' heartbeat follows;
                ``None`` reads them from the app settings.
        """
        super().__init__(runner_type=runner_type, runner_config=runner_config)
        self._store = store
        self._reaper = reaper

    @classmethod
    def from_settings(
        cls,
        settings: LauncherSettings,
        *,
        postgres: PostgresSettings,
        runner: RunnerSettings,
        reaper: ReaperSettings,
        catalog: Catalog | None,
        store: Any | None = None,
    ) -> InProcessLauncher:
        """Construct from settings; uses the runner config, the liveness settings and the shared store.

        Args:
            settings: The launcher settings block, unused by this launcher.
            postgres: Postgres settings, unused: the store is shared in-process.
            runner: Runner settings the executor's runner is built from.
            reaper: Run liveness settings the executors' heartbeat follows.
            catalog: Catalog the store hydrates against.
            store: An existing Store to reuse. Defaults to ``None``, which
                builds one from settings.

        Returns:
            The configured in-process launcher.
        """
        return cls(runner_type=runner.type, runner_config=runner.config, store=store, reaper=reaper)

    def launch(self, run_id: UUID) -> None:
        """Launch a run in a background thread.

        Args:
            run_id: The run UUID to execute.
        """
        from interloper.runner import Runner
        from interloper.settings import RunnerSettings

        from interloper_scheduler.executor import RunExecutor

        runner = Runner.from_settings(RunnerSettings(type=self._runner_type, config=self._runner_config))
        # The run shares this process, so a lost heartbeat cannot exit it:
        # the run is ended in the database and its thread runs to its end.
        executor = RunExecutor(store=self._store, runner=runner, reaper=self._reaper)

        # A bare thread does not inherit contextvars — carry the launch-time
        # OTel context across so the run's spans parent under the launch span.
        context = otel_context.get_current()

        def _execute() -> None:
            token = otel_context.attach(context)
            try:
                executor.execute(run_id)
            finally:
                otel_context.detach(token)

        thread = threading.Thread(target=_execute, daemon=True)
        thread.start()
        logger.info("Launched run %s in background thread", run_id)
