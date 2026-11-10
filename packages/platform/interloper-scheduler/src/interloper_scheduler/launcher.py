"""Launcher interface, in-process implementation, and the launcher registry."""

from __future__ import annotations

import asyncio
import logging
import threading
from abc import ABC, abstractmethod
from functools import partial
from typing import TYPE_CHECKING, Any

from interloper.dag import DAG
from interloper.errors import ConfigError, RunLostError, format_exception
from interloper.events import Event, EventRecorder, EventType
from interloper.registry import Registry
from interloper.runner import Runner
from interloper.settings import AppSettings, RunnerSettings
from interloper_db import Store
from opentelemetry import context as otel_context

if TYPE_CHECKING:
    from uuid import UUID

    from interloper.catalog import Catalog
    from interloper.settings import LauncherSettings, ReaperSettings, ServerSettings
    from interloper_db.models import Run

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
    is still alive is not the launcher's to say: the run's heartbeat, renewed
    by every batch of events it reports, says it.
    """

    @classmethod
    def from_settings(
        cls,
        settings: LauncherSettings,
        *,
        server: ServerSettings,
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
            server: Server settings; a launcher that spawns isolated processes
                hands them the API's ``internal_url`` to report to.
            runner: Runner settings forwarded to every launcher.
            reaper: Run liveness settings, whose heartbeat every run follows.
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
            settings, server=server, runner=runner, reaper=reaper, catalog=catalog, store=store
        )

    def __init__(
        self,
        runner_type: str = "async",
        runner_config: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the launcher.

        Args:
            runner_type: Runner type name (``async``, ``serial``, or a registered key).
            runner_config: Runner-specific kwargs forwarded to the runner constructor.
        """
        self._runner_type = runner_type
        self._runner_config = runner_config or {}

    @abstractmethod
    def launch(self, run: Run, token: str) -> None:
        """Start executing a dispatched run somewhere.

        The process loads what the run executes by its id, as its token, and
        reports back with the same token.

        Args:
            run: The run, dispatched.
            token: The run's token.
        """

    @staticmethod
    def command(run: Run) -> list[str]:
        """The ``interloper launch`` argv that executes *run*.

        The token is not an argument: it travels in the environment, which a
        process listing does not show.

        Args:
            run: The run.

        Returns:
            The argv.
        """
        return ["interloper", "launch", str(run.id)]

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
    """Launches runs in a detached thread of this process, loading and reporting straight through the store.

    The store is shared, so the run needs no token and makes no HTTP call;
    otherwise it runs exactly as a pod's ``interloper launch`` does.
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
            runner_type: Runner type name (``async``, ``serial``, or a registered key).
            runner_config: Runner-specific kwargs forwarded to the runner constructor.
            store: The Store runs report through. Defaults to the settings-configured one.
            reaper: Run liveness settings the runs' recorders follow;
                ``None`` reads them from the app settings.
        """
        super().__init__(runner_type=runner_type, runner_config=runner_config)
        self._store = store or Store.from_settings()
        self._reaper = reaper or AppSettings.get().reaper

    @classmethod
    def from_settings(
        cls,
        settings: LauncherSettings,
        *,
        server: ServerSettings,
        runner: RunnerSettings,
        reaper: ReaperSettings,
        catalog: Catalog | None,
        store: Any | None = None,
    ) -> InProcessLauncher:
        """Construct from settings; uses the runner config, the liveness settings and the shared store.

        Args:
            settings: The launcher settings block, unused by this launcher.
            server: Server settings, unused: the run reports through the shared store.
            runner: Runner settings each run's runner is built from.
            reaper: Run liveness settings the runs' recorders follow.
            catalog: Catalog the store hydrates against, unused here.
            store: An existing Store to reuse. Defaults to ``None``, which
                builds one from settings.

        Returns:
            The configured in-process launcher.
        """
        return cls(runner_type=runner.type, runner_config=runner.config, store=store, reaper=reaper)

    def launch(self, run: Run, token: str) -> None:
        """Launch a run in a background thread.

        The run shares this process, so a lost run cannot exit it: the run is
        ended in the database and its thread runs to its end.

        Args:
            run: The dispatched run.
            token: Unused: the run loads and reports through the shared store.
        """
        runner = Runner.from_settings(RunnerSettings(type=self._runner_type, config=self._runner_config))
        # A bare thread does not inherit contextvars: the launch span is carried across so the run continues it.
        context = otel_context.get_current()
        threading.Thread(
            target=self._execute, args=(run.id, runner, context), name=f"run-{run.id}", daemon=True
        ).start()
        logger.info("Launched run %s in a background thread", run.id)

    def _execute(self, run_id: UUID, runner: Runner, context: otel_context.Context) -> None:
        """Load the run and drive it on its thread, its events and effects reported through the store's ingest.

        Args:
            run_id: The dispatched run.
            runner: The runner to drive.
            context: The launching thread's trace context, which the run continues.
        """
        attached = otel_context.attach(context)
        recorder = EventRecorder(
            partial(self._store.runs.ingest, run_id),
            run_id=str(run_id),
            heartbeat_interval=self._reaper.heartbeat_interval,
            heartbeat_timeout=self._reaper.heartbeat_timeout,
        )
        try:
            with recorder:
                try:
                    spec = self._store.runs.spec(run_id)
                    dag = DAG.from_spec(spec.dag) if spec is not None else None
                    reason = f"Run {run_id} selects no operations"
                except Exception as error:  # noqa: BLE001 - a run that cannot be built fails with the reason
                    spec, dag, reason = None, None, format_exception(error)
                if spec is None or dag is None:
                    ending = {"run_id": str(run_id), "error": reason, "message": f"Run failed: {reason}"}
                    recorder(Event(type=EventType.RUN_FAILED, metadata=ending))
                    return
                recorded = runner.model_copy(update={"on_event": recorder, "reraise": False})
                recorder.finish(asyncio.run(recorded.run(dag, spec.window, spec.metadata)))
        except RunLostError as error:
            logger.info("%s", error)
        except Exception:
            logger.exception("Run %s did not complete", run_id)
        finally:
            otel_context.detach(attached)
