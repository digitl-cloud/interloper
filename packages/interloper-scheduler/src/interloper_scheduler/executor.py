"""Run executor: the envelope that assembles a run's operations and drives the runner.

The executor owns the run lifecycle: load, mark running, heartbeat, trace,
terminal status, failure event, and skipping the retry lineage's prior
successes.
Flattening the hydrated target workload into its operations and joining
bound upstreams the run itself does not materialize are the framework's own
concern (``Workload.operations()``, ``DAG._include_read_only_upstreams()``),
not the executor's. The runner executes the operations; their returned
effects (config and state fields) are applied generically to each
operation's component row after the run.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any
from uuid import UUID

import interloper as il
from interloper.errors import ConflictError, NotFoundError, format_exception
from interloper.runner import ExecutionStatus, Runner
from interloper.settings import AppSettings, ReaperSettings
from interloper.telemetry import attributes
from interloper.telemetry.propagation import context_from_env, inject_metadata
from interloper.telemetry.tracer import tracer
from interloper_db import ExecutionQuery, Store
from interloper_db.models import Run
from opentelemetry.context import Context
from opentelemetry.trace import Link, get_current_span

from interloper_scheduler.heartbeat import RunHeartbeat

logger = logging.getLogger(__name__)


class RunExecutor:
    """Executes a run: loads from DB, assembles the operations, runs them.

    Uses the ``Store`` for hydration so all reconstruction goes through
    the standard framework path.
    """

    def __init__(
        self,
        store: Store | None = None,
        runner: Runner | None = None,
        *,
        reaper: ReaperSettings | None = None,
        on_lost: Callable[[], None] | None = None,
    ) -> None:
        """Initialize the executor.

        Args:
            store: The Store. Defaults to the settings-configured one.
            runner: Runner template, copied per run. Defaults to an
                ``AsyncRunner``.
            reaper: The run liveness settings the heartbeat follows. Defaults
                to the settings-configured ones.
            on_lost: Called when a run's heartbeat learns it should stop (see
                :class:`~interloper_scheduler.heartbeat.RunHeartbeat`); a
                container passes its process exit. ``None`` stops only the
                heartbeat.
        """
        self._store = store or Store.from_settings()
        self._runner = runner or il.AsyncRunner()
        self._reaper = reaper or AppSettings.get().reaper
        self._on_lost = on_lost

    def execute(self, run_id: UUID) -> bool:
        """Execute a run with full lifecycle tracking.

        Synchronous DB orchestration around the async DAG run; the async
        boundary lives in :meth:`_run_dag` where the engine is actually driven.

        A component whose kind declares no workload cannot be executed, and
        fails the run rather than silently succeeding.

        Args:
            run_id: The run to execute.

        Returns:
            ``True`` if the run completed successfully, ``False`` otherwise.
        """
        logger.info("Starting run %s", run_id)
        try:
            db_run = self._store.runs.start(run_id)
        except (NotFoundError, ConflictError) as e:
            logger.warning("Run %s cannot start, skipping: %s", run_id, e)
            return False
        run_metadata = db_run.event_metadata(db_run.target)
        heartbeat = RunHeartbeat(
            self._store,
            run_id,
            interval=self._reaper.heartbeat_interval,
            timeout=self._reaper.heartbeat_timeout,
            on_lost=self._on_lost,
        )
        with heartbeat:
            try:
                return self._execute_started(db_run, run_metadata)
            except Exception as e:
                logger.exception("Run %s failed", run_id)
                try:
                    self._store.runs.fail(run_id, format_exception(e), metadata=run_metadata)
                except ConflictError as conflict:
                    logger.warning("Run %s was completed by another writer first: %s", run_id, conflict)
                except Exception:
                    logger.exception("Failed to mark run %s as failed", run_id)
                return False

    def _execute_started(self, db_run: Run, run_metadata: dict[str, Any]) -> bool:
        """Assemble a started run's operations, drive them, and record the verdict.

        Args:
            db_run: The run, just marked running, its target loaded.
            run_metadata: The run's event metadata.

        Returns:
            ``True`` if the run completed successfully, ``False`` otherwise.

        Raises:
            NotFoundError: If the run's target was deleted.
            TypeError: If the run's component declares no workload.
        """
        run_id = db_run.id
        if db_run.component_id is None:
            raise NotFoundError(f"Run {run_id} targets a component that was deleted")
        component_id = db_run.component_id
        org_id = db_run.org_id
        partition_key = db_run.partition_key
        retry_of = db_run.retry_of if db_run.retry_scope == "failed" else None

        # The run roots its own trace: dispatch and execution are
        # asynchronous, so the dispatch span (the ``TRACEPARENT`` env in a
        # launched container, the ambient launch span in-process) is
        # linked, not parented. Injecting the root into the run metadata
        # is what nests the runner span here — it prefers a metadata
        # parent over the environment one it would inherit otherwise.
        dispatch = get_current_span(context_from_env()).get_span_context()
        with tracer().start_as_current_span(
            "interloper.run.execute",
            context=Context(),
            links=[Link(dispatch)] if dispatch.is_valid else [],
            attributes={attributes.RUN_ID: str(run_id)},
        ):
            inject_metadata(run_metadata)

            target = self._store.components.load(component_id)
            if not isinstance(target, il.Workload):
                raise TypeError(f"Component kind '{type(target).kind}' declares no workload")

            operations = target.operations()
            if not operations:
                logger.info("No operations for run %s, marking success", run_id)
                return self._complete(run_id, success=True)

            if retry_of:
                successes = self._prior_successes(org_id, retry_of)
                for operation in operations:
                    if UUID(operation.id) in successes:
                        operation.enabled = False

            dag = il.DAG(*operations)
            partition = il.TimePartition.from_key(partition_key) if partition_key else None
            result = self._run_dag(dag, partition, org_id=org_id, run_id=run_id, metadata=run_metadata)

        self._apply_effects(result)
        success = result.status == ExecutionStatus.COMPLETED
        logger.info("Run %s completed: %s", run_id, result.status.name)
        return self._complete(run_id, success=success)

    def _complete(self, run_id: UUID, *, success: bool) -> bool:
        """Record the run's verdict, unless another writer recorded one first.

        A run can be ended while it executes (reaped, timed out, canceled);
        its verdict is then not this executor's to write, so the store's
        refusal is logged and read as a failure.

        Args:
            run_id: The run to complete.
            success: The verdict this execution reached.

        Returns:
            ``success`` once recorded, ``False`` when the run was already terminal.
        """
        try:
            self._store.runs.complete(run_id, success=success)
        except ValueError as e:
            logger.warning("Run %s was completed by another writer first: %s", run_id, e)
            return False
        return success

    # -- Internals -------------------------------------------------------------

    def _prior_successes(self, org_id: UUID, retry_of: UUID) -> set[UUID]:
        """Node row ids that already succeeded in the retry lineage.

        For a ``"failed"``-scope retry, nodes that completed successfully in an
        earlier attempt are read from their destination instead of recomputed;
        only the previously failed/cancelled nodes re-execute. Successes are
        resolved by walking the ``retry_of`` chain back to the root attempt so
        that nodes skipped by an intermediate failed-only retry (which emit no
        events) still carry their earlier success forward. Statuses are matched
        by node row id, never by key — a run can span many assets sharing one
        key (e.g. an ``ads_stats`` per account), and one success must not skip
        the others.

        Args:
            org_id: Organisation the retry lineage belongs to.
            retry_of: The retried run, the walk's starting point.

        Returns:
            The successful node row ids.
        """
        statuses: dict[UUID, str] = {}
        parent_id: UUID | None = retry_of
        while parent_id:
            for row in self._store.executions.list(org_id, ExecutionQuery(limit=None), run_id=parent_id).items:
                # Closest ancestor wins: only record a node the first time we see it.
                statuses.setdefault(row.component_id, row.status)
            parent_id = self._store.runs.get(parent_id, org_id=org_id).retry_of
        return {asset_id for asset_id, status in statuses.items() if status == "success"}

    def _apply_effects(self, result: il.RunResult) -> None:
        """Persist the executed operations' effects onto their component rows.

        Args:
            result: The run result whose per-node execution infos carry the
                effects; nodes without effects are untouched.
        """
        for info in result.executions.values():
            effects = info.effects
            if effects is None:
                continue
            if effects.config:
                self._store.components.merge_config(UUID(info.component_id), effects.config)
            if effects.state:
                self._store.components.stamp_state(UUID(info.component_id), **effects.state)

    def _run_dag(
        self,
        dag: il.DAG,
        partition: il.TimePartition | None,
        *,
        org_id: UUID,
        run_id: UUID,
        metadata: dict[str, Any],
    ) -> il.RunResult:
        """Drive the DAG through a per-run copy of the runner template.

        Args:
            dag: The assembled operation graph.
            partition: The run's partition scope, when partitioned.
            org_id: Organisation the run's events belong to.
            run_id: The run the events attach to.
            metadata: Run-level metadata spread into every event.

        Returns:
            The runner's result.
        """

        def handle_event(event: il.Event) -> None:
            self._store.events.save(event, org_id=org_id, run_id=run_id)

        # A fresh copy per execution: the runner template is shared across
        # runs, but run state and the event handler are per-run.
        runner = self._runner.model_copy(update={"on_event": handle_event})
        return asyncio.run(runner.run(dag, partition, metadata=metadata))
