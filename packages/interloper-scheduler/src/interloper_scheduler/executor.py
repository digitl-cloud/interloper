"""Run executor: assembles a dispatched run's operations and drives the runner.

Everything the run says reaches the store through one path. A `RunRecorder`
delivers the runner's events in batches, and the run's effects with the last
one, to `RunStore.ingest`, which folds the run's start, its verdict and its
heartbeat from them: the verdict is the ingestion of the run's own terminal
event, so it never exists without the events before it. A run that cannot
even be assembled (its target deleted, a kind that declares no workload, a
window its operations refuse) is failed with the reason.

Flattening the hydrated target into its operations and joining the bound
upstreams the run does not materialize are the framework's concern
(``Workload.operations()``, the DAG), not the executor's.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from functools import partial
from typing import Any
from uuid import UUID

import interloper as il
from interloper.errors import RunLostError, format_exception
from interloper.events import RunRecorder
from interloper.runner import ExecutionStatus, Runner
from interloper.settings import AppSettings, ReaperSettings
from interloper.telemetry import attributes
from interloper.telemetry.propagation import context_from_env, inject_metadata
from interloper.telemetry.tracer import tracer
from interloper_db import ConflictError, ExecutionQuery, NotFoundError, RunStatus, Store
from interloper_db.models import Run
from opentelemetry.context import Context
from opentelemetry.trace import Link, get_current_span

logger = logging.getLogger(__name__)


class RunExecutor:
    """Executes a dispatched run: assembles its operations from the store and drives them, reporting through ingest."""

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
            reaper: The run liveness settings the recorder follows. Defaults
                to the settings-configured ones.
            on_lost: Called when the run is lost (ended elsewhere, or unable
                to report for half the heartbeat timeout); a container passes
                its process exit. ``None`` lets the run finish on its own.
        """
        self._store = store or Store.from_settings()
        self._runner = runner or il.AsyncRunner()
        self._reaper = reaper or AppSettings.get().reaper
        self._on_lost = on_lost

    def execute(self, run_id: UUID) -> bool:
        """Execute a dispatched run.

        The run roots its own trace: dispatch and execution are asynchronous,
        so the dispatch span (the ``TRACEPARENT`` env in a launched container,
        the ambient launch span in-process) is linked, not parented.

        Args:
            run_id: The run to execute.

        Returns:
            ``True`` if the run completed successfully, ``False`` otherwise.
        """
        try:
            db_run = self._store.runs.get(run_id)
        except NotFoundError:
            logger.warning("Run %s not found; skipping", run_id)
            return False
        if db_run.status != RunStatus.DISPATCHED:
            logger.warning("Run %s is %s, not dispatched; skipping", run_id, db_run.status)
            return False
        logger.info("Starting run %s", run_id)
        metadata = db_run.event_metadata(db_run.target)
        dispatch = get_current_span(context_from_env()).get_span_context()
        with tracer().start_as_current_span(
            "interloper.run.execute",
            context=Context(),
            links=[Link(dispatch)] if dispatch.is_valid else [],
            attributes={attributes.RUN_ID: str(run_id)},
        ):
            # The runner nests its span under the metadata's parent before the environment's.
            inject_metadata(metadata)
            try:
                dag = self._assemble(db_run)
                if dag is None:
                    logger.info("No operations for run %s, marking success", run_id)
                    self._store.runs.complete(run_id)
                    return True
                return self._run(db_run, dag, metadata)
            except Exception as error:
                logger.exception("Run %s failed", run_id)
                self._fail(run_id, format_exception(error))
                return False

    def _assemble(self, db_run: Run) -> il.DAG | None:
        """Hydrate the run's target into the DAG of operations it executes.

        A failed-scope retry's prior successes are disabled, so the run
        re-executes only what had not succeeded.

        Args:
            db_run: The dispatched run, its target loaded.

        Returns:
            The DAG, or ``None`` when the target selects no operations.

        Raises:
            NotFoundError: If the run's target was deleted.
            TypeError: If the target's kind declares no workload.
        """
        if db_run.component_id is None:
            raise NotFoundError(f"Run {db_run.id} targets a component that was deleted")
        target = self._store.components.load(db_run.component_id)
        if not isinstance(target, il.Workload):
            raise TypeError(f"Component kind '{type(target).kind}' declares no workload")
        operations = target.operations()
        if not operations:
            return None
        if db_run.retry_scope == "failed" and db_run.retry_of is not None:
            successes = self._prior_successes(db_run.org_id, db_run.retry_of)
            for operation in operations:
                if UUID(operation.id) in successes:
                    operation.enabled = False
        return il.DAG(*operations)

    def _run(self, db_run: Run, dag: il.DAG, metadata: dict[str, Any]) -> bool:
        """Drive the runner over the DAG, every event and the effects recorded through ingest.

        An operation's failure is the run's result and its events, not an
        exception, so the runner never re-raises it here.

        Args:
            db_run: The run.
            dag: Its operations.
            metadata: Its event metadata.

        Returns:
            Whether the run completed; ``False`` too when it was lost.
        """
        window = il.TimePartition.from_key(db_run.partition_key) if db_run.partition_key else None
        recorder = RunRecorder(
            partial(self._store.runs.ingest, db_run.id),
            run_id=str(db_run.id),
            heartbeat_interval=self._reaper.heartbeat_interval,
            heartbeat_timeout=self._reaper.heartbeat_timeout,
            on_lost=self._on_lost,
        )
        runner = self._runner.model_copy(update={"on_event": recorder, "reraise": False})
        try:
            with recorder:
                result = asyncio.run(runner.run(dag, window, metadata=metadata))
                recorder.finish(result)
        except RunLostError:
            return False
        logger.info("Run %s completed: %s", db_run.id, result.status.name)
        return result.status is ExecutionStatus.COMPLETED

    def _fail(self, run_id: UUID, error: str) -> None:
        """Fail a run that could not execute, unless another writer ended it first.

        Args:
            run_id: The run.
            error: Why, as a reader should see it.
        """
        try:
            self._store.runs.fail(run_id, error)
        except ConflictError as conflict:
            logger.warning("Run %s was completed by another writer first: %s", run_id, conflict)
        except Exception:
            logger.exception("Failed to mark run %s as failed", run_id)

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
