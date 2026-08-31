"""Run executor: the envelope that assembles a run's operations and drives the runner.

The executor owns the run lifecycle — load, mark running, trace, terminal
status, failure event — and the platform side of graph assembly: flattening
the hydrated target workload into its operations, joining upstream
dependencies from the store as non-materializable context, and skipping the
retry lineage's prior successes. The runner executes the operations; their
returned effects (config and state fields) are applied generically to each
operation's component row after the run.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
from typing import Any, cast
from uuid import UUID

import interloper as il
from interloper.errors import format_exception
from interloper.runner import ExecutionStatus, Runner
from interloper.telemetry import attributes
from interloper.telemetry.propagation import context_from_env, inject_metadata
from interloper.telemetry.tracer import tracer
from interloper_db import Store
from interloper_db.models import Component, Run
from opentelemetry.context import Context
from opentelemetry.trace import Link, get_current_span
from sqlmodel import Session

logger = logging.getLogger(__name__)


def run_event_metadata(run: Run, target: Component | None) -> dict[str, Any]:
    """Run-level event metadata: the run's ids plus its target's identity.

    The runner spreads this dict into every event it emits, and the
    ``target_*`` keys have no structured ``events`` column — they land in
    each event's ``data``, making events self-describing for telemetry
    (which component the run executed, under what name at the time)
    without a join through ``runs``.

    Args:
        run: The run the metadata describes.
        target: The run's target component row, when it still exists.

    Returns:
        The metadata dict.
    """
    metadata: dict[str, Any] = {
        "run_id": str(run.id),
        "backfill_id": str(run.backfill_id) if run.backfill_id else None,
        "org_id": str(run.org_id),
    }
    if target is not None:
        metadata |= {
            "target_id": str(target.id),
            "target_kind": target.kind,
            "target_key": target.key,
            "target_name": target.name,
        }
    return metadata


class RunExecutor:
    """Executes a run: loads from DB, assembles the operations, runs them.

    Uses the ``Store`` for hydration so all reconstruction goes through
    the standard framework path.
    """

    def __init__(
        self,
        store: Store | None = None,
        runner: Runner | None = None,
    ) -> None:
        """Initialize the executor.

        Args:
            store: The Store. Defaults to the settings-configured one.
            runner: Runner template, copied per run. Defaults to an
                ``AsyncRunner``.
        """
        self._store = store or Store.from_settings()
        self._runner = runner or il.AsyncRunner()

    def execute(self, run_id: UUID) -> bool:
        """Execute a run with full lifecycle tracking.

        Synchronous DB orchestration around the async DAG run; the async
        boundary lives in :meth:`_run_dag` where the engine is actually driven.

        Args:
            run_id: The run to execute.

        Returns:
            ``True`` if the run completed successfully, ``False`` otherwise.
        """
        org_id: UUID | None = None
        run_metadata: dict[str, Any] = {"run_id": str(run_id), "backfill_id": None}

        try:
            logger.info("Starting run %s", run_id)

            with Session(self._store.engine) as session:
                db_run = session.get(Run, run_id)
                if not db_run or not db_run.component_id:
                    logger.info("Run %s not found, skipping", run_id)
                    return False

                component_id = db_run.component_id
                org_id = db_run.org_id
                partition_key = db_run.partition_key
                retry_of = db_run.retry_of if db_run.retry_scope == "failed" else None
                run_metadata = run_event_metadata(db_run, session.get(Component, component_id))

                self._mark_running(session, db_run)

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
                    raise ValueError(f"Component kind '{type(target).kind}' declares no workload")

                operations = target.operations()
                if not operations:
                    logger.info("No operations for run %s, marking success", run_id)
                    self._store.runs.complete(run_id, success=True)
                    return True

                self._resolve_upstream(operations)
                if retry_of:
                    successes = self._prior_successes(retry_of)
                    for operation in operations:
                        if UUID(operation.id) in successes:
                            operation.materializable = False

                dag = il.DAG(*operations)
                partition = il.TimePartition.from_key(partition_key) if partition_key else None
                result = self._run_dag(dag, partition, org_id=org_id, run_id=run_id, metadata=run_metadata)

            self._apply_effects(result)
            success = result.status == ExecutionStatus.COMPLETED
            logger.info("Run %s completed: %s", run_id, result.status.name)
            self._store.runs.complete(run_id, success=success)
            return success

        except Exception as e:
            logger.exception("Run %s failed: %s", run_id, e)
            try:
                if org_id is not None:
                    metadata = {**run_metadata, "error": format_exception(e)}
                    event = il.Event(type=il.EventType.RUN_FAILED, metadata=metadata)
                    self._store.events.save(event, org_id=org_id, run_id=run_id)
                self._store.runs.complete(run_id, success=False)
            except Exception:
                logger.exception("Failed to mark run %s as failed", run_id)
            return False

    # -- Helpers ---------------------------------------------------------------

    @staticmethod
    def _mark_running(session: Session, db_run: Run) -> None:
        """Flip the run to ``running`` and stamp its start time.

        Args:
            session: Open session the write joins.
            db_run: The run row to mark.
        """
        db_run.status = "running"
        db_run.started_at = dt.datetime.now(dt.timezone.utc)
        session.add(db_run)
        session.commit()

    def _resolve_upstream(self, operations: list[il.Operation]) -> None:
        """Add transitive upstream dependencies to *operations* as non-materializable.

        Platform-side graph assembly: hydrated nodes carry their dependencies
        as row ids, so the walk loads each unseen id from the store and
        follows the dependencies it declares in turn. Joined upstream nodes
        are read from their destinations, never recomputed.

        Args:
            operations: The nodes to walk from, extended in place.
        """
        visited = {operation.id for operation in operations}
        frontier = list(operations)
        while frontier:
            next_frontier: list[il.Operation] = []
            for operation in frontier:
                for dependency_id in operation.dependencies.values():
                    if dependency_id in visited:
                        continue
                    visited.add(dependency_id)
                    upstream = cast(il.Asset, self._store.components.load(UUID(dependency_id)))
                    upstream.materializable = False
                    operations.append(upstream)
                    next_frontier.append(upstream)
            frontier = next_frontier

    def _prior_successes(self, retry_of: UUID) -> set[UUID]:
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
            retry_of: The retried run, the walk's starting point.

        Returns:
            The successful node row ids.
        """
        statuses: dict[UUID, str] = {}
        parent_id: UUID | None = retry_of
        with Session(self._store.engine) as session:
            while parent_id:
                for row in self._store.events.list_asset_executions(parent_id):
                    # Closest ancestor wins: only record a node the first time we see it.
                    statuses.setdefault(row.asset_id, row.status)
                parent = session.get(Run, parent_id)
                parent_id = parent.retry_of if parent else None

        return {asset_id for asset_id, status in statuses.items() if status == "success"}

    def _apply_effects(self, result: il.RunResult) -> None:
        """Persist the executed operations' effects onto their component rows.

        Args:
            result: The run result whose per-node execution infos carry the
                effects; nodes without effects are untouched.
        """
        for info in result.asset_executions.values():
            effects = info.effects
            if effects is None:
                continue
            if effects.config:
                self._store.components.merge_config(UUID(info.asset_id), effects.config)
            if effects.state:
                self._store.components.stamp_state(UUID(info.asset_id), **effects.state)

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
