"""Run executor: loads a run from DB, builds the DAG, and executes it."""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
from typing import Any, cast
from uuid import UUID

import interloper as il
from interloper.runner import ExecutionStatus, Runner
from interloper_db import Store
from interloper_db.models import ComponentRelation, Run
from sqlmodel import Session, col, select

logger = logging.getLogger(__name__)


class RunExecutor:
    """Executes a run: loads from DB, builds the DAG, runs it, tracks events.

    Uses the ``Store`` for hydration so all reconstruction goes through
    the standard framework path.
    """

    def __init__(
        self,
        store: Store | None = None,
        runner: Runner | None = None,
    ) -> None:
        self._store = store or Store.from_settings()
        self._runner = runner or il.AsyncRunner()

    def execute(self, run_id: UUID) -> bool:
        """Execute a run with full lifecycle tracking.

        Synchronous DB orchestration around the async DAG run; the async
        boundary lives in :meth:`_run_dag` where the engine is actually driven.

        Returns:
            ``True`` if the run completed successfully, ``False`` otherwise.
        """
        org_id: UUID | None = None
        backfill_id: str | None = None

        try:
            logger.info("Starting run %s", run_id)

            with Session(self._store.engine) as session:
                db_run = session.get(Run, run_id)
                if not db_run or not db_run.component_id:
                    logger.info("Run %s not found, skipping", run_id)
                    return False

                component_id = db_run.component_id
                org_id = db_run.org_id
                backfill_id = str(db_run.backfill_id) if db_run.backfill_id else None
                partition_date = db_run.partition_date
                retry_of = db_run.retry_of if db_run.retry_scope == "failed" else None

                self._mark_running(session, db_run)

            target = self._store.load(component_id)
            assets = _target_assets(target)
            if not assets:
                logger.info("No sources or assets for run %s, marking success", run_id)
                self._store.complete_run(run_id, success=True)
                return True

            self._resolve_upstream_deps(assets)

            if retry_of:
                self._skip_succeeded_assets(retry_of, assets)

            dag = il.DAG(*assets)
            partition = il.TimePartition(partition_date) if partition_date else None

            result = self._run_dag(dag, partition, org_id=org_id, run_id=run_id, backfill_id=backfill_id)

            success = result.status == ExecutionStatus.COMPLETED
            logger.info("Run %s completed: %s", run_id, result.status.name)
            self._store.complete_run(run_id, success=success)
            return success

        except Exception as e:
            logger.exception("Run %s failed: %s", run_id, e)
            try:
                metadata: dict[str, Any] = {
                    "run_id": str(run_id),
                    "backfill_id": backfill_id,
                    "error": str(e),
                }
                if org_id is not None:
                    event = il.Event(type=il.EventType.RUN_FAILED, metadata=metadata)
                    self._store.save_event(event, org_id=org_id, run_id=run_id)
                self._store.complete_run(run_id, success=False)
            except Exception:
                logger.exception("Failed to mark run %s as failed", run_id)
            return False

    # -- Helpers ---------------------------------------------------------------

    @staticmethod
    def _mark_running(session: Session, db_run: Run) -> None:
        db_run.status = "running"
        db_run.started_at = dt.datetime.now(dt.timezone.utc)
        session.add(db_run)
        session.commit()

    def _skip_succeeded_assets(self, retry_of: UUID, assets: list[il.Asset]) -> None:
        """Mark assets that already succeeded in the retry lineage as non-materializable.

        For a ``"failed"``-scope retry, assets that completed successfully in an
        earlier attempt are read from their destination instead of recomputed;
        only the previously failed/cancelled assets re-execute. Successes are
        resolved by walking the ``retry_of`` chain back to the root attempt so
        that assets skipped by an intermediate failed-only retry (which emit no
        events) still carry their earlier success forward.
        """
        statuses: dict[str, str] = {}
        parent_id: UUID | None = retry_of
        with Session(self._store.engine) as session:
            while parent_id:
                for row in self._store.list_asset_executions(parent_id):
                    # Closest ancestor wins: only record a key the first time we see it.
                    if row.asset_key:
                        statuses.setdefault(row.asset_key, row.status)
                parent = session.get(Run, parent_id)
                parent_id = parent.retry_of if parent else None

        success_keys = {key for key, status in statuses.items() if status == "success"}
        for asset in assets:
            if type(asset).key in success_keys:
                asset.materializable = False

    def _resolve_upstream_deps(self, assets: list[il.Asset]) -> None:
        """Add transitive upstream deps to *assets* as non-materializable.

        Hydrated assets carry their row id, so the dependency relations are
        walked directly from the ids already in hand.
        """
        visited = {UUID(asset.id) for asset in assets}
        frontier = list(visited)
        with Session(self._store.engine) as session:
            while frontier:
                dependencies = session.exec(
                    select(ComponentRelation).where(
                        col(ComponentRelation.src_id).in_(frontier),
                        ComponentRelation.type == "dependency",
                    )
                ).all()
                next_frontier: list[UUID] = []
                for relation in dependencies:
                    if relation.dst_id not in visited:
                        visited.add(relation.dst_id)
                        next_frontier.append(relation.dst_id)
                        upstream_asset = cast(il.Asset, self._store.load(relation.dst_id))
                        upstream_asset.materializable = False
                        assets.append(upstream_asset)
                frontier = next_frontier

    def _run_dag(
        self,
        dag: il.DAG,
        partition: il.TimePartition | None,
        *,
        org_id: UUID,
        run_id: UUID,
        backfill_id: str | None,
    ) -> il.RunResult:
        def handle_event(event: il.Event) -> None:
            self._store.save_event(event, org_id=org_id, run_id=run_id)

        # A fresh copy per execution: the runner template is shared across
        # runs, but run state and the event handler are per-run.
        runner = self._runner.model_copy(update={"on_event": handle_event})
        return asyncio.run(
            runner.run(
                dag,
                partition,
                metadata={
                    "run_id": str(run_id),
                    "backfill_id": backfill_id,
                },
            )
        )


def _target_assets(component: il.Component) -> list[il.Asset]:
    """Flatten a run's hydrated target component into the assets to materialize.

    A job contributes its targets' assets, a source its own assets, and an
    asset itself — any runnable component resolves to a DAG-able list.
    """
    if isinstance(component, il.Job):
        assets: list[il.Asset] = []
        for target in component.targets:
            assets.extend(target.assets if isinstance(target, il.Source) else [target])
        return assets
    if isinstance(component, il.Source):
        return list(component.assets)
    if isinstance(component, il.Asset):
        return [component]
    raise ValueError(f"Component kind '{type(component).kind}' is not runnable")
