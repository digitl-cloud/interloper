"""Run executor: loads a run from DB, builds the DAG, and executes it."""

from __future__ import annotations

import datetime as dt
import logging
from typing import Any
from uuid import UUID

import interloper as il
from interloper.runner import ExecutionStatus
from interloper.runner.sync_runner import SyncRunner
from interloper_db import Store, get_engine
from interloper_db.models import AssetDependency, Job, Run, Source
from sqlalchemy.orm import selectinload
from sqlmodel import Session, col, select

logger = logging.getLogger(__name__)


# TODO: cache source and asset hydrations


class RunExecutor:
    """Executes a run: loads from DB, builds the DAG, runs it, tracks events.

    Uses the ``Store`` for hydration so all reconstruction goes through
    the standard framework path.
    """

    def __init__(
        self,
        store: Store | None = None,
        runner_type: type[SyncRunner] = il.MultiThreadRunner,
        runner_kwargs: dict[str, Any] | None = None,
    ) -> None:
        if store is None:
            from interloper.catalog import Catalog

            store = Store.from_settings(catalog=Catalog.from_settings())
        self._store = store
        self._runner_type = runner_type
        self._runner_kwargs = runner_kwargs or {}

    def execute(self, run_id: UUID) -> bool:
        """Execute a run with full lifecycle tracking.

        Returns:
            ``True`` if the run completed successfully, ``False`` otherwise.
        """
        org_id: UUID | None = None
        backfill_id: str | None = None

        try:
            logger.info("Starting run %s", run_id)

            with Session(get_engine()) as session:
                db_run = self._load_run(session, run_id)
                if not db_run or not db_run.job:
                    logger.info("Run %s not found, skipping", run_id)
                    return False

                org_id = db_run.org_id
                backfill_id = str(db_run.backfill_id) if db_run.backfill_id else None

                self._mark_running(session, db_run)

                assets = self._hydrate_job_assets(db_run.job)
                if not assets:
                    logger.info("No sources or assets for run %s, marking success", run_id)
                    self._store.complete_run(run_id, success=True)
                    return True

                self._resolve_upstream_deps(db_run.job, assets)

                dag = il.DAG(*assets)
                partition = il.TimePartition(db_run.partition_date) if db_run.partition_date else None

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

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _load_run(session: Session, run_id: UUID) -> Run | None:
        return session.get(
            Run,
            run_id,
            options=[
                selectinload(Run.job).selectinload(Job.sources).selectinload(Source.assets),  # type: ignore[arg-type]
                selectinload(Run.job).selectinload(Job.assets),  # type: ignore[arg-type]
            ],
        )

    @staticmethod
    def _mark_running(session: Session, db_run: Run) -> None:
        db_run.status = "running"
        db_run.started_at = dt.datetime.now(dt.timezone.utc)
        session.add(db_run)
        session.commit()

    def _hydrate_job_assets(self, db_job: Job) -> list[il.Asset]:
        """Hydrate job sources/assets and return only DB-registered assets."""
        assets: list[il.Asset] = []

        # Source-owned: hydrate the full source, then cherry-pick registered assets.
        for db_source in db_job.sources:
            assert db_source.id is not None
            source = self._store.load_source(db_source.id)
            registered_keys = {db_asset.key for db_asset in db_source.assets}
            for asset in source.assets:
                if type(asset).key in registered_keys:
                    assets.append(asset)

        # Standalone assets
        for db_asset in db_job.assets:
            assert db_asset.id is not None
            assets.append(self._store.load_asset(db_asset.id))

        return assets

    def _resolve_upstream_deps(self, db_job: Job, assets: list[il.Asset]) -> None:
        """Add transitive upstream deps to *assets* as non-materializable."""
        db_asset_ids: set[UUID] = set()
        for db_source in db_job.sources:
            for db_asset in db_source.assets:
                assert db_asset.id is not None
                db_asset_ids.add(db_asset.id)
        for db_asset in db_job.assets:
            assert db_asset.id is not None
            db_asset_ids.add(db_asset.id)

        frontier = list(db_asset_ids)
        visited = set(db_asset_ids)
        with Session(get_engine()) as session:
            while frontier:
                dependency_rows = session.exec(
                    select(AssetDependency).where(col(AssetDependency.asset_id).in_(frontier))
                ).all()
                next_frontier: list[UUID] = []
                for dependency in dependency_rows:
                    if dependency.upstream_asset_id not in visited:
                        visited.add(dependency.upstream_asset_id)
                        next_frontier.append(dependency.upstream_asset_id)
                        upstream_asset = self._store.load_asset(dependency.upstream_asset_id)
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
            self._store.save_event(event, org_id=org_id, run_id=run_id)  # type: ignore[arg-type]

        with self._runner_type(
            **self._runner_kwargs,
            on_event=handle_event,
        ) as runner:
            return runner.run(
                dag,
                partition,
                metadata={
                    "run_id": str(run_id),
                    "backfill_id": backfill_id,
                },
            )
