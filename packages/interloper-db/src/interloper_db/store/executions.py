"""Execution reads: one row per operation of a run, derived from its events.

``executions`` is a database view over the run events, never written: each
row is an operation's verdict in one run, its timestamps spanning every
attempt. Every read here goes through the view, so the cost of deriving it is
paid in one place.
"""

from __future__ import annotations

import builtins
import datetime as dt
from collections.abc import Sequence
from typing import NamedTuple
from uuid import UUID

from interloper.partitioning import TimeGranularity
from sqlalchemy import Engine, String, case, cast
from sqlalchemy import select as sa_select
from sqlalchemy.orm import aliased
from sqlmodel import col, func, select

from interloper_db.models import Execution, Run
from interloper_db.session import session_scope
from interloper_db.store.page import Page, PageQuery
from interloper_db.store.runs import partition_key_range


class PartitionExecution(NamedTuple):
    """Whether one asset ever succeeded for one partition of a job.

    Attributes:
        partition_key: The partition.
        component_id: The asset.
        component_key: The asset's key.
        succeeded: Whether any of its executions for that partition succeeded.
    """

    partition_key: str
    component_id: UUID
    component_key: str | None
    succeeded: bool


class CoverageRow(NamedTuple):
    """Whether one asset ever succeeded or failed for one time partition, from runs of any target.

    Attributes:
        asset_id: The asset.
        partition_key: The partition, in its own granularity's key format.
        succeeded: Whether any execution of the asset for that partition succeeded.
        failed: Whether any execution of the asset for that partition failed,
            whether or not another one succeeded; an asset attempted but
            neither succeeded nor failed (in flight, canceled) is neither.
        failed_run_id: The greatest id among the runs whose execution of the
            asset failed, or ``None``.
    """

    asset_id: UUID
    partition_key: str
    succeeded: bool
    failed: bool
    failed_run_id: UUID | None


class ExecutionQuery(PageQuery):
    """Which operation executions a listing reads.

    Attributes:
        latest: Keep only each component's newest execution, across runs.
    """

    latest: bool = False


class ExecutionStore:
    """Store methods for reading operation executions."""

    def __init__(self, engine: Engine) -> None:
        """Bind the facet to what it works through.

        Args:
            engine: Engine the facet opens its sessions on.
        """
        self._engine = engine

    def list(self, org_id: UUID, query: ExecutionQuery, *, run_id: UUID | None = None) -> Page[Execution]:
        """List an organisation's operation executions, oldest first.

        Args:
            org_id: Organisation UUID.
            query: Whether to keep only each component's newest execution, and
                the window to read.
            run_id: Keep the executions of this run; ``None`` keeps every run's.

        Returns:
            The page of execution rows.
        """
        statement = select(Execution).where(Execution.org_id == org_id)
        if run_id is not None:
            statement = statement.where(Execution.run_id == run_id)
        if query.latest:
            rank = (
                func.row_number()
                .over(partition_by=col(Execution.component_id), order_by=col(Execution.created_at).desc())
                .label("rank")
            )
            ranked = statement.add_columns(rank).subquery()
            newest = aliased(Execution, ranked)
            statement = select(newest).where(ranked.c.rank == 1)
            order = (col(newest.created_at), col(newest.component_id))
        else:
            order = (col(Execution.created_at), col(Execution.component_id))
        with session_scope(self._engine) as session:
            return Page.read(session, statement.order_by(*order), query)

    def counts(self, run_ids: Sequence[UUID]) -> dict[UUID, dict[str, int]]:
        """Count each run's operation executions by status, in one query.

        Args:
            run_ids: The runs to count, typically one listing page.

        Returns:
            Per run, its execution count per status; a run with no executions
            yet is absent.
        """
        if not run_ids:
            return {}
        statement = (
            select(Execution.run_id, Execution.status, func.count())
            .where(col(Execution.run_id).in_(run_ids))
            .group_by(col(Execution.run_id), col(Execution.status))
        )
        counts: dict[UUID, dict[str, int]] = {}
        with session_scope(self._engine) as session:
            for run_id, status, count in session.exec(statement).all():
                counts.setdefault(run_id, {})[status] = count
        return counts

    def partition_coverage(
        self, org_id: UUID, job_id: UUID, start_key: str, end_key: str
    ) -> builtins.list[PartitionExecution]:
        """Whether each asset ever succeeded, per partition of a job's runs.

        Args:
            org_id: Organisation UUID.
            job_id: The job whose runs are read.
            start_key: First partition key of the range.
            end_key: Last partition key of the range (inclusive); must share
                the start key's granularity.

        Returns:
            One row per partition and asset that executed at least once.
        """
        statement = (
            select(
                col(Run.partition_key),
                col(Execution.component_id),
                func.max(col(Execution.component_key)),
                func.max(case((col(Execution.status) == "success", 1), else_=0)),
            )
            .join(Run, col(Run.id) == col(Execution.run_id))
            .where(
                Execution.org_id == org_id,
                Run.org_id == org_id,
                Run.component_id == job_id,
                *partition_key_range(start_key, end_key),
            )
            .group_by(col(Run.partition_key), col(Execution.component_id))
        )
        with session_scope(self._engine) as session:
            return [
                PartitionExecution(partition_key, component_id, component_key, bool(succeeded))
                for partition_key, component_id, component_key, succeeded in session.exec(statement).all()
                if partition_key is not None
            ]

    def coverage_rows(self, org_id: UUID) -> builtins.list[CoverageRow]:
        """Per asset and time partition, all-time, whether it ever succeeded or failed.

        Runs of every target count (a job, a source, the asset itself, a
        backfill, a deleted target): coverage is a property of the asset's
        data, not of what triggered it. Keys of every time granularity are
        read, and of every period: the caller derives both each asset's
        attempted span and a window's days from these rows, because any read
        of the executions view scans the organisation's operation events
        whole, so one all-time read costs less than a bounds read plus a
        windowed one.

        Args:
            org_id: Organisation UUID.

        Returns:
            One row per asset and time partition key that executed at least once.
        """
        key = col(Run.partition_key)
        key_lengths = [
            len(granularity.format(dt.datetime(2000, 1, 1)))
            for granularity in TimeGranularity
            if granularity.key_format is not None
        ]
        # Cast for a portable max(): Postgres has no max(uuid), and UUID() parses both its dashed text and SQLite's hex.
        failed_run = func.max(case((col(Execution.status) == "failed", cast(col(Run.id), String))))
        # The asset id comes back as text and is parsed once per asset: a UUID per row costs more than the roll-up.
        asset = cast(col(Execution.component_id), String)
        statement = (
            sa_select(asset, key, func.max(case((col(Execution.status) == "success", 1), else_=0)), failed_run)
            .join(Run, col(Run.id) == col(Execution.run_id))
            .where(
                col(Execution.org_id) == org_id,
                col(Run.org_id) == org_id,
                key.is_not(None),
                func.length(key).in_(key_lengths),
            )
            .group_by(col(Execution.component_id), key)
        )
        asset_ids: dict[str, UUID] = {}
        with session_scope(self._engine) as session:
            return [
                CoverageRow(
                    asset_ids.get(asset_text) or asset_ids.setdefault(asset_text, UUID(asset_text)),
                    partition_key,
                    succeeded=bool(succeeded),
                    failed=failed is not None,
                    failed_run_id=UUID(failed) if failed else None,
                )
                for asset_text, partition_key, succeeded, failed in session.execute(statement).all()  # ty: ignore[deprecated]
            ]
