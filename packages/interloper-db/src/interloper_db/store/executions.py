"""Execution reads: one row per operation of a run, derived from its events.

``executions`` is a database view over the run events, never written: each
row is an operation's verdict in one run, its timestamps spanning every
attempt. Every read here goes through the view, so the cost of deriving it is
paid in one place.
"""

from __future__ import annotations

from collections.abc import Sequence
from uuid import UUID

from sqlalchemy import Engine
from sqlalchemy.orm import aliased
from sqlmodel import col, func, select

from interloper_db.models import Execution
from interloper_db.session import session_scope
from interloper_db.store.page import Page, PageQuery


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
