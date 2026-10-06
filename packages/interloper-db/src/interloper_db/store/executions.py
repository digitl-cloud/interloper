"""Execution reads: one row per operation of a run, folded from its events.

Each row is an operation's verdict in one run, its timestamps spanning every
attempt. A trigger on ``events`` keeps the table current as events are
saved, so reading it costs what the rows read cost, whatever the history.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from uuid import UUID

from sqlalchemy import Engine, and_
from sqlmodel import col, func, select

from interloper_db.models import Component, Execution
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
                the window to read. Within one run every component executes
                once, so the newest is already all there is.
            run_id: Keep the executions of this run; ``None`` keeps every run's.

        Returns:
            The page of execution rows.
        """
        if query.latest and run_id is None:
            newest = and_(
                col(Execution.component_id) == col(Component.id),
                col(Execution.run_id) == self._newest_run(org_id),
            )
            statement = select(Execution).select_from(Component).join(Execution, newest)
            statement = statement.where(Component.org_id == org_id)
        else:
            statement = select(Execution).where(Execution.org_id == org_id)
            if run_id is not None:
                statement = statement.where(Execution.run_id == run_id)
        order = (col(Execution.created_at), col(Execution.component_id))
        with session_scope(self._engine) as session:
            return Page.read(session, statement.order_by(*order), query)

    @staticmethod
    def _newest_run(org_id: UUID) -> Any:
        """The run of a component's newest execution, correlated to the component being read.

        One index probe per component the listing reads, so the cost follows
        the organisation's components rather than its history; a component
        that was deleted no longer has a newest execution.

        Args:
            org_id: Organisation UUID.

        Returns:
            The correlated scalar subquery.
        """
        return (
            select(Execution.run_id)
            .where(Execution.org_id == org_id, col(Execution.component_id) == col(Component.id))
            .order_by(col(Execution.created_at).desc())
            .limit(1)
            .correlate(Component)
            .scalar_subquery()
        )

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
