"""Health: what is failing, what needs a person, and what each job will do next.

One definition each:

- A job is *failing* when it is enabled and its latest attempt failed.
- An asset is *failing* when its latest execution failed, and its source with
  it; a hook when its last firing, within :data:`HOOK_FAILURE_HORIZON`, failed.
- A job is *overdue* when it is enabled and its stored next slot passed more
  than :data:`OVERDUE_AFTER` ago, so the scheduler did not fire it.
- *Attention* is anything a person has to act on: an error group, a connection
  whose renewal failed, a stack still failing after retries, a drifted
  component, an overdue job.
"""

from __future__ import annotations

import datetime as dt
from collections import Counter
from dataclasses import dataclass
from typing import Literal
from uuid import UUID

from interloper.partitioning.time import TimePartitionWindow

from interloper_db.models import Component, Run, RunStatus
from interloper_db.store.components import ComponentStatus
from interloper_db.store.insights.failures import ErrorGroup

OVERDUE_AFTER = dt.timedelta(minutes=15)
HOOK_FAILURE_HORIZON = dt.timedelta(days=30)
INVENTORY_KINDS = ("source", "asset", "destination", "connection", "job", "hook")

AttentionKind = Literal["error_group", "renewal_error", "still_failing", "drift", "overdue"]


@dataclass(frozen=True)
class JobHealth:
    """One job's state and its next firing.

    Attributes:
        job: The job row.
        failing: Whether it is enabled and its latest attempt failed.
        latest_run: Its most recent attempt, or ``None`` when it never ran.
        last_success_at: When it last succeeded, or ``None`` when it never did.
        next_run_at: Its stored next slot, or ``None`` before the scheduler
            first saw it.
        overdue: Whether it is enabled and its next slot passed more than
            :data:`OVERDUE_AFTER` ago.
        window: The partitions its next firing covers, or ``None`` for an
            unpartitioned job or one with no next slot.
    """

    job: Component
    failing: bool
    latest_run: Run | None
    last_success_at: dt.datetime | None
    next_run_at: dt.datetime | None
    overdue: bool
    window: TimePartitionWindow | None


@dataclass(frozen=True)
class Attention:
    """One thing that needs a person.

    Attributes:
        kind: What it is.
        since: When it started or was last seen.
        component: The component it concerns, when one does.
        run_id: The run to open, when one does.
        error_group: The group, for an ``error_group``.
        status: The catalog status, for a ``drift``.
        detail: The first line of the renewal error, for a ``renewal_error``.
        attempt: The attempt the stack reached, for a ``still_failing``.
        partition_key: The stack's partition, for a ``still_failing``.
    """

    kind: AttentionKind
    since: dt.datetime | None
    component: Component | None = None
    run_id: UUID | None = None
    error_group: ErrorGroup | None = None
    status: ComponentStatus | None = None
    detail: str | None = None
    attempt: int | None = None
    partition_key: str | None = None

    @classmethod
    def from_error_groups(cls, groups: list[ErrorGroup], jobs: dict[UUID, Component]) -> list[Attention]:
        """Flag each error group that holds a final failure.

        Args:
            groups: The window's error groups.
            jobs: The job rows by id; a group whose runs target anything else
                (an ad-hoc asset run, a deleted target) names no job.

        Returns:
            One item per group with a terminal failure, in the groups' order.
        """
        return [
            cls(
                "error_group",
                group.last_seen,
                component=jobs.get(group.job_id) if group.job_id else None,
                run_id=group.sample_run_id,
                error_group=group,
            )
            for group in groups
            if group.terminal_failures
        ]

    @classmethod
    def from_connections(cls, connections: list[Component]) -> list[Attention]:
        """Flag the connections whose last renewal failed.

        Args:
            connections: The organisation's connection rows.

        Returns:
            One item per connection carrying ``state.last_renewal_error``.
        """
        return [
            cls(
                "renewal_error",
                connection.state_datetime("last_renewed_at"),
                component=connection,
                detail=error.splitlines()[0],
            )
            for connection in connections
            if (error := connection.state_text("last_renewal_error"))
        ]

    @classmethod
    def from_stacks(
        cls, failed: list[Run], jobs: dict[UUID, Component], since: dt.datetime, now: dt.datetime
    ) -> list[Attention]:
        """Flag the stacks whose latest attempt is a retry that still failed.

        Every stack counts, not only each job's newest one, so a partition
        still failing after retries stays listed once a later run starts.

        Args:
            failed: The latest attempt of each job stack whose latest attempt
                failed, whether or not that attempt ever started.
            jobs: The job rows by id.
            since: Only stacks that finished at or after this instant count.
            now: Only stacks that finished by this instant count.

        Returns:
            One item per stack.
        """
        items = []
        for run in failed:
            if run.status != RunStatus.FAILED or run.attempt <= 1 or run.completed_at is None:
                continue
            finished = run.completed_at
            if since <= finished <= now:
                job = jobs.get(run.component_id) if run.component_id else None
                items.append(
                    cls(
                        "still_failing",
                        finished,
                        component=job,
                        run_id=run.id,
                        attempt=run.attempt,
                        partition_key=run.partition_key,
                    )
                )
        return items

    @classmethod
    def from_drift(cls, statuses: list[tuple[Component, ComponentStatus]]) -> list[Attention]:
        """Flag the components whose catalog status is not ``ok``.

        An owned asset whose source is itself drifted is left out: it drifts
        because its source does, and the source's one item covers it.

        Args:
            statuses: Every component row with its status.

        Returns:
            One item per drifted component whose parent, if any, is not drifted.
        """
        drifted = {component.id for component, status in statuses if status is not ComponentStatus.OK}
        return [
            cls("drift", None, component=component, status=status)
            for component, status in statuses
            if status is not ComponentStatus.OK and component.parent_id not in drifted
        ]

    @classmethod
    def from_overdue(cls, jobs: list[JobHealth]) -> list[Attention]:
        """Flag the overdue jobs.

        Args:
            jobs: Every job's health.

        Returns:
            One item per overdue job, since its missed slot.
        """
        return [cls("overdue", job.next_run_at, component=job.job) for job in jobs if job.overdue]


@dataclass(frozen=True)
class KindInventory:
    """How many components of one kind sit in each state.

    Attributes:
        kind: The component kind.
        total: Every component of the kind.
        healthy: Live, enabled and neither failing nor needing attention.
        failing: Enabled and failing.
        attention: Enabled, not failing, but drifted or needing attention.
        disabled: Disabled.
    """

    kind: str
    total: int
    healthy: int
    failing: int
    attention: int
    disabled: int

    @classmethod
    def from_statuses(
        cls, statuses: list[tuple[Component, ComponentStatus]], failing: set[UUID], attention: set[UUID]
    ) -> list[KindInventory]:
        """Count each kind's components by state, first match winning: disabled, failing, attention, healthy.

        Args:
            statuses: Every component row with its status.
            failing: The failing components.
            attention: Components needing attention for a reason other than drift.

        Returns:
            One row per kind in :data:`INVENTORY_KINDS`, in that order.
        """
        counts: dict[str, Counter[str]] = {kind: Counter() for kind in INVENTORY_KINDS}
        for component, status in statuses:
            if component.kind not in counts:
                continue
            if not component.enabled:
                state = "disabled"
            elif component.id in failing:
                state = "failing"
            elif status is not ComponentStatus.OK or component.id in attention:
                state = "attention"
            else:
                state = "healthy"
            counts[component.kind][state] += 1
        return [
            cls(kind, tally.total(), tally["healthy"], tally["failing"], tally["attention"], tally["disabled"])
            for kind, tally in counts.items()
        ]


@dataclass(frozen=True)
class OrgHealth:
    """An organisation's health at one instant.

    Attributes:
        jobs: Every job's health.
        failing: The failing components: jobs, assets, the sources of failing
            assets, and hooks.
        attention: What needs a person, unsorted.
        inventory: Each kind's components by state.
    """

    jobs: list[JobHealth]
    failing: set[UUID]
    attention: list[Attention]
    inventory: list[KindInventory]
