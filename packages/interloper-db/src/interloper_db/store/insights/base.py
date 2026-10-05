"""The insights facet: derived reads over runs, events and components.

Every answer the app's overview and the agent's analytics give comes from
here, so each concept has one definition (see :mod:`.health`,
:mod:`.outcomes`, :mod:`.failures`, :mod:`.coverage`). The facet returns facts;
wording them for a reader is its consumers' job. The aggregate queries behind
the facts live here too, so a later optimisation of any of them has one place
to land.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Sequence
from typing import Any
from uuid import UUID

from interloper.catalog.base import Catalog
from interloper.errors import ConfigError, NotFoundError
from interloper.job.cron import CronJob
from interloper.partitioning.time import TimeGranularity, TimePartition
from sqlalchemy import Engine, String, case, cast
from sqlalchemy import select as sa_select
from sqlmodel import col, func, select

from interloper_db.models import (
    ACTIVE_BACKFILL_STATUSES,
    Component,
    Event,
    Execution,
    Invitation,
    Organisation,
    Profile,
    Run,
    RunStatus,
    UserOrganisation,
)
from interloper_db.session import session_scope
from interloper_db.store.backfills import BackfillQuery, BackfillStore
from interloper_db.store.components import ComponentQuery, ComponentStore
from interloper_db.store.executions import ExecutionQuery, ExecutionStore
from interloper_db.store.insights.coverage import AssetEvidence, CoverageGroup, CoverageRow, JobCoverage
from interloper_db.store.insights.failures import GROUP_KEYS, ErrorGroup, ErrorGroups, ErrorRow
from interloper_db.store.insights.feed import ActivityEntry
from interloper_db.store.insights.health import (
    HOOK_FAILURE_HORIZON,
    OVERDUE_AFTER,
    Attention,
    JobHealth,
    KindInventory,
    OrgHealth,
)
from interloper_db.store.insights.outcomes import Activity, JobOutcome
from interloper_db.store.page import Page, PageQuery
from interloper_db.store.runs import RunQuery, RunStore, partition_key_range

# Every attempt that failed, retried ones included; step-level failures
# (dest_write_failed and the like) repeat their operation's own verdict.
ERROR_EVENT_TYPES = ("operation_retried", "operation_failed", "run_failed")
MAX_ERROR_ROWS = 20_000


class InsightStore:
    """Store methods for the reads derived from runs, events and components."""

    def __init__(
        self,
        engine: Engine,
        catalog: Catalog,
        components: ComponentStore,
        runs: RunStore,
        backfills: BackfillStore,
        executions: ExecutionStore,
    ) -> None:
        """Bind the facet to what it works through.

        Args:
            engine: Engine the facet opens its sessions on.
            catalog: Catalog the assets' partitioning resolves against.
            components: Component facet, for the rows and their statuses.
            runs: Run facet, for the run listings the facts read.
            backfills: Backfill facet, for backfill progress.
            executions: Execution facet, for each component's latest execution.
        """
        self._engine = engine
        self._catalog = catalog
        self._components = components
        self._runs = runs
        self._backfills = backfills
        self._executions = executions

    # -- Public API ------------------------------------------------------------

    def health(self, org_id: UUID, *, now: dt.datetime) -> OrgHealth:
        """An organisation's health at *now*: its jobs, what is failing, what needs a person.

        Error groups and stacks still failing after retries are read over the
        24 hours before *now*.

        Args:
            org_id: Organisation UUID.
            now: The reference instant, aware UTC.

        Returns:
            The organisation's health.
        """
        day_ago = now - dt.timedelta(hours=24)
        components = self._components.list(org_id, ComponentQuery(roots_only=False, limit=None)).items
        statuses = [(component, self._components.read(component).status) for component in components]
        job_rows = [component for component in components if component.kind == "job"]
        jobs = self._job_health(org_id, job_rows, now)
        job_by_id = {job.id: job for job in job_rows}

        failing_assets = {
            execution.component_id
            for execution in self._executions.list(org_id, ExecutionQuery(latest=True, limit=None)).items
            if execution.status == "failed"
        }
        failing = {job.job.id for job in jobs if job.failing} | failing_assets
        for component in components:
            if component.kind == "asset" and component.id in failing_assets and component.parent_id:
                failing.add(component.parent_id)
            if (
                component.kind == "hook"
                and component.state_text("last_error")
                and (fired_at := component.state_datetime("last_fired_at"))
                and fired_at >= now - HOOK_FAILURE_HORIZON
            ):
                failing.add(component.id)

        failed = RunQuery(
            status=[RunStatus.FAILED], component_kind="job", completed_after=day_ago, completed_before=now, limit=None
        )
        failed_stacks = self._runs.list(org_id, failed).items
        errors = self.error_groups(org_id, since=day_ago, until=now, group_by=("job", "cause"))
        connections = [component for component in components if component.kind == "connection"]
        attention = [
            *Attention.from_error_groups(errors.groups, job_by_id),
            *Attention.from_connections(connections),
            *Attention.from_stacks(failed_stacks, job_by_id, day_ago, now),
            *Attention.from_drift(statuses),
            *Attention.from_overdue(jobs),
        ]
        renewals = {item.component.id for item in attention if item.kind == "renewal_error" and item.component}
        return OrgHealth(
            jobs=jobs,
            failing=failing,
            attention=attention,
            inventory=KindInventory.from_statuses(statuses, failing, renewals),
        )

    def activity(self, org_id: UUID, *, now: dt.datetime) -> Activity:
        """The last 24 clock hours of attempts, what is executing now, and backfill progress.

        Args:
            org_id: Organisation UUID.
            now: The reference instant, aware UTC.

        Returns:
            The activity.
        """
        day_ago = now - dt.timedelta(hours=24)
        backfills = self._backfills.list(org_id, BackfillQuery(status=[*ACTIVE_BACKFILL_STATUSES], limit=None)).items
        return Activity.from_runs(
            completed=self._runs.list(
                org_id, RunQuery(completed_after=day_ago, completed_before=now, all_attempts=True, limit=None)
            ).items,
            running=self._runs.list(org_id, RunQuery(status=[RunStatus.RUNNING], limit=None)).items,
            queued=self._runs.list(org_id, RunQuery(status=[RunStatus.QUEUED], limit=1)).total,
            backfills=backfills,
            backfill_counts=self._backfills.run_counts([backfill.id for backfill in backfills]),
            now=now,
        )

    def outcomes(
        self, org_id: UUID, *, since: dt.datetime | None, until: dt.datetime | None = None, job_id: UUID | None = None
    ) -> list[JobOutcome]:
        """Each job's run outcomes over the runs that executed inside a window.

        Args:
            org_id: Organisation UUID.
            since: The window's start; ``None`` leaves it open in the past.
            until: The window's end; ``None`` leaves it open.
            job_id: Keep this job alone; ``None`` keeps every job.

        Returns:
            One outcome per job that ran, the most failed stacks first.
        """
        query = RunQuery(component_id=job_id, after=since, before=until, all_attempts=True, limit=None)
        return JobOutcome.from_runs(self._runs.list(org_id, query).items)

    def error_groups(
        self,
        org_id: UUID,
        *,
        since: dt.datetime | None = None,
        until: dt.datetime | None = None,
        job_id: UUID | None = None,
        backfill_id: UUID | None = None,
        run_id: UUID | None = None,
        group_by: Sequence[str] = GROUP_KEYS,
    ) -> ErrorGroups:
        """Group an organisation's failed attempts by job, asset and classified cause.

        Identical texts collapse in the database, so each distinct text is
        classified once per run; the loudest groups come first, so a capped
        read keeps the loudest errors.

        Args:
            org_id: Organisation UUID.
            since: Keep failures at or after this instant.
            until: Keep failures before this instant.
            job_id: Keep failures of runs targeting this component.
            backfill_id: Keep failures of this backfill's runs.
            run_id: Keep failures of this run.
            group_by: Any of ``job``, ``asset``, ``cause``.

        Returns:
            The groups, and whether the read hit its cap.

        Raises:
            ConfigError: If *group_by* names an unknown key.
        """
        if unknown := set(group_by) - set(GROUP_KEYS):
            raise ConfigError(f"Unknown group_by key(s): {sorted(unknown)}; expected any of {list(GROUP_KEYS)}")
        rows = self._error_rows(org_id, since=since, until=until, job_id=job_id, backfill_id=backfill_id, run_id=run_id)
        kept = rows[:MAX_ERROR_ROWS]
        return ErrorGroups(ErrorGroup.merge(kept, group_by), rows=len(kept), truncated=len(rows) > MAX_ERROR_ROWS)

    def coverage_by_day(self, org_id: UUID, *, since: dt.date, until: dt.date, now: dt.datetime) -> list[CoverageGroup]:
        """Per source and day, the asset-partitions owed, covered and failed over a window.

        Args:
            org_id: Organisation UUID.
            since: First day of the window.
            until: Last day of the window, inclusive.
            now: The reference instant, aware UTC: it decides which periods
                have closed and how much of today is owed.

        Returns:
            The groups with a day expected in the window, by name.
        """
        query = ComponentQuery(kind=["source", "asset", "job"], roots_only=False, limit=None)
        assets = AssetEvidence.from_components(
            self._components.list(org_id, query).items,
            self._components.asset_partitionings(org_id),
            self._coverage_rows(org_id),
        )
        return CoverageGroup.from_assets(assets, since, until, now)

    def coverage_by_key(self, org_id: UUID, job_id: UUID, *, start_key: str, end_key: str) -> JobCoverage:
        """The coverage of a job's target assets over a range of partition keys.

        Args:
            org_id: Organisation UUID.
            job_id: The job, of the organisation.
            start_key: First partition key of the range.
            end_key: Last partition key, inclusive.

        Returns:
            Every target asset with its covered and failed keys.

        Raises:
            ConfigError: If the keys are of different granularities.
        """
        job = self._components.get(job_id, org_id=org_id, kind="job")
        first, last = TimePartition.from_key(start_key), TimePartition.from_key(end_key)
        if first.granularity is not last.granularity:
            raise ConfigError(f"{start_key!r} and {end_key!r} are keys of different granularities")
        keys = [first.granularity.format(value) for value in first.granularity.period_range(first.value, last.value)]
        assets = self._components.target_assets(job)
        rows = self._coverage_rows(org_id, asset_ids=list(assets), start_key=start_key, end_key=end_key)
        return JobCoverage.from_rows(job.id, keys, assets, rows)

    def feed(self, org_id: UUID, query: PageQuery) -> Page[ActivityEntry]:
        """A derived activity feed for one organisation, newest first.

        Composed purely from existing records: the organisation row,
        memberships, pending invitations, source components, and daily
        successful-run aggregates. There is no audit table, so events whose
        source rows are gone (accepted invitations' inviters, quota-change
        history) are not reconstructible and deliberately absent.

        Args:
            org_id: Organisation UUID; a soft-deleted organisation still has
                its feed, ending in its deletion.
            query: The window to read.

        Returns:
            The page of entries.

        Raises:
            NotFoundError: If the organisation is not found.
        """
        entries: list[ActivityEntry] = []
        with session_scope(self._engine) as session:
            organisation = session.get(Organisation, org_id)
            if not organisation:
                raise NotFoundError(f"Organisation {org_id} not found")
            if organisation.created_at:
                entries.append(ActivityEntry("org_created", organisation.created_at))
            if organisation.deleted_at:
                entries.append(ActivityEntry("org_deleted", organisation.deleted_at))
            memberships = session.exec(
                select(UserOrganisation, Profile).where(
                    UserOrganisation.organisation_id == org_id, col(Profile.id) == UserOrganisation.user_id
                )
            ).all()
            for membership, profile in memberships:
                if membership.created_at:
                    subject = profile.name or profile.email
                    entries.append(ActivityEntry("member_joined", membership.created_at, subject, membership.role))
            invitations = session.exec(select(Invitation).where(Invitation.organisation_id == org_id)).all()
            inviter_ids = {invitation.invited_by for invitation in invitations}
            inviters = {
                profile.id: profile
                for profile in session.exec(select(Profile).where(col(Profile.id).in_(inviter_ids))).all()
            }
            for invitation in invitations:
                if invitation.created_at:
                    inviter = inviters.get(invitation.invited_by)
                    entries.append(
                        ActivityEntry(
                            "invitation_sent",
                            invitation.created_at,
                            invitation.email,
                            (inviter.name or inviter.email) if inviter else None,
                        )
                    )
            sources = session.exec(
                select(Component).where(col(Component.org_id) == org_id, col(Component.kind) == "source")
            ).all()
            for source in sources:
                if source.created_at:
                    entries.append(ActivityEntry("source_added", source.created_at, source.name or source.key))
            # func.date() buckets per calendar day on both Postgres and SQLite.
            day = func.date(col(Run.completed_at)).label("day")
            run_days = session.exec(
                select(day, func.count(), func.max(col(Run.completed_at)))
                .where(col(Run.org_id) == org_id, col(Run.status) == RunStatus.SUCCESS)
                .group_by(day)
            ).all()
            for _day, count, latest in run_days:
                if latest is not None:
                    entries.append(ActivityEntry("runs_completed", latest, str(count)))
        entries.sort(key=lambda entry: entry.when, reverse=True)
        return Page.window(entries, query)

    # -- Internals -------------------------------------------------------------

    def _job_health(self, org_id: UUID, jobs: list[Component], now: dt.datetime) -> list[JobHealth]:
        """Each job's state and next firing.

        Args:
            org_id: Organisation UUID.
            jobs: The organisation's job rows.
            now: The reference instant, aware UTC.

        Returns:
            One entry per job, in the order of *jobs*.
        """
        latest = {run.component_id: run for run in self._runs.latest_by_target(org_id, kind="job")}
        last_success = self._runs.last_successes(org_id)
        granularities = self._components.job_partition_granularities([job.id for job in jobs if job.enabled])
        health = []
        for job in jobs:
            next_run = job.state_datetime("next_run_at")
            run = latest.get(job.id)
            window = None
            shared = granularities.get(job.id, set())
            granularity = next(iter(shared)) if len(shared) == 1 else None
            if job.enabled and next_run is not None:
                try:
                    window = CronJob.window(job.config or {}, fires_at=next_run, granularity=granularity)
                except ValueError:
                    window = None
            health.append(
                JobHealth(
                    job=job,
                    failing=job.enabled and run is not None and run.status == RunStatus.FAILED,
                    latest_run=run,
                    last_success_at=last_success.get(job.id),
                    next_run_at=next_run,
                    overdue=job.enabled and next_run is not None and now - next_run > OVERDUE_AFTER,
                    window=window,
                )
            )
        return health

    def _error_rows(
        self,
        org_id: UUID,
        *,
        since: dt.datetime | None,
        until: dt.datetime | None,
        job_id: UUID | None,
        backfill_id: UUID | None,
        run_id: UUID | None,
    ) -> list[ErrorRow]:
        """Failed-attempt events grouped by job, run, component, type and text, one past the cap.

        Args:
            org_id: Organisation UUID.
            since: Keep events at or after this instant.
            until: Keep events before this instant.
            job_id: Keep events of runs targeting this component.
            backfill_id: Keep events of this backfill's runs.
            run_id: Keep events of this run.

        Returns:
            The rows, the largest first, at most one past :data:`MAX_ERROR_ROWS`.
        """
        filters: list[Any] = [
            Event.org_id == org_id,
            col(Event.error).is_not(None),
            col(Event.event_type).in_(ERROR_EVENT_TYPES),
        ]
        if since is not None:
            filters.append(col(Event.timestamp) >= since)
        if until is not None:
            filters.append(col(Event.timestamp) < until)
        if job_id is not None:
            filters.append(Run.component_id == job_id)
        if backfill_id is not None:
            filters.append(Run.backfill_id == backfill_id)
        if run_id is not None:
            filters.append(Event.run_id == run_id)
        count = func.count().label("count")
        grouped = (col(Run.component_id), col(Event.run_id), col(Event.component_key), col(Event.event_type))
        statement = (
            sa_select(
                *grouped,
                col(Event.error),
                count,
                func.min(col(Event.timestamp)),
                func.max(col(Event.timestamp)),
            )
            .join(Run, col(Run.id) == col(Event.run_id))
            .where(*filters)
            .group_by(*grouped, col(Event.error))
            .order_by(count.desc(), func.max(col(Event.timestamp)).desc())
            .limit(MAX_ERROR_ROWS + 1)
        )
        with session_scope(self._engine) as session:
            return [ErrorRow(*row) for row in session.execute(statement).all()]  # ty: ignore[deprecated]

    def _coverage_rows(
        self,
        org_id: UUID,
        *,
        asset_ids: Sequence[UUID] | None = None,
        start_key: str | None = None,
        end_key: str | None = None,
    ) -> list[CoverageRow]:
        """Per asset and time partition, whether any execution of it succeeded or failed.

        Runs of every target count. Without a key range every granularity and
        every period is read: the calendar derives both each asset's attempted
        span and a window's days from these rows, because any read of the
        executions view scans the organisation's operation events whole, so
        one all-time read costs less than a bounds read plus a windowed one.

        Args:
            org_id: Organisation UUID.
            asset_ids: Keep these assets; ``None`` keeps every asset.
            start_key: With *end_key*, keep the keys of that inclusive range.
            end_key: Last key of the range.

        Returns:
            One row per asset and partition key that executed at least once.
        """
        key = col(Run.partition_key)
        if start_key is not None and end_key is not None:
            keys: list[Any] = partition_key_range(start_key, end_key)
        else:
            key_lengths = [
                len(granularity.format(dt.datetime(2000, 1, 1)))
                for granularity in TimeGranularity
                if granularity.key_format is not None
            ]
            keys = [key.is_not(None), func.length(key).in_(key_lengths)]
        if asset_ids is not None:
            keys.append(col(Execution.component_id).in_(asset_ids))
        # Cast for a portable max(): Postgres has no max(uuid), and UUID() parses both its dashed text and SQLite's hex.
        failed_run = func.max(case((col(Execution.status) == "failed", cast(col(Run.id), String))))
        # The asset id comes back as text and is parsed once per asset: a UUID per row costs more than the roll-up.
        asset = cast(col(Execution.component_id), String)
        statement = (
            sa_select(asset, key, func.max(case((col(Execution.status) == "success", 1), else_=0)), failed_run)
            .join(Run, col(Run.id) == col(Execution.run_id))
            .where(col(Execution.org_id) == org_id, col(Run.org_id) == org_id, *keys)
            .group_by(col(Execution.component_id), key)
        )
        parsed: dict[str, UUID] = {}
        with session_scope(self._engine) as session:
            return [
                CoverageRow(
                    parsed.get(asset_text) or parsed.setdefault(asset_text, UUID(asset_text)),
                    partition_key,
                    succeeded=bool(succeeded),
                    failed=failed is not None,
                    failed_run_id=UUID(failed) if failed else None,
                )
                for asset_text, partition_key, succeeded, failed in session.execute(statement).all()  # ty: ignore[deprecated]
            ]
