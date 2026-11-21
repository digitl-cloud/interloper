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
from collections.abc import Collection, Iterator, Sequence
from typing import Any
from uuid import UUID

from interloper.catalog import Catalog
from interloper.errors import ConfigError
from interloper.job import CronJob
from interloper.partitioning import TimeGranularity, TimePartition
from sqlalchemy import Engine, String, case, cast
from sqlalchemy import select as sa_select
from sqlalchemy.orm import selectinload
from sqlmodel import col, func, select

from interloper_db.errors import NotFoundError
from interloper_db.models import (
    ACTIVE_BACKFILL_STATUSES,
    Component,
    ComponentRelation,
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
from interloper_db.store.components import ComponentStore
from interloper_db.store.executions import ExecutionStore
from interloper_db.store.insights.coverage import GroupCoverage, JobCoverage, PartitionResult, Period
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
from interloper_db.store.runs import RunQuery, RunStore, partition_keys_overlapping

# Every operation attempt that failed, retried ones included; step-level failures
# (dest_write_failed and the like) repeat their operation's own verdict. A run's
# own failure is on the run.
ERROR_EVENT_TYPES = ("operation_retried", "operation_failed")
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
        outlines = self._components.outlines(org_id)
        rows = self._rows(org_id, kinds=["job", "hook", "connection"])
        job_rows = [row for row in rows if row.kind == "job"]
        jobs = self._job_health(org_id, job_rows, now)
        job_by_id = {job.id: job for job in job_rows}

        execution_failed = self._executions.latest_failed(org_id)
        failing = {job.job.id for job in jobs if job.failing} | execution_failed
        for outline in outlines:
            if outline.kind == "asset" and outline.id in execution_failed and outline.parent_id:
                failing.add(outline.parent_id)
        for row in rows:
            if (
                row.kind == "hook"
                and row.state_text("last_error")
                and (fired_at := row.state_datetime("last_fired_at"))
                and fired_at >= now - HOOK_FAILURE_HORIZON
            ):
                failing.add(row.id)

        failed = RunQuery(
            status=[RunStatus.FAILED], component_kind="job", completed_after=day_ago, completed_before=now, limit=None
        )
        failed_stacks = self._runs.list(org_id, failed).items
        errors = self.error_groups(org_id, since=day_ago, until=now, group_by=("job", "cause"))
        connections = [row for row in rows if row.kind == "connection"]
        drifted = Attention.drifted(outlines)
        attention = [
            *Attention.from_error_groups(errors.groups, job_by_id),
            *Attention.from_connections(connections),
            *Attention.from_stacks(failed_stacks, job_by_id, day_ago, now),
            *Attention.from_drift(self._rows(org_id, ids=list(drifted)), drifted),
            *Attention.from_overdue(jobs),
        ]
        renewals = {item.component.id for item in attention if item.kind == "renewal_error" and item.component}
        return OrgHealth(
            jobs=jobs,
            failing=failing,
            attention=attention,
            inventory=KindInventory.from_outlines(outlines, failing, renewals),
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
        scope = {"since": since, "until": until, "job_id": job_id, "backfill_id": backfill_id, "run_id": run_id}
        rows = sorted(
            [*self._error_rows(org_id, **scope), *self._run_error_rows(org_id, **scope)],
            key=lambda row: (row.count, row.last_seen),
            reverse=True,
        )
        kept = rows[:MAX_ERROR_ROWS]
        return ErrorGroups(ErrorGroup.merge(kept, group_by), rows=len(kept), truncated=len(rows) > MAX_ERROR_ROWS)

    def coverage_by_day(self, org_id: UUID, *, since: dt.date, until: dt.date, now: dt.datetime) -> list[GroupCoverage]:
        """Per source and day, the asset-partitions successful, failed, missing and missed over a window.

        Args:
            org_id: Organisation UUID.
            since: First day of the window.
            until: Last day of the window, inclusive.
            now: The reference instant, aware UTC: it decides which periods
                have closed and how much of today counts.

        Returns:
            The groups with a day in the window, by name.
        """
        components = self._rows(org_id, kinds=["source", "asset"])
        window = partition_keys_overlapping(col(Execution.partition_key), since, min(until, now.date()))
        return GroupCoverage.from_components(
            components,
            self._targeted(org_id),
            self._components.asset_partitionings(component for component in components if component.kind == "asset"),
            self._partition_results(org_id, [window]),
            self._attempted_spans(org_id),
            since=since,
            until=until,
            now=now,
        )

    def coverage_by_key(
        self, org_id: UUID, job_id: UUID, *, start_key: str, end_key: str, now: dt.datetime
    ) -> JobCoverage:
        """The coverage of a job's target assets over a range of partition keys.

        The results read are every key overlapping the range's days, so a
        snapshot target's observations of another granularity are read too.

        Args:
            org_id: Organisation UUID.
            job_id: The job, of the organisation.
            start_key: First partition key of the range.
            end_key: Last partition key, inclusive.
            now: The reference instant, aware UTC: it decides which snapshot
                periods have ended.

        Returns:
            Every target asset with its successful, failed, missing and missed keys.

        Raises:
            ConfigError: If the keys are of different granularities.
        """
        job = self._components.get(job_id, org_id=org_id, kind="job")
        first, last = TimePartition.from_key(start_key), TimePartition.from_key(end_key)
        if first.granularity is not last.granularity:
            raise ConfigError(f"{start_key!r} and {end_key!r} are keys of different granularities")
        keys = [first.granularity.format(value) for value in first.granularity.period_range(first.value, last.value)]
        assets = self._components.target_assets(job)
        first_day, last_day = Period.from_key(start_key).first_day, Period.from_key(end_key).last_day
        overlapping = partition_keys_overlapping(col(Execution.partition_key), first_day, last_day)
        results = self._partition_results(org_id, [overlapping], asset_ids=[asset.id for asset in assets])
        return JobCoverage.from_results(
            job.id, keys, assets, self._components.asset_partitionings(assets), results, now=now
        )

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
            return [ErrorRow._make(row) for row in session.connection().execute(statement)]

    def _run_error_rows(
        self,
        org_id: UUID,
        *,
        since: dt.datetime | None,
        until: dt.datetime | None,
        job_id: UUID | None,
        backfill_id: UUID | None,
        run_id: UUID | None,
    ) -> list[ErrorRow]:
        """Failed runs that recorded an error of their own, one row each, one past the cap.

        Args:
            org_id: Organisation UUID.
            since: Keep runs that ended at or after this instant.
            until: Keep runs that ended before this instant.
            job_id: Keep runs targeting this component.
            backfill_id: Keep this backfill's runs.
            run_id: Keep this run.

        Returns:
            The rows, the latest first, at most one past :data:`MAX_ERROR_ROWS`.
        """
        filters: list[Any] = [Run.org_id == org_id, Run.status == RunStatus.FAILED, col(Run.error).is_not(None)]
        if since is not None:
            filters.append(col(Run.completed_at) >= since)
        if until is not None:
            filters.append(col(Run.completed_at) < until)
        if job_id is not None:
            filters.append(Run.component_id == job_id)
        if backfill_id is not None:
            filters.append(Run.backfill_id == backfill_id)
        if run_id is not None:
            filters.append(Run.id == run_id)
        statement = select(Run).where(*filters).order_by(col(Run.completed_at).desc()).limit(MAX_ERROR_ROWS + 1)
        rows: list[ErrorRow] = []
        with session_scope(self._engine) as session:
            for run in session.exec(statement).all():
                assert run.completed_at is not None, "a failed run has ended"
                ended = run.completed_at
                rows.append(ErrorRow(run.component_id, run.id, None, "run_failed", run.error or "", 1, ended, ended))
        return rows

    def _rows(
        self, org_id: UUID, *, kinds: Sequence[str] | None = None, ids: Collection[UUID] | None = None
    ) -> list[Component]:
        """An organisation's component rows, oldest first, with their parents and no other relation.

        The insights read many rows of an organisation at once, and the
        relations the listings load alongside (children, outgoing relations
        and their targets) would more than double what each row holds. The
        parent stays: an asset's qualified key, and so its status and
        partitioning, and its inherited history start read it.

        Args:
            org_id: Organisation UUID.
            kinds: Keep these kinds; ``None`` keeps every kind.
            ids: Keep these rows; ``None`` keeps every row.

        Returns:
            The rows, safe to read detached as far as their parents.
        """
        statement = (
            select(Component)
            .where(Component.org_id == org_id)
            .options(selectinload(Component.parent))  # ty: ignore[invalid-argument-type]
            .order_by(col(Component.created_at), col(Component.id))
        )
        if kinds is not None:
            statement = statement.where(col(Component.kind).in_(kinds))
        if ids is not None:
            statement = statement.where(col(Component.id).in_(ids))
        with session_scope(self._engine) as session:
            return list(session.exec(statement).all())

    def _targeted(self, org_id: UUID) -> set[UUID]:
        """The components an enabled job of an organisation targets.

        Args:
            org_id: Organisation UUID.

        Returns:
            The targets' ids: sources and assets, as the jobs name them.
        """
        statement = (
            select(Component, ComponentRelation.dst_id)
            .join(ComponentRelation, col(ComponentRelation.src_id) == col(Component.id))
            .where(Component.org_id == org_id, Component.kind == "job", ComponentRelation.name == "targets")
        )
        with session_scope(self._engine) as session:
            return {target for job, target in session.exec(statement).all() if job.enabled}

    def _partition_results(
        self, org_id: UUID, keys: Sequence[Any], *, asset_ids: Sequence[UUID] | None = None
    ) -> Iterator[PartitionResult]:
        """Per asset and time partition, whether any execution of it succeeded or failed, grouped by asset.

        Runs of every target count: the partition key is the one stamped on
        each execution, which its operation named (a snapshot's observed
        period), else its run's. The rows stream in batches from the
        database, in asset order, so a consumer holding one asset's at a
        time never holds the whole history read.

        Args:
            org_id: Organisation UUID.
            keys: Filters over ``executions.partition_key`` selecting the
                partitions to read.
            asset_ids: Keep these assets; ``None`` keeps every asset.

        Yields:
            One row per asset and selected partition key that executed at
            least once, an asset's rows consecutive.
        """
        key = col(Execution.partition_key)
        filters = [col(Execution.org_id) == org_id, key.is_not(None), *keys]
        if asset_ids is not None:
            filters.append(col(Execution.component_id).in_(asset_ids))
        # Cast for a portable max(): Postgres has no max(uuid), and UUID() parses both its dashed text and SQLite's hex.
        failed_run = func.max(case((col(Execution.status) == "failed", cast(col(Execution.run_id), String))))
        # The asset id comes back as text and is parsed once per asset: a UUID per row costs more than the roll-up.
        asset = cast(col(Execution.component_id), String())
        statement = (
            select(asset, key, func.max(case((col(Execution.status) == "success", 1), else_=0)), failed_run)
            .where(*filters)
            .group_by(col(Execution.component_id), key)
            .order_by(col(Execution.component_id))
            .execution_options(yield_per=10_000)
        )
        parsed: dict[str, UUID] = {}
        with session_scope(self._engine) as session:
            for asset_text, partition_key, succeeded, failed in session.exec(statement):
                assert partition_key is not None, "the query keeps keyed executions only"
                yield PartitionResult(
                    parsed.get(asset_text) or parsed.setdefault(asset_text, UUID(asset_text)),
                    partition_key,
                    succeeded=bool(succeeded),
                    failed=failed is not None,
                    failed_run_id=UUID(failed) if failed else None,
                )

    def _attempted_spans(self, org_id: UUID) -> dict[UUID, Period]:
        """The period each asset's attempted partitions span, all-time, from its first to its last.

        Two index probes per asset of the organisation, whatever its history:
        its smallest and greatest time partition key, which sort
        chronologically within the asset's granularity.

        Args:
            org_id: Organisation UUID.

        Returns:
            The period per asset that was ever attempted.
        """
        key = col(Execution.partition_key)
        lengths = [
            len(granularity.format(dt.datetime(2000, 1, 1)))
            for granularity in TimeGranularity
            if granularity.key_format is not None
        ]

        def edge(aggregate: Any) -> Any:
            return (
                select(aggregate(key))
                .where(
                    col(Execution.org_id) == org_id,
                    col(Execution.component_id) == col(Component.id),
                    func.length(key).in_(lengths),
                )
                .correlate(Component)
                .scalar_subquery()
            )

        statement = select(col(Component.id), edge(func.min), edge(func.max)).where(
            col(Component.org_id) == org_id, col(Component.kind) == "asset"
        )
        with session_scope(self._engine) as session:
            return {
                asset_id: Period.enclosing([Period.from_key(first), Period.from_key(last)])
                for asset_id, first, last in session.exec(statement).all()
                if first is not None
            }
