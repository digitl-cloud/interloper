#!/usr/bin/env python
"""Populate the scale-test database with organisations shaped like production ones.

Every organisation one invocation adds gets the same population: demo sources
with their five assets, cron jobs over a share of them, members with sessions,
and a history of backfills, runs, executions and events. The invocation's
first organisation's components go through the store, as the app creates
them; the others clone those rows, and a later invocation adds organisations
of another shape after them. History is copied in with ``COPY`` while the
tables' user triggers are off: the execution fold and the realtime
notifications are the live phase's to exercise, not the load's, so executions
are written as the fold would have left them.

The last ``--event-days`` days carry every event a run reports. Older runs
keep their executions and their failures' ``operation_failed`` events, which
is all the overview, coverage and failure readers look at; their full event
history would only add bulk, which ``--event-days`` scales on purpose.

Sessions for the members are written to ``state/sessions.json`` for the load
driver. Run from ``dev/``, whose ``interloper.yaml`` holds the catalog, with
the ``INTERLOPER_*`` connection settings the Makefile's ``scale-*`` targets
set::

    uv run python scale/generate.py --orgs 10
"""

from __future__ import annotations

import argparse
import datetime as dt
import itertools
import json
import random
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import interloper as il
import psycopg
from interloper_db import init_engine
from interloper_db.settings import PostgresSettings
from interloper_db.store import Store

STATE = Path(__file__).parent / "state"
SESSIONS = STATE / "sessions.json"
SOURCE_KEY = "demo_source"
ROLES = ("admin", "editor", "viewer")
ERRORS = (
    "HTTPError: 429 Too Many Requests for url https://api.example.com/v3/reports?level=ad&fields=spend,clicks",
    "HTTPError: 401 Unauthorized: the access token expired, reconnect the account to grant a new one",
    "TimeoutError: the read operation timed out after 300 seconds waiting for the report to be generated",
    "ValueError: column 'spend' declared FLOAT64 but the response carries a string for 12 rows",
    "DestinationError: quota exceeded: too many table update operations for this table, retry later",
)


# -- Shape ---------------------------------------------------------------------


@dataclass(frozen=True)
class Shape:
    """What each generated organisation holds.

    Attributes:
        sources: Demo sources, each owning five assets.
        jobs: Cron jobs, sharing the targeted sources between them.
        job_share: Share of the sources some job targets.
        days: Days of run history, ending yesterday.
        runs_per_day: Runs per job and day: the scheduled one, its backfill's,
            then reruns three hours apart.
        event_days: Most recent days whose runs carry every event.
        failure_rate: Share of the history's executions that failed.
        live_failure_rate: The demo sources' random failure probability, per
            asset, when the scheduler probe runs them.
        members: Members per organisation, each with a session.
    """

    sources: int
    jobs: int
    job_share: float
    days: int
    runs_per_day: int
    event_days: int
    failure_rate: float
    live_failure_rate: float
    members: int

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> Shape:
        """Read the shape off the command line.

        Args:
            args: The parsed arguments.

        Returns:
            The shape.
        """
        return cls(
            sources=args.sources,
            jobs=args.jobs,
            job_share=args.job_share,
            days=args.days,
            runs_per_day=args.runs_per_day,
            event_days=args.event_days,
            failure_rate=args.failure_rate,
            live_failure_rate=args.live_failure_rate,
            members=args.members,
        )


@dataclass(frozen=True)
class Asset:
    """An asset a job's runs execute.

    Attributes:
        id: The asset's component id.
        key: Its key within its source (``a`` to ``e``).
        parent_id: Its source's component id.
    """

    id: uuid.UUID
    key: str
    parent_id: uuid.UUID


@dataclass(frozen=True)
class Job:
    """A job and the assets its runs execute.

    Attributes:
        id: The job's component id.
        name: Its name.
        assets: The assets of the sources it targets.
    """

    id: uuid.UUID
    name: str
    assets: tuple[Asset, ...]


@dataclass(frozen=True)
class Outcome:
    """How one asset fared in one run.

    Attributes:
        asset: The asset.
        status: ``success``, ``failed`` or ``skipped``.
        started_at: When its operation started.
        completed_at: When it ended.
        error: Why it failed, for a failure.
    """

    asset: Asset
    status: str
    started_at: dt.datetime
    completed_at: dt.datetime
    error: str | None = None


@dataclass
class PlannedRun:
    """One run of the history and what its operations did.

    Attributes:
        id: The run id.
        job: The job it ran.
        partition: Its partition key, the day it covers.
        backfill_id: The backfill it belongs to, if any.
        started_at: When it started.
        outcomes: One per asset the job executes.
    """

    id: uuid.UUID
    job: Job
    partition: str
    backfill_id: uuid.UUID | None
    started_at: dt.datetime
    outcomes: list[Outcome] = field(default_factory=list)

    @property
    def status(self) -> str:
        """The run's verdict: failed when any operation failed.

        Returns:
            ``failed`` or ``success``.
        """
        return "failed" if any(outcome.status == "failed" for outcome in self.outcomes) else "success"

    @property
    def completed_at(self) -> dt.datetime:
        """When the last operation ended.

        Returns:
            The run's end.
        """
        return max(outcome.completed_at for outcome in self.outcomes)


# -- History -------------------------------------------------------------------


class History:
    """One organisation's run history, as rows for ``COPY``."""

    def __init__(self, org_id: uuid.UUID, jobs: list[Job], shape: Shape, now: dt.datetime) -> None:
        """Plan the history: every run and how each of its operations fared.

        Args:
            org_id: The organisation.
            jobs: Its jobs, with the assets they execute.
            shape: The shape to generate.
            now: The instant the history ends at.
        """
        self.org_id = org_id
        self._shape = shape
        self._now = now
        self._random = random.Random(str(org_id))
        self._events_since = now - dt.timedelta(days=shape.event_days)
        self._first_day = now.date() - dt.timedelta(days=shape.days)
        self._backfills = {job.id: uuid.uuid4() for job in jobs}
        self._backfill_created = dt.datetime.combine(self._first_day, dt.time(9), dt.UTC)
        self.runs = [run for job in jobs for run in self._plan(job)]

    def _plan(self, job: Job) -> Iterator[PlannedRun]:
        """Plan a job's runs, day by day.

        Args:
            job: The job.

        Yields:
            Each of its runs that started before now.
        """
        for offset in range(self._shape.days):
            day = self._first_day + dt.timedelta(days=offset)
            scheduled = dt.datetime.combine(day + dt.timedelta(days=1), dt.time(6), dt.UTC)
            for index in range(self._shape.runs_per_day):
                backfill_id = self._backfills[job.id] if index == 1 else None
                if index == 0:
                    started = scheduled + dt.timedelta(seconds=self._random.randint(0, 120))
                elif index == 1:
                    started = self._backfill_created + dt.timedelta(minutes=offset * 2)
                else:
                    started = scheduled + dt.timedelta(hours=3 * (index - 1))
                if started >= self._now:
                    continue
                run = PlannedRun(uuid.uuid4(), job, day.isoformat(), backfill_id, started)
                run.outcomes = [self._outcome(asset, started) for asset in job.assets]
                yield run

    def _outcome(self, asset: Asset, run_started: dt.datetime) -> Outcome:
        """Decide how an asset fared in a run.

        Args:
            asset: The asset.
            run_started: When its run started.

        Returns:
            The outcome, ending before now.
        """
        draw = self._random.random()
        if draw < self._shape.failure_rate:
            status = "failed"
        else:
            status = "skipped" if draw < self._shape.failure_rate + 0.03 else "success"
        started = run_started + dt.timedelta(seconds=self._random.uniform(0, 600))
        completed = min(started + dt.timedelta(seconds=self._random.uniform(5, 300)), self._now)
        error = self._random.choice(ERRORS) if status == "failed" else None
        return Outcome(asset, status, started, completed, error)

    # -- Rows ------------------------------------------------------------------

    def rows(self) -> dict[str, Iterator[dict[str, Any]]]:
        """Every table's rows, in the order their foreign keys need them copied.

        Returns:
            Each history table's rows, by table.
        """
        return {
            "backfills": self._backfill_rows(),
            "runs": self._run_rows(),
            "executions": self._execution_rows(),
            "events": self._event_rows(),
        }

    def _backfill_rows(self) -> Iterator[dict[str, Any]]:
        """Yield one backfill per job, covering the whole history.

        Yields:
            ``backfills`` rows.
        """
        done = self._backfill_created + dt.timedelta(minutes=self._shape.days * 2 + 15)
        for job_id, backfill_id in self._backfills.items():
            yield {
                "id": backfill_id,
                "component_id": job_id,
                "org_id": self.org_id,
                "status": "success",
                "start_key": self._first_day.isoformat(),
                "end_key": (self._first_day + dt.timedelta(days=self._shape.days - 1)).isoformat(),
                "concurrency": 4,
                "fail_fast": False,
                "partitions": self._shape.days,
                "started_at": self._backfill_created,
                "completed_at": done,
                "hooks_evaluated_at": done,
                "created_at": self._backfill_created,
            }

    def _run_rows(self) -> Iterator[dict[str, Any]]:
        """Yield the runs, ended and their hooks evaluated.

        Yields:
            ``runs`` rows.
        """
        for run in self.runs:
            completed = run.completed_at
            yield {
                "id": run.id,
                "component_id": run.job.id,
                "org_id": self.org_id,
                "backfill_id": run.backfill_id,
                "partition_key": run.partition,
                "status": run.status,
                "attempt": 1,
                "root_run_id": run.id,
                "billable": True,
                "started_at": run.started_at,
                "heartbeat_at": completed,
                "completed_at": completed,
                "hooks_evaluated_at": completed,
                "created_at": run.started_at,
            }

    def _execution_rows(self) -> Iterator[dict[str, Any]]:
        """Yield every operation of every run, as the event fold leaves it.

        Yields:
            ``executions`` rows.
        """
        for run in self.runs:
            for outcome in run.outcomes:
                yield {
                    "run_id": run.id,
                    "component_id": outcome.asset.id,
                    "org_id": self.org_id,
                    "component_key": outcome.asset.key,
                    "partition_key": run.partition,
                    "status": outcome.status,
                    "attempts": 1,
                    "started_at": outcome.started_at,
                    "completed_at": outcome.completed_at,
                    "created_at": run.started_at,
                }

    def _event_rows(self) -> Iterator[dict[str, Any]]:
        """Yield the runs' events: all of them for recent runs, the failures of older ones.

        Yields:
            ``events`` rows.
        """
        for run in self.runs:
            if run.started_at >= self._events_since:
                yield from self._run_events(run)
                continue
            for outcome in run.outcomes:
                if outcome.status == "failed":
                    yield self._operation_event(run, outcome, "operation_failed", outcome.completed_at)

    def _run_events(self, run: PlannedRun) -> Iterator[dict[str, Any]]:
        """Yield everything a run reports, in the proportions production runs report it.

        Args:
            run: The run.

        Yields:
            ``events`` rows.
        """
        count = len(run.outcomes)
        yield self._row(run, None, "run_started", run.started_at, message=f"Run started ({count} operations)")
        for outcome in run.outcomes:
            yield self._operation_event(run, outcome, "operation_queued", run.started_at)
            if outcome.status == "skipped":
                yield self._operation_event(run, outcome, "operation_skipped", outcome.completed_at)
                continue
            yield self._operation_event(run, outcome, "operation_started", outcome.started_at)
            yield self._operation_event(run, outcome, "asset_data_started", outcome.started_at)
            span = (outcome.completed_at - outcome.started_at) / 5
            for index in range(self._random.randint(2, 4)):
                yield self._operation_event(
                    run,
                    outcome,
                    "log",
                    outcome.started_at + span * (index + 1),
                    level="INFO",
                    message=f"Fetched page {index + 1} of the {outcome.asset.key} report for {run.partition}",
                )
            if outcome.status == "failed":
                yield self._operation_event(run, outcome, "asset_data_failed", outcome.completed_at, traceback=True)
                yield self._operation_event(run, outcome, "operation_failed", outcome.completed_at)
                continue
            yield self._operation_event(run, outcome, "asset_data_completed", outcome.completed_at)
            if self._random.random() < 0.57:
                yield self._operation_event(run, outcome, "dest_write_started", outcome.completed_at)
                yield self._operation_event(run, outcome, "dest_write_completed", outcome.completed_at, rows=1200)
            yield self._operation_event(run, outcome, "operation_completed", outcome.completed_at)
        succeeded = sum(outcome.status == "success" for outcome in run.outcomes)
        ending = "run_failed" if run.status == "failed" else "run_completed"
        yield self._row(run, None, ending, run.completed_at, message=f"Run completed ({succeeded}/{count} succeeded)")

    def _operation_event(
        self,
        run: PlannedRun,
        outcome: Outcome,
        event_type: str,
        timestamp: dt.datetime,
        *,
        level: str | None = None,
        message: str | None = None,
        traceback: bool = False,
        rows: int | None = None,
    ) -> dict[str, Any]:
        """Build one event of an operation.

        Args:
            run: The run.
            outcome: The operation's outcome.
            event_type: The event type.
            timestamp: When it happened.
            level: Its log level, for a log line.
            message: Its message; a default one names the asset.
            traceback: Whether it carries the failure's traceback.
            rows: Rows written, for a destination write.

        Returns:
            An ``events`` row.
        """
        failure = event_type in ("asset_data_failed", "operation_failed")
        return self._row(
            run,
            outcome.asset,
            event_type,
            timestamp,
            level=level,
            message=message or f"{event_type.replace('_', ' ').capitalize()}: '{outcome.asset.key}'",
            error=outcome.error if failure else None,
            traceback=_traceback(outcome.error) if traceback and outcome.error else None,
            extra={"rows": rows, "destination_key": "bigquery_destination"} if rows is not None else {},
        )

    def _row(
        self,
        run: PlannedRun,
        asset: Asset | None,
        event_type: str,
        timestamp: dt.datetime,
        *,
        level: str | None = None,
        message: str | None = None,
        error: str | None = None,
        traceback: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Build one event row, its ``data`` shaped like the runner's.

        Args:
            run: The run.
            asset: The asset it concerns; ``None`` for a run-level event.
            event_type: The event type.
            timestamp: When it happened.
            level: Its log level.
            message: Its message.
            error: The failure's reason.
            traceback: The failure's traceback.
            extra: Further ``data`` keys.

        Returns:
            An ``events`` row.
        """
        data: dict[str, Any] = {
            "window": run.partition,
            "attempt": 1,
            "target_id": str(run.job.id),
            "target_key": "cron_job",
            "target_kind": "job",
            "target_name": run.job.name,
            "observed_at": run.started_at.isoformat(),
        }
        if run.backfill_id is not None:
            data["backfill_id"] = str(run.backfill_id)
        if asset is not None:
            data |= {"parent_id": str(asset.parent_id), "qualified_key": f"{SOURCE_KEY}.{asset.key}"}
        data |= extra or {}
        return {
            "id": uuid.uuid4(),
            "org_id": self.org_id,
            "run_id": run.id,
            "event_type": event_type,
            "error": error,
            "traceback": traceback,
            "component_id": asset.id if asset else None,
            "component_kind": "asset" if asset else None,
            "component_key": asset.key if asset else None,
            "message": message,
            "level": level,
            "data": json.dumps(data),
            "timestamp": timestamp,
        }


def _traceback(error: str) -> str:
    """Build a traceback the size production failures carry.

    Args:
        error: The failure's reason, its last line.

    Returns:
        A multi-frame traceback of about four kilobytes.
    """
    frames = "".join(
        f'  File "/app/.venv/lib/python3.14/site-packages/interloper/runner/frame_{index}.py", line {40 + index}, '
        f"in step_{index}\n    result = await self._call(operation, context, upstream_{index})\n"
        for index in range(24)
    )
    return f"Traceback (most recent call last):\n{frames}{error}\n"


# -- Population ----------------------------------------------------------------


class Population:
    """Creates organisations, their members, components and history."""

    def __init__(self, shape: Shape) -> None:
        """Connect the store and a raw connection for the bulk writes.

        Args:
            shape: What each organisation holds.
        """
        self._shape = shape
        settings = PostgresSettings.get()
        init_engine(settings.dsn)
        self._store = Store.from_settings(il.Catalog.from_settings())
        self._connection = psycopg.connect(
            settings.dsn.replace("postgresql+psycopg://", "postgresql://"), autocommit=True
        )
        self._template: uuid.UUID | None = None

    def existing(self) -> int:
        """Count the organisations a previous generation created.

        Returns:
            How many there are.
        """
        row = self._connection.execute("SELECT count(*) FROM organisations WHERE name LIKE 'Scale %'").fetchone()
        return row[0] if row else 0

    def create(self, number: int, now: dt.datetime) -> dict[str, Any]:
        """Create one organisation with its members, components and history.

        Args:
            number: Its number, which names it and its members.
            now: The instant its history ends at.

        Returns:
            Its id, name and its members' sessions.
        """
        name = f"Scale {number:04d}"
        profiles = [
            self._store.profiles.provision(email=f"member{index}.org{number}@scale.test", name=f"Member {index}")
            for index in range(self._shape.members)
        ]
        org = self._store.organisations.create(name=name, creator_id=profiles[0].id)
        for index, profile in enumerate(profiles[1:], start=1):
            self._store.members.add(org.id, profile.id, ROLES[index % len(ROLES)])
        if self._template is None:
            self._components(org.id)
            self._template = org.id
        else:
            self._clone(org.id)
        history = History(org.id, self._jobs(org.id), self._shape, now)
        self._copy_history(history)
        return {
            "id": str(org.id),
            "name": name,
            "sessions": [self._store.tokens.create_session(profile.id) for profile in profiles],
        }

    def analyze(self) -> None:
        """Refresh the planner's statistics after the bulk load."""
        self._connection.execute("ANALYZE")

    def _components(self, org_id: uuid.UUID) -> None:
        """Create the template organisation's destination, sources and jobs through the store, as the app does.

        The demo assets read their upstreams back from a destination, so
        every source writes to a CSV destination under ``state/csv``, which
        the scheduler probe's pods write to.

        Args:
            org_id: The organisation.
        """
        destination = self._store.components.create(
            org_id, kind="destination", key="csv_destination", name="Files", config={"base_path": str(STATE / "csv")}
        )
        sources = [
            self._store.components.create(
                org_id,
                kind="source",
                key=SOURCE_KEY,
                name=f"Demo {index:03d}",
                config={"dataset": f"demo_{index:03d}", "random_failure_probability": self._shape.live_failure_rate},
                relations={"destinations": [destination.id]},
            )
            for index in range(self._shape.sources)
        ]
        targeted = sources[: int(len(sources) * self._shape.job_share)]
        for index in range(self._shape.jobs):
            self._store.components.create(
                org_id,
                kind="job",
                key="cron_job",
                name=f"Job {index:02d}",
                config={"cron": f"{(index * 7) % 60} {5 + index % 4} * * *", "enabled": True},
                relations={"targets": [source.id for source in targeted[index :: self._shape.jobs]]},
            )

    def _clone(self, org_id: uuid.UUID) -> None:
        """Copy the template organisation's components and relations into another.

        Args:
            org_id: The organisation receiving them.
        """
        parameters = {"template": self._template, "org": org_id}
        with self._connection.transaction():
            self._connection.execute(
                "CREATE TEMP TABLE clone_map ON COMMIT DROP AS "
                "SELECT id AS old_id, gen_random_uuid() AS new_id FROM components WHERE org_id = %(template)s",
                parameters,
            )
            self._connection.execute(
                "INSERT INTO components (id, org_id, kind, key, name, parent_id, config, encrypted, created_at, "
                "updated_at) SELECT m.new_id, %(org)s, c.kind, c.key, c.name, p.new_id, c.config, c.encrypted, now(), "
                "now() FROM components c JOIN clone_map m ON m.old_id = c.id LEFT JOIN clone_map p ON p.old_id = "
                "c.parent_id",
                parameters,
            )
            self._connection.execute(
                "INSERT INTO component_relations (src_id, name, dst_id, org_id, src_kind, dst_kind) "
                "SELECT s.new_id, r.name, d.new_id, %(org)s, r.src_kind, r.dst_kind FROM component_relations r "
                "JOIN clone_map s ON s.old_id = r.src_id JOIN clone_map d ON d.old_id = r.dst_id",
                parameters,
            )

    def _jobs(self, org_id: uuid.UUID) -> list[Job]:
        """Read an organisation's jobs and the assets their runs execute.

        Args:
            org_id: The organisation.

        Returns:
            The jobs, in name order.
        """
        rows = self._connection.execute(
            "SELECT j.id, j.name, a.id, a.key, a.parent_id FROM components j "
            "JOIN component_relations r ON r.src_id = j.id AND r.name = 'targets' "
            "JOIN components a ON a.parent_id = r.dst_id "
            "WHERE j.org_id = %s AND j.kind = 'job' ORDER BY j.name, a.parent_id, a.key",
            (org_id,),
        ).fetchall()
        assets: dict[tuple[uuid.UUID, str], list[Asset]] = {}
        for job_id, job_name, asset_id, asset_key, parent_id in rows:
            assets.setdefault((job_id, job_name), []).append(Asset(asset_id, asset_key, parent_id))
        return [Job(job_id, job_name, tuple(owned)) for (job_id, job_name), owned in assets.items()]

    def _copy_history(self, history: History) -> None:
        """Copy an organisation's history in, its tables' user triggers off, and meter its successful runs.

        The usage ledger counts what dispatch would have metered, so the
        reaper's reconciliation finds no drift to correct.

        Args:
            history: The history.
        """
        tables = history.rows()
        with self._connection.transaction():
            for table in tables:
                self._connection.execute(f"ALTER TABLE {table} DISABLE TRIGGER USER")
            for table, rows in tables.items():
                first = next(rows, None)
                if first is None:
                    continue
                columns = list(first)
                with self._connection.cursor().copy(f"COPY {table} ({', '.join(columns)}) FROM STDIN") as copy:
                    for row in itertools.chain([first], rows):
                        copy.write_row([row[column] for column in columns])
            for table in tables:
                self._connection.execute(f"ALTER TABLE {table} ENABLE TRIGGER USER")
            self._connection.execute(
                "INSERT INTO usage (org_id, metric, period_start, used, reserved) "
                "SELECT org_id, 'successful_runs', date_trunc('month', completed_at AT TIME ZONE 'UTC')::date, "
                "count(*), 0 FROM runs WHERE org_id = %s AND status = 'success' AND billable GROUP BY 1, 3",
                (history.org_id,),
            )


# -- Entry point ---------------------------------------------------------------


def main() -> None:
    """Generate the organisations the command line asks for, after any already there."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--orgs", type=int, default=10, help="organisations to add")
    parser.add_argument("--sources", type=int, default=200, help="demo sources per organisation")
    parser.add_argument("--jobs", type=int, default=15, help="cron jobs per organisation")
    parser.add_argument("--job-share", type=float, default=0.5, help="share of the sources some job targets")
    parser.add_argument("--days", type=int, default=120, help="days of run history")
    parser.add_argument("--runs-per-day", type=int, default=3, help="runs per job and day")
    parser.add_argument("--event-days", type=int, default=30, help="recent days carrying every event")
    parser.add_argument("--failure-rate", type=float, default=0.15, help="share of the history's executions failed")
    parser.add_argument("--live-failure-rate", type=float, default=0.005, help="per-asset failure rate of live runs")
    parser.add_argument("--members", type=int, default=5, help="members per organisation, each with a session")
    args = parser.parse_args()

    shape = Shape.from_args(args)
    population = Population(shape)
    STATE.mkdir(exist_ok=True)
    state = json.loads(SESSIONS.read_text()) if SESSIONS.exists() else {"organisations": []}
    first = population.existing() + 1
    now = dt.datetime.now(dt.UTC)
    for number in range(first, first + args.orgs):
        started = time.monotonic()
        created = population.create(number, now)
        state["organisations"].append(created)
        SESSIONS.write_text(json.dumps(state, indent=2))
        print(f"{created['name']}: {time.monotonic() - started:5.1f}s", flush=True)
    population.analyze()
    print(f"{len(state['organisations'])} organisations; sessions in {SESSIONS}")


if __name__ == "__main__":
    main()
