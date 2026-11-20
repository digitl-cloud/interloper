#!/usr/bin/env python
"""Drive the scale-test stack's run traffic: the scheduler's loops, and pods reporting over HTTP.

The scheduler's own controllers run against the scale database, every pass
timed: cron with its hook and renewal controllers, the queue worker and the
reaper. The worker launches each run it claims onto a simulated pod, a
thread doing what ``interloper launch`` does in a run's pod: it loads the
run's spec from the API as the run's token, executes the demo assets, and
posts their events back in batches, so the API and the database take a
run's real traffic. Only the pod's process is simulated, which in
production runs on another node and costs the API nothing.

A feeder keeps ``--live-runs`` runs open in each of ``--orgs``
organisations, each one the backfill a daily job's cron firing creates.
The serial runner (the default) executes one asset at a time, about a
second each, so a run of a job over 30 assets lasts about half a minute.

Prints every ``--report`` seconds the loops' pass times, the pods' spec
loads (``spec``) and event deliveries (``ingest``), and the runs' dispatch
latency, and writes a summary to ``state/`` at the end. Run from ``dev/``,
whose ``interloper.yaml`` holds the catalog the pods build their runs with::

    uv run python scale/scheduler.py --orgs 10 --live-runs 3 --duration 600
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import logging
import random
import statistics
import threading
import time
from collections import Counter, defaultdict
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path
from typing import Any
from uuid import UUID

import interloper as il
import psycopg
from interloper.dag import DAG
from interloper.events import EventBatch, EventRecorder
from interloper.rest import HTTPBearerAuth, HttpRetryPolicy, RESTClient
from interloper.runner import Runner
from interloper.runner.spec import RunSpec
from interloper.settings import RunnerSettings
from interloper.utils import Supervisor
from interloper_db import init_engine
from interloper_db.models import Run
from interloper_db.settings import PostgresSettings
from interloper_db.store import Store
from interloper_scheduler.controller import Controller
from interloper_scheduler.launcher import Launcher
from interloper_scheduler.queue import QueueController
from interloper_scheduler.reaper import Reaper
from interloper_scheduler.services import scheduler_services
from interloper_scheduler.settings import SchedulerSettings

STATE = Path(__file__).parent / "state"
OPEN_STATUSES = ("pending", "queued", "dispatched", "running")

logger = logging.getLogger("scale")


# -- Timing --------------------------------------------------------------------


class PassTimer:
    """Times the controllers' passes and the pods' calls to the API, by name."""

    def __init__(self) -> None:
        """Start with nothing recorded."""
        self._lock = threading.Lock()
        self._passes: dict[str, list[float]] = defaultdict(list)
        self.totals: dict[str, list[float]] = defaultdict(list)

    def wrap(self, name: str, controller: Controller) -> Controller:
        """Time the controller's passes under a name.

        Args:
            name: The loop's name in the report.
            controller: The controller, whose ``_tick`` is shadowed in place.

        Returns:
            The controller.
        """
        controller._tick = partial(self.time, name, controller._tick)  # ty: ignore[invalid-assignment]
        return controller

    def drain(self) -> dict[str, list[float]]:
        """Hand over what was recorded since the last call.

        Returns:
            Each name's durations in seconds.
        """
        with self._lock:
            passes, self._passes = self._passes, defaultdict(list)
        return passes

    def time(self, name: str, call: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Make a call and record how long it took, failed or not.

        Args:
            name: Its name in the report.
            call: The call.
            *args: Its positional arguments.
            **kwargs: Its keyword arguments.

        Returns:
            What the call returned.
        """
        started = time.perf_counter()
        try:
            return call(*args, **kwargs)
        finally:
            elapsed = time.perf_counter() - started
            with self._lock:
                self._passes[name].append(elapsed)
                self.totals[name].append(elapsed)


# -- Pods ----------------------------------------------------------------------


class PodLauncher(Launcher):
    """Launches each run onto a thread that does what a run's pod does, over HTTP."""

    def __init__(self, api_url: str, *, pods: int, runner_type: str, timer: PassTimer) -> None:
        """Hold the API the pods report to and the threads they run on.

        Args:
            api_url: The API's base URL, as a pod's ``server.internal_url``.
            pods: Most pods executing at once; a run beyond waits dispatched.
            runner_type: The runner each pod executes its DAG with.
            timer: Times the pods' spec loads and event deliveries.
        """
        super().__init__(runner_type=runner_type)
        self._api_url = api_url
        self._pool = ThreadPoolExecutor(max_workers=pods, thread_name_prefix="pod")
        self._timer = timer
        self.outcomes: Counter[str] = Counter()

    def launch(self, run: Run, token: str) -> None:
        """Start the run's pod.

        Args:
            run: The dispatched run.
            token: The run's token, which the pod presents to the API.
        """
        self._pool.submit(self._execute, str(run.id), token)

    def close(self) -> None:
        """Wait for the pods already launched to end their runs."""
        self._pool.shutdown(wait=True)

    def _execute(self, run_id: str, token: str) -> None:
        """Load the run from the API, execute it and report its events back, as ``interloper launch`` does.

        Args:
            run_id: The run.
            token: The run's token.
        """
        client = RESTClient(self._api_url, auth=HTTPBearerAuth(token), timeout=30, retry=HttpRetryPolicy())
        recorder = EventRecorder(
            partial(self._timer.time, "ingest", EventBatch.post, client=client, run_id=run_id),
            run_id=run_id,
            heartbeat_interval=10,
            heartbeat_timeout=90,
        )
        try:
            with recorder:
                spec = self._timer.time("spec", RunSpec.from_api, client, run_id, timeout=45)
                runner = Runner.from_settings(RunnerSettings(type=self._runner_type))
                recorded = runner.model_copy(update={"on_event": recorder, "reraise": False})
                recorder.finish(asyncio.run(recorded.run(DAG.from_spec(spec.dag), spec.window, spec.metadata)))
            self.outcomes["executed"] += 1
        except Exception:
            logger.exception("Pod of run %s crashed", run_id)
            self.outcomes["crashed"] += 1


# -- Feeding -------------------------------------------------------------------


class Feeder(Controller):
    """Keeps a number of runs open in each organisation, as cron firings would."""

    def __init__(self, store: Store, connection: psycopg.Connection, orgs: list[UUID], live_runs: int) -> None:
        """Load the organisations' jobs.

        Args:
            store: The store the backfills are created through.
            connection: A connection to count the open runs on.
            orgs: The organisations fed.
            live_runs: Runs to keep open in each.
        """
        super().__init__(poll_interval=2)
        self._store = store
        self._connection = connection
        self._orgs = orgs
        self._live_runs = live_runs
        self._random = random.Random(0)
        self._jobs: dict[UUID, list[UUID]] = defaultdict(list)
        rows = connection.execute(
            "SELECT org_id, id FROM components WHERE kind = 'job' AND org_id = ANY(%s)", (orgs,)
        ).fetchall()
        for org_id, job_id in rows:
            self._jobs[org_id].append(job_id)
        self.created = 0

    def _tick(self) -> None:
        """Create a daily job's backfill of yesterday for every run an organisation is short of."""
        rows = self._connection.execute(
            "SELECT org_id, count(*) FROM runs WHERE org_id = ANY(%s) AND status = ANY(%s) GROUP BY org_id",
            (self._orgs, list(OPEN_STATUSES)),
        ).fetchall()
        self._connection.commit()
        open_runs = dict(rows)
        yesterday = (dt.datetime.now(dt.UTC).date() - dt.timedelta(days=1)).isoformat()
        for org_id in self._orgs:
            for _ in range(self._live_runs - open_runs.get(org_id, 0)):
                job_id = self._random.choice(self._jobs[org_id])
                self._store.backfills.create(org_id, component_id=job_id, start_key=yesterday, end_key=yesterday)
                self.created += 1


# -- Reporting -----------------------------------------------------------------


class Reporter(Controller):
    """Prints the loops' pass times and the runs' progress at a fixed interval."""

    def __init__(
        self,
        connection: psycopg.Connection,
        orgs: list[UUID],
        timer: PassTimer,
        launcher: PodLauncher,
        interval: int,
    ) -> None:
        """Hold what the report reads.

        Args:
            connection: A connection to read the runs on.
            orgs: The organisations fed.
            timer: The loops' pass timer.
            launcher: The pods' launcher, counting their outcomes.
            interval: Seconds between reports.
        """
        super().__init__(poll_interval=interval)
        self._connection = connection
        self._orgs = orgs
        self._timer = timer
        self._launcher = launcher
        self._began = dt.datetime.now(dt.UTC)
        self._since = self._began
        self.windows: list[dict[str, Any]] = []

    def _tick(self) -> None:
        """Print one window's report and keep it for the summary."""
        now = dt.datetime.now(dt.UTC)
        if now - self._since < dt.timedelta(seconds=1):
            return
        window = {"elapsed": round((now - self._began).total_seconds()), **self.runs(self._since)}
        window["passes"] = {name: _spread(passes) for name, passes in sorted(self._timer.drain().items())}
        window["pods"] = dict(self._launcher.outcomes)
        self._since = now
        self.windows.append(window)
        print(_format(window), flush=True)

    def runs(self, since: dt.datetime) -> dict[str, Any]:
        """Read how the fed organisations' runs fared since an instant.

        Args:
            since: The window's start.

        Returns:
            Runs created, started and ended in the window, those open now, and
            the started ones' dispatch latency and the ended ones' duration.
        """
        row = self._connection.execute(
            "SELECT "
            "count(*) FILTER (WHERE created_at >= %(since)s), "
            "count(*) FILTER (WHERE started_at >= %(since)s), "
            "count(*) FILTER (WHERE completed_at >= %(since)s), "
            "count(*) FILTER (WHERE completed_at >= %(since)s AND status = 'failed'), "
            "count(*) FILTER (WHERE status = ANY(%(open)s)), "
            "percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM started_at - created_at)) "
            "FILTER (WHERE started_at >= %(since)s), "
            "percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM started_at - created_at)) "
            "FILTER (WHERE started_at >= %(since)s), "
            "percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM completed_at - started_at)) "
            "FILTER (WHERE completed_at >= %(since)s) "
            "FROM runs WHERE org_id = ANY(%(orgs)s) AND (created_at >= %(since)s OR status = ANY(%(open)s))",
            {"since": since, "orgs": self._orgs, "open": list(OPEN_STATUSES)},
        ).fetchone()
        self._connection.commit()
        created, started, ended, failed, open_runs, dispatch_p50, dispatch_p95, duration_p50 = row or (0,) * 8
        return {
            "created": created,
            "started": started,
            "ended": ended,
            "failed": failed,
            "open": open_runs,
            "dispatch_p50": _round(dispatch_p50),
            "dispatch_p95": _round(dispatch_p95),
            "duration_p50": _round(duration_p50),
        }


def _spread(values: list[float]) -> dict[str, float]:
    """Summarise durations.

    Args:
        values: Durations in seconds.

    Returns:
        Their count, median, 95th percentile and maximum, in milliseconds.
    """
    ordered = sorted(values)
    return {
        "count": len(ordered),
        "p50_ms": round(statistics.median(ordered) * 1000, 1),
        "p95_ms": round(ordered[int(len(ordered) * 0.95)] * 1000, 1),
        "max_ms": round(ordered[-1] * 1000, 1),
    }


def _round(value: float | None) -> float | None:
    """Round a reading to a tenth, keeping an absent one absent.

    Args:
        value: The reading.

    Returns:
        It rounded, or ``None``.
    """
    return None if value is None else round(float(value), 1)


def _format(window: dict[str, Any]) -> str:
    """Lay one window's report out for the console.

    Args:
        window: The window's readings.

    Returns:
        Its lines.
    """
    header = (
        f"[{window['elapsed']:>5}s] runs created {window['created']}, started {window['started']}, "
        f"ended {window['ended']} ({window['failed']} failed), open {window['open']} | dispatch p50 "
        f"{window['dispatch_p50']}s p95 {window['dispatch_p95']}s | run p50 {window['duration_p50']}s | "
        f"pods {window['pods']}"
    )
    lines = [header]
    for name, spread in window["passes"].items():
        lines.append(
            f"         {name:<8} calls  {spread['count']:>4}  p50 {spread['p50_ms']:>8.1f}ms  "
            f"p95 {spread['p95_ms']:>8.1f}ms  max {spread['max_ms']:>8.1f}ms"
        )
    return "\n".join(lines)


# -- Entry point ---------------------------------------------------------------


def main() -> None:
    """Run the scheduler's loops and the pods for the duration, then write the summary."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--api", default="http://localhost:3010", help="the API the pods report to")
    parser.add_argument("--orgs", type=int, default=10, help="organisations fed, in the order generated")
    parser.add_argument("--offset", type=int, default=0, help="generated organisations to skip before them")
    parser.add_argument("--live-runs", type=int, default=3, help="runs kept open in each organisation")
    parser.add_argument("--runner", default="serial", help="the runner each pod executes with")
    parser.add_argument("--duration", type=int, default=600, help="seconds to run for")
    parser.add_argument("--report", type=int, default=30, help="seconds between reports")
    args = parser.parse_args()
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    postgres = PostgresSettings.get()
    init_engine(postgres.dsn)
    catalog = il.Catalog.from_settings()
    store = Store.from_settings(catalog)
    settings = SchedulerSettings.get()
    dsn = postgres.dsn.replace("postgresql+psycopg://", "postgresql://")
    orgs = [
        row[0]
        for row in psycopg.connect(dsn)
        .execute(
            "SELECT id FROM organisations WHERE name LIKE 'Scale %%' ORDER BY name LIMIT %s OFFSET %s",
            (args.orgs, args.offset),
        )
        .fetchall()
    ]

    timer = PassTimer()
    launcher = PodLauncher(args.api, pods=args.orgs * args.live_runs * 2 + 8, runner_type=args.runner, timer=timer)
    services: dict[str, Any] = {
        name: timer.wrap(name, service)
        for name, service in scheduler_services(
            settings, store=store, catalog=catalog, cron=True, worker=False, reaper=False
        ).items()
    }
    services["worker"] = timer.wrap(
        "worker",
        QueueController(
            launcher=launcher,
            store=store,
            poll_interval=settings.worker.poll_interval,
            heartbeat_timeout=settings.reaper.heartbeat_timeout,
        ),
    )
    services["reaper"] = timer.wrap(
        "reaper",
        Reaper(
            store=store,
            launcher=launcher,
            startup_timeout=settings.reaper.startup_timeout,
            heartbeat_timeout=settings.reaper.heartbeat_timeout,
            run_timeout=settings.reaper.run_timeout,
            poll_interval=settings.reaper.poll_interval,
            credential_retention=settings.reaper.credential_retention,
        ),
    )
    feeder = Feeder(store, psycopg.connect(dsn), orgs, args.live_runs)
    reporter = Reporter(psycopg.connect(dsn), orgs, timer, launcher, args.report)
    services["feeder"] = timer.wrap("feeder", feeder)
    services["reporter"] = reporter

    supervisor = Supervisor(services)
    threading.Timer(args.duration, supervisor.stop, args=("duration reached",)).start()
    print(f"Feeding {len(orgs)} organisations, {args.live_runs} open runs each, for {args.duration}s", flush=True)
    supervisor.run()
    print("Waiting for the launched pods to end their runs...", flush=True)
    launcher.close()

    summary = {
        "arguments": vars(args),
        "backfills_created": feeder.created,
        "pods": dict(launcher.outcomes),
        "passes": {name: _spread(passes) for name, passes in sorted(timer.totals.items())},
        "windows": reporter.windows,
    }
    STATE.mkdir(exist_ok=True)
    path = STATE / f"scheduler-{dt.datetime.now(dt.UTC):%Y%m%dT%H%M%S}.json"
    path.write_text(json.dumps(summary, indent=2, default=str))
    print(json.dumps(summary["passes"], indent=2))
    print(f"Summary in {path}")


if __name__ == "__main__":
    main()
