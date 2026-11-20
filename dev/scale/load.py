#!/usr/bin/env python
"""Load the scale-test stack's API the way browsers do, and report what it costs.

Virtual users sign in with the generator's sessions and walk the app's pages,
each page making the requests the app makes for it: the app shell on arrival
(catalog, components, latest executions), the overview (overview, coverage
and the timeline's runs, refreshed every minute while it stays open), the
collection's runs list, a run's page and a component's page.

Each user also holds its organisation's realtime socket and does with it what
the app's runs store does: a run arriving over the socket enters the store,
every event of a run the store holds marks that run stale, and the stale runs
are refetched together at most once a second. That refetch is most of
production's traffic, and it grows with the runs open in an organisation
times the browsers open on it, so run ``scheduler.py`` alongside to have runs
open.

Users ramp in stages. Each stage reports every route's latency and errors,
the socket messages delivered, the containers' CPU and memory, and the
database's transactions, cache hits and costliest statements
(``pg_stat_statements``, reset at each stage)::

    uv run python scale/load.py --orgs 10 --stages 10,25,50 --stage-seconds 180
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import random
import re
import statistics
import subprocess
import sys
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlsplit

import httpx
import psycopg
from websockets.asyncio.client import connect

STATE = Path(__file__).parent / "state"
SESSIONS = STATE / "sessions.json"
CONTAINER_PREFIX = "interloper-scale-"
PAGE_WEIGHTS = {"overview": 0.35, "collection": 0.3, "run": 0.25, "component": 0.1}
TIMELINE_REFRESH = 60.0
UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


# -- Recording -----------------------------------------------------------------


@dataclass
class Sample:
    """One request's result.

    Attributes:
        seconds: How long it took, until its body was read.
        status: Its HTTP status, or ``0`` when it raised.
        size: Its body's bytes.
    """

    seconds: float
    status: int
    size: int


@dataclass
class Stage:
    """What one stage of the ramp recorded.

    Attributes:
        samples: Each route's requests.
        messages: Socket messages delivered, by table.
        refetches: Runs the realtime refetch asked for.
    """

    samples: dict[str, list[Sample]] = field(default_factory=lambda: defaultdict(list))
    messages: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    refetches: int = 0

    def routes(self, seconds: float) -> list[dict[str, Any]]:
        """Summarise every route's requests, the busiest first.

        Args:
            seconds: The stage's length, for the request rate.

        Returns:
            One row per route: requests per second, latency percentiles in
            milliseconds, mean response size and errors.
        """
        rows = []
        for route, samples in self.samples.items():
            durations = sorted(sample.seconds for sample in samples)
            rows.append(
                {
                    "route": route,
                    "count": len(samples),
                    "rps": round(len(samples) / seconds, 2),
                    "p50_ms": round(statistics.median(durations) * 1000),
                    "p95_ms": round(_percentile(durations, 0.95) * 1000),
                    "p99_ms": round(_percentile(durations, 0.99) * 1000),
                    "max_ms": round(durations[-1] * 1000),
                    "kb": round(statistics.mean(sample.size for sample in samples) / 1024, 1),
                    "errors": sum(not 200 <= sample.status < 300 for sample in samples),
                }
            )
        return sorted(rows, key=lambda row: -row["count"])


def _percentile(ordered: list[float], share: float) -> float:
    """Read a percentile off sorted values.

    Args:
        ordered: The values, sorted.
        share: The percentile, between 0 and 1.

    Returns:
        The value at that share.
    """
    return ordered[min(int(len(ordered) * share), len(ordered) - 1)]


def _route(url: str) -> str:
    """Name a request's route: its path with ids stood for, and its query's keys.

    Args:
        url: The request's path and query.

    Returns:
        The route, ``GET /organisations/{id}/runs/{id}`` style.
    """
    parts = urlsplit(url)
    path = UUID.sub("{id}", parts.path).removeprefix("/api")
    keys = sorted({key for key, _ in parse_qsl(parts.query)} - {"limit", "offset"})
    return f"{path}?{','.join(keys)}" if keys else path


# -- Users ---------------------------------------------------------------------


class Visitor:
    """One browser on one organisation, walking its pages and following its realtime socket."""

    def __init__(self, api: str, org_id: str, session: str, stage: Stage, seed: int) -> None:
        """Bind the user to its organisation and session.

        Args:
            api: The API's base URL.
            org_id: The organisation it browses.
            session: Its session token.
            stage: Where its requests are recorded, swapped at each stage.
            seed: Its random seed, so a ramp replays the same walk.
        """
        self.stage = stage
        self._api = api
        self._org = f"/api/organisations/{org_id}"
        self._socket_url = api.replace("http", "ws", 1) + f"{self._org}/ws"
        self._session = session
        self._random = random.Random(seed)
        self._held: set[str] = set()
        self._stale: set[str] = set()
        self._flush: asyncio.Task[None] | None = None
        self._components: list[str] = []
        self._client: httpx.AsyncClient | None = None

    async def run(self, stop: asyncio.Event) -> None:
        """Browse until told to stop.

        Args:
            stop: Set when the load ends.
        """
        await asyncio.sleep(self._random.uniform(0, 10))
        limits = httpx.Limits(max_connections=6, max_keepalive_connections=6)
        async with httpx.AsyncClient(
            base_url=self._api, cookies={"session_token": self._session}, timeout=120, limits=limits
        ) as client:
            self._client = client
            socket = asyncio.create_task(self._listen(stop))
            await self._visit("shell")
            page = "overview"
            while not stop.is_set():
                await self._visit(page)
                await self._dwell(page, stop)
                page = self._random.choices(list(PAGE_WEIGHTS), weights=list(PAGE_WEIGHTS.values()))[0]
            socket.cancel()
            if self._flush is not None:
                self._flush.cancel()

    async def _visit(self, page: str) -> None:
        """Load a page, carrying on past a failure as a browser shows it and stays.

        Args:
            page: The page's name, ``shell`` for the app shell.
        """
        try:
            await getattr(self, f"_{page}")()
        except Exception as error:  # noqa: BLE001 - counted, a browser stays on an errored page
            self.stage.messages[f"{page} failed: {type(error).__name__}"] += 1

    # -- Pages -----------------------------------------------------------------

    async def _shell(self) -> None:
        """Load what the app shell loads on entering the organisation."""
        await asyncio.gather(
            self._get(f"{self._org}/catalog"),
            self._get(f"{self._org}/catalog/resource-kinds"),
            self._get("/api/oauth/providers"),
            self._get_all(f"{self._org}/executions", latest="true"),
        )
        components = await self._get_all(f"{self._org}/components")
        self._components = [component["id"] for component in components]

    async def _overview(self) -> None:
        """Load the overview page: the overview, its coverage and the timeline."""
        today = dt.datetime.now(dt.UTC).date()
        await asyncio.gather(
            self._get(f"{self._org}/overview"),
            self._get(f"{self._org}/overview/coverage?since={today - dt.timedelta(days=90)}&until={today}"),
            self._timeline(),
        )

    async def _timeline(self) -> None:
        """Load the timeline's window of runs, the last sixteen hours."""
        now = dt.datetime.now(dt.UTC)
        after = (now - dt.timedelta(hours=16)).isoformat().replace("+00:00", "Z")
        before = now.isoformat().replace("+00:00", "Z")
        await self._get(f"{self._org}/runs?after={after}&before={before}&limit=500&offset=0")

    async def _collection(self) -> None:
        """Load the collection page's runs list, which the runs store then holds."""
        page = await self._get(f"{self._org}/runs?limit=50&offset=0")
        self._held.update(run["id"] for run in (page or {}).get("items", []))

    async def _run(self) -> None:
        """Open one held run's page: the run, its executions and its events."""
        if not self._held:
            await self._collection()
        if not self._held:
            return
        run = self._random.choice(sorted(self._held))
        await asyncio.gather(
            self._get(f"{self._org}/runs/{run}"),
            self._get_all(f"{self._org}/runs/{run}/executions"),
            self._get(f"{self._org}/runs/{run}/events?limit=100&offset=0"),
        )

    async def _component(self) -> None:
        """Open one component's page."""
        if self._components:
            await self._get(f"{self._org}/components/{self._random.choice(self._components)}")

    async def _dwell(self, page: str, stop: asyncio.Event) -> None:
        """Stay on a page for a while, refreshing the timeline while it shows.

        Args:
            page: The page shown.
            stop: Set when the load ends.
        """
        until = time.monotonic() + self._random.uniform(20, 90)
        while not stop.is_set() and time.monotonic() < until:
            wait = min(TIMELINE_REFRESH, until - time.monotonic())
            try:
                await asyncio.wait_for(stop.wait(), timeout=max(wait, 0))
            except TimeoutError:
                if page == "overview" and time.monotonic() < until:
                    await self._timeline()

    # -- Realtime --------------------------------------------------------------

    async def _listen(self, stop: asyncio.Event) -> None:
        """Follow the organisation's realtime socket, reconnecting as the app does.

        Args:
            stop: Set when the load ends.
        """
        delay = 1.0
        while not stop.is_set():
            try:
                async with connect(
                    self._socket_url, additional_headers={"Cookie": f"session_token={self._session}"}, max_size=None
                ) as socket:
                    delay = 1.0
                    async for raw in socket:
                        self._receive(json.loads(raw))
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - the app reconnects on any close
                self.stage.messages["reconnects"] += 1
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)

    def _receive(self, message: dict[str, Any]) -> None:
        """Do with a realtime message what the runs store does.

        Args:
            message: The message: its table and its record.
        """
        table = message.get("table", "?")
        record = message.get("record") or {}
        self.stage.messages[table] += 1
        if table == "runs" and record.get("id"):
            self._held.add(record["id"])
        elif table == "events" and record.get("run_id") in self._held:
            self._stale.add(record["run_id"])
            if self._flush is None:
                self._flush = asyncio.get_running_loop().create_task(self._refetch())

    async def _refetch(self) -> None:
        """Refetch the stale runs together, a second after the first went stale."""
        await asyncio.sleep(1)
        stale, self._stale, self._flush = self._stale, set(), None
        self.stage.refetches += len(stale)
        await asyncio.gather(*(self._get(f"{self._org}/runs/{run}") for run in stale), return_exceptions=True)

    # -- Requests --------------------------------------------------------------

    async def _get(self, url: str) -> Any:
        """Request a URL and record how it went.

        Args:
            url: The path and query.

        Returns:
            The decoded JSON body, or ``None`` when the request failed.
        """
        assert self._client is not None
        started = time.perf_counter()
        try:
            response = await self._client.get(url)
        except httpx.HTTPError:
            self.stage.samples[_route(url)].append(Sample(time.perf_counter() - started, 0, 0))
            return None
        self.stage.samples[_route(url)].append(
            Sample(time.perf_counter() - started, response.status_code, len(response.content))
        )
        return response.json() if response.is_success else None

    async def _get_all(self, path: str, **params: str) -> list[dict[str, Any]]:
        """Request every page of a listing, as the app's ``fetchAll`` does.

        Args:
            path: The listing's path.
            **params: Its filters.

        Returns:
            Every item, or those read before a page failed.
        """
        items: list[dict[str, Any]] = []
        while True:
            query = "&".join([*(f"{key}={value}" for key, value in params.items()), f"limit=500&offset={len(items)}"])
            page = await self._get(f"{path}?{query}")
            if not page:
                return items
            items.extend(page["items"])
            if not page["items"] or len(items) >= page["total"]:
                return items


# -- Monitoring ----------------------------------------------------------------


class Monitor:
    """Samples the containers' resources and reads the database's statistics."""

    def __init__(self, database: str) -> None:
        """Connect to the database and prepare its statistics.

        Args:
            database: The database's connection string.
        """
        self._connection = psycopg.connect(database, autocommit=True)
        self._connection.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        self._samples: dict[str, list[tuple[float, float]]] = defaultdict(list)
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._baseline: tuple[Any, ...] = ()
        threading.Thread(target=self._sample_containers, daemon=True).start()

    def begin(self) -> None:
        """Start a stage: drop the samples and statement statistics, and note the counters."""
        with self._lock:
            self._samples.clear()
        self._connection.execute("SELECT pg_stat_statements_reset()")
        self._baseline = self._database_counters()

    def end(self, seconds: float) -> dict[str, Any]:
        """Read what a stage cost.

        Args:
            seconds: The stage's length, for the rates.

        Returns:
            The containers' CPU and memory, the database's rates and cache
            hits, and its costliest statements.
        """
        with self._lock:
            containers = {
                name.removeprefix(CONTAINER_PREFIX): {
                    "cpu_mean": round(statistics.mean(cpu for cpu, _ in samples)),
                    "cpu_max": round(max(cpu for cpu, _ in samples)),
                    "memory_max_mb": round(max(memory for _, memory in samples)),
                }
                for name, samples in sorted(self._samples.items())
            }
        counters = self._database_counters()
        commits, hits, reads, returned = (now - before for now, before in zip(counters, self._baseline, strict=True))
        statements = self._connection.execute(
            "SELECT calls, round(total_exec_time::numeric / 1000, 1), round(mean_exec_time::numeric, 1), "
            "shared_blks_read, left(regexp_replace(query, '\\s+', ' ', 'g'), 160) FROM pg_stat_statements "
            "WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database()) "
            "ORDER BY total_exec_time DESC LIMIT 12"
        ).fetchall()
        size = self._connection.execute("SELECT pg_database_size(current_database()) / 1048576").fetchone()
        return {
            "containers": containers,
            "database": {
                "commits_per_s": round(commits / seconds, 1),
                "rows_per_s": round(returned / seconds),
                "cache_hit": round(hits / (hits + reads), 4) if hits + reads else None,
                "blocks_read_per_s": round(reads / seconds, 1),
                "size_mb": size[0] if size else None,
            },
            "statements": [
                {"calls": calls, "total_s": float(total), "mean_ms": float(mean), "blocks_read": read, "query": text}
                for calls, total, mean, read, text in statements
            ],
        }

    def close(self) -> None:
        """Stop sampling."""
        self._stop.set()

    def _database_counters(self) -> tuple[Any, ...]:
        """Read the database's cumulative counters.

        Returns:
            Commits, buffer hits, blocks read and rows returned so far.
        """
        row = self._connection.execute(
            "SELECT xact_commit, blks_hit, blks_read, tup_returned FROM pg_stat_database "
            "WHERE datname = current_database()"
        ).fetchone()
        return tuple(row or (0, 0, 0, 0))

    def _sample_containers(self) -> None:
        """Sample the stack's containers' CPU (percent of a core) and memory (MiB) until stopped."""
        while not self._stop.is_set():
            output = subprocess.run(
                ["docker", "stats", "--no-stream", "--format", "{{json .}}"], check=True, capture_output=True, text=True
            ).stdout
            with self._lock:
                for line in output.splitlines():
                    stats = json.loads(line)
                    if stats["Name"].startswith(CONTAINER_PREFIX):
                        cpu = float(stats["CPUPerc"].rstrip("%"))
                        self._samples[stats["Name"]].append((cpu, _mebibytes(stats["MemUsage"].split("/")[0])))
            self._stop.wait(3)


def _mebibytes(text: str) -> float:
    """Read a Docker memory figure in MiB.

    Args:
        text: The figure, ``345.6MiB`` style.

    Returns:
        It in MiB.
    """
    value = text.strip()
    for unit, factor in (("GiB", 1024), ("MiB", 1), ("KiB", 1 / 1024), ("B", 1 / 1048576)):
        if value.endswith(unit):
            return float(value.removesuffix(unit)) * factor
    return 0.0


# -- Reporting -----------------------------------------------------------------


def _print_stage(report: dict[str, Any]) -> None:
    """Lay one stage's report out for the console.

    Args:
        report: The stage's readings.
    """
    print(
        f"\n== {report['users']} users over {report['orgs']} organisations, {report['seconds']}s "
        f"(driver lag p95 {report['driver_lag_ms']['p95']}ms, max {report['driver_lag_ms']['max']}ms) =="
    )
    print(f"{'route':<58}{'rps':>7}{'p50':>7}{'p95':>7}{'p99':>7}{'max':>7}{'KB':>8}{'err':>5}")
    for row in report["routes"]:
        print(
            f"{row['route'][:57]:<58}{row['rps']:>7}{row['p50_ms']:>7}{row['p95_ms']:>7}{row['p99_ms']:>7}"
            f"{row['max_ms']:>7}{row['kb']:>8}{row['errors']:>5}"
        )
    print(f"socket messages {report['messages']}, runs refetched {report['refetches']}")
    for name, container in report["containers"].items():
        print(
            f"{name:<14} cpu mean {container['cpu_mean']:>4}% max {container['cpu_max']:>4}%  "
            f"memory max {container['memory_max_mb']:>5} MiB"
        )
    print(f"database {report['database']}")
    for statement in report["statements"][:8]:
        print(
            f"  {statement['total_s']:>7}s {statement['calls']:>7}x {statement['mean_ms']:>8}ms  {statement['query']}"
        )
    sys.stdout.flush()


# -- Entry point ---------------------------------------------------------------


async def _sleep_measuring_lag(seconds: float) -> list[float]:
    """Sleep through a stage, measuring how late the event loop wakes.

    A loop that wakes late is a driver too busy to send its requests on time,
    whose latencies then overstate the API's.

    Args:
        seconds: How long to sleep.

    Returns:
        How late each half-second wake was, in seconds.
    """
    lags = []
    deadline = time.monotonic() + seconds
    while (remaining := deadline - time.monotonic()) > 0:
        step = min(0.5, remaining)
        started = time.monotonic()
        await asyncio.sleep(step)
        lags.append(time.monotonic() - started - step)
    return lags


async def _ramp(args: argparse.Namespace) -> list[dict[str, Any]]:
    """Ramp the users stage by stage and report each stage.

    Args:
        args: The parsed command line.

    Returns:
        Each stage's report.
    """
    organisations = json.loads(SESSIONS.read_text())["organisations"][args.offset : args.offset + args.orgs]
    monitor = Monitor(args.database)
    stop = asyncio.Event()
    visitors: list[Visitor] = []
    tasks: list[asyncio.Task[None]] = []
    reports = []
    for users in args.stages:
        stage = Stage()
        for visitor in visitors:
            visitor.stage = stage
        for index in range(len(visitors), users):
            organisation = organisations[index % len(organisations)]
            sessions = organisation["sessions"]
            session = sessions[(index // len(organisations)) % len(sessions)]
            visitor = Visitor(args.api, organisation["id"], session, stage, seed=index)
            visitors.append(visitor)
            tasks.append(asyncio.create_task(visitor.run(stop)))
        monitor.begin()
        started = time.monotonic()
        lags = sorted(await _sleep_measuring_lag(args.stage_seconds))
        seconds = time.monotonic() - started
        report = {
            "users": users,
            "orgs": len(organisations),
            "seconds": round(seconds),
            "driver_lag_ms": {"p95": round(_percentile(lags, 0.95) * 1000), "max": round(lags[-1] * 1000)},
            "routes": stage.routes(seconds),
            "messages": dict(stage.messages),
            "refetches": stage.refetches,
            **await asyncio.to_thread(monitor.end, seconds),
        }
        _print_stage(report)
        reports.append(report)
    stop.set()
    await asyncio.gather(*tasks, return_exceptions=True)
    monitor.close()
    return reports


def main() -> None:
    """Run the ramp and write its reports to ``state/``."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--api", default="http://localhost:3010", help="the API, behind the stack's proxy")
    parser.add_argument(
        "--database", default="postgresql://postgres:postgres@localhost:5434/interloper", help="the scale database"
    )
    parser.add_argument("--orgs", type=int, default=10, help="organisations browsed, in the order generated")
    parser.add_argument("--offset", type=int, default=0, help="generated organisations to skip before them")
    parser.add_argument(
        "--stages", type=lambda text: [int(users) for users in text.split(",")], default=[5, 10, 25], help="users"
    )
    parser.add_argument("--stage-seconds", type=int, default=180, help="seconds each stage lasts")
    args = parser.parse_args()
    reports = asyncio.run(_ramp(args))
    STATE.mkdir(exist_ok=True)
    path = STATE / f"load-{dt.datetime.now(dt.UTC):%Y%m%dT%H%M%S}.json"
    path.write_text(json.dumps({"arguments": vars(args), "stages": reports}, indent=2, default=str))
    print(f"\nReports in {path}")


if __name__ == "__main__":
    main()
