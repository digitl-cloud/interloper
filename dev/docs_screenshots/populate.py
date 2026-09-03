"""Populate a throwaway database with the demo content the documentation screenshots show.

Run after ``dev/seed.py`` against the same database (both read the connection
from ``INTERLOPER_POSTGRES_*``). Idempotent on component names: re-running adds
nothing that already exists. Also mints a session for the seeded dev user and
writes its token next to this file, for ``shoot.mjs``.
"""

from __future__ import annotations

import datetime as dt
import os
import random
import time
from pathlib import Path

import interloper as il
import psycopg2
from interloper.settings import AppSettings
from interloper_db import init_engine
from interloper_db.engine import get_engine
from interloper_db.models import Organisation, Profile
from interloper_db.store import Store
from sqlmodel import Session, select

HERE = Path(__file__).parent
DEV_USER_EMAIL = os.environ.get("INTERLOPER_DEV_USER_EMAIL", "admin@dev.local")


def main() -> None:
    """Create the demo components, run them, and spread the runs over the last day."""
    settings = AppSettings.get()
    init_engine(settings.postgres.dsn)
    store = Store.from_settings(il.Catalog.from_settings())

    with Session(get_engine()) as session:
        org = session.exec(select(Organisation).where(Organisation.name == "Dev Org")).one()
        profile = session.exec(select(Profile).where(Profile.email == DEV_USER_EMAIL)).one()
        org_id, user_id = org.id, profile.id

    components = {c.name: c for c in store.components.list_all(org_id) if c.name}

    def ensure(kind: str, key: str, name: str, **kwargs):  # noqa: ANN202
        if name not in components:
            components[name] = store.components.create(org_id, kind=kind, key=key, name=name, **kwargs)
            print("created", kind, name)
        return components[name]

    csv_dest = ensure("destination", "csv_destination", "Local CSV", config={"base_path": "/tmp/interloper-docs"})
    ensure("destination", "memory_destination", "Scratch memory")

    demo = components["Demo Data"]
    store.components.update(demo.id, relations={"destination": [(csv_dest.id, "")]})
    flaky = ensure(
        "source",
        "demo_source",
        "Demo Flaky",
        config={"hello": "chaos", "random_failure_probability": 0.35, "dataset": "demo_flaky"},
        relations={"destination": [(csv_dest.id, "")]},
    )
    monthly = ensure("source", "demo_monthly_source", "Demo Monthly", relations={"destination": [(csv_dest.id, "")]})

    daily_job = components["Demo Daily"]
    monthly_job = ensure(
        "job",
        "cron_job",
        "Demo Monthly Rollup",
        config={"cron": "0 7 1 * *", "timezone": "Europe/Berlin", "lookback": 1, "offset": 1, "enabled": True},
        relations={"target": [(monthly.id, "")]},
    )
    flaky_job = ensure(
        "job",
        "cron_job",
        "Demo Flaky Nightly",
        config={"cron": "30 2 * * *", "timezone": "UTC", "lookback": 3, "offset": 1, "enabled": True},
        relations={"target": [(flaky.id, "")]},
    )
    ensure(
        "hook",
        "webhook_hook",
        "Alert ops on failure",
        config={
            "url": "https://ops.example.com/interloper/alerts",
            "timeout": 10.0,
            "events": ["run_failed"],
            "enabled": True,
        },
        relations={"watch": [(flaky_job.id, ""), (daily_job.id, "")]},
    )
    ensure(
        "hook",
        "trigger_hook",
        "Roll up after daily",
        config={"events": ["run_completed"], "enabled": True},
        relations={"watch": [(daily_job.id, "")], "target": [(monthly_job.id, "")]},
    )

    if not store.runs.list_backfills(org_id):
        store.runs.create_backfill(
            org_id, component_id=demo.id, start_key="2026-08-25", end_key="2026-09-01", concurrency=3
        )
        store.runs.create_backfill(
            org_id, component_id=flaky.id, start_key="2026-08-28", end_key="2026-09-01", concurrency=2
        )
        store.runs.create_backfill(
            org_id, component_id=monthly.id, start_key="2026-05", end_key="2026-08", concurrency=2
        )
        store.runs.create(org_id, component_id=daily_job.id, partition_key="2026-09-02")
        store.runs.create(org_id, component_id=flaky_job.id, partition_key="2026-09-02")
        store.runs.create(org_id, component_id=monthly_job.id, partition_key="2026-08")
        print("runs queued; waiting for the worker")
        _wait_and_backdate(settings.postgres.dsn)

    (HERE / "session_token").write_text(store.auth.create_session(user_id, org_id))
    print("session minted")


def _wait_and_backdate(dsn: str) -> None:
    """Wait for every run to finish, then spread them over the last 22 hours.

    The runs all execute within a minute, which makes the timeline a single
    cluster of bars; shifting each run (and its events) back by a distinct
    offset gives the screenshot a day of history. Relative durations are kept.
    """
    connection = psycopg2.connect(dsn)
    connection.autocommit = True
    cursor = connection.cursor()
    for _ in range(120):
        cursor.execute("select count(*) from runs where status in ('queued', 'pending', 'dispatched', 'running')")
        if cursor.fetchone()[0] == 0:
            break
        time.sleep(5)
    cursor.execute("select id from runs order by created_at")
    run_ids = [row[0] for row in cursor.fetchall()]
    random.seed(7)
    for index, run_id in enumerate(run_ids):
        hours_back = 22 * (len(run_ids) - 1 - index) / max(len(run_ids) - 1, 1) + random.uniform(0, 0.4)
        delta = dt.timedelta(hours=hours_back)
        cursor.execute(
            "update runs set created_at=created_at-%s, started_at=started_at-%s, completed_at=completed_at-%s "
            "where id=%s",
            (delta, delta, delta, run_id),
        )
        cursor.execute("update events set timestamp=timestamp-%s where run_id=%s", (delta, run_id))
    cursor.execute(
        "update backfills b set created_at=sub.mn, started_at=sub.mn, completed_at=sub.mx "
        "from (select backfill_id, min(created_at) mn, max(completed_at) mx from runs "
        "where backfill_id is not null group by backfill_id) sub where sub.backfill_id=b.id"
    )
    cursor.execute("update runs set completed_at = completed_at + (interval '1 minute' * (3 + floor(random()*12)))")


if __name__ == "__main__":
    main()
