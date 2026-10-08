"""Tests for the hook evaluator (``interloper_scheduler.hooks``).

SQLite stands in for Postgres; ``EventStore.save`` (a pg-dialect upsert) is
replaced with a plain insert so the claim-dedup reads work.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from typing import Any
from uuid import UUID, uuid4

import interloper as il
import pytest
from interloper.settings import AppSettings, ServerSettings
from interloper_assets.demo.source import DemoSource, demo_asset
from interloper_db import RunStatus, Store
from interloper_db import engine as engine_module
from interloper_db.models import Backfill, Component, ComponentRelation, Organisation, Quota, Run, Usage
from interloper_db.models import Event as EventRow
from sqlalchemy import event
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, select

from interloper_scheduler.hooks import HookController

_ORG = uuid4()
_PAST = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)


@pytest.fixture
def store(monkeypatch: pytest.MonkeyPatch) -> Iterator[Store]:
    """A store over an in-memory database, with a SQLite-friendly ``save``.

    Yields:
        The store bound to that database, disposed once the test finishes.
    """
    eng = engine_module.init_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(eng, "connect")
    def _sqlite_uuid(dbapi_connection: Any, _record: Any) -> None:
        dbapi_connection.create_function("gen_random_uuid", 0, lambda: uuid4().hex)

    for model in (Organisation, Component, ComponentRelation, Backfill, Run, EventRow, Quota, Usage):
        model.__table__.create(eng)  # ty: ignore[unresolved-attribute]

    store = Store(catalog=il.Catalog.from_assets([DemoSource, demo_asset]))

    try:
        yield store
    finally:
        eng.dispose()
        engine_module._engine = None


def _terminal_run(store: Store, component_id: UUID, *, status: RunStatus = RunStatus.SUCCESS) -> Run:
    run = store.runs.create(_ORG, component_id=component_id, partition_key="2026-07-06")
    with Session(engine_module.get_engine()) as session:
        db_run = session.get(Run, run.id)
        assert db_run is not None
        db_run.status = status
        db_run.completed_at = dt.datetime.now(dt.timezone.utc)
        session.add(db_run)
        session.commit()
        session.refresh(db_run)
        return db_run


def _capture_posts(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Capture WebhookHook payloads, answering every POST with 200.

    Returns:
        The list the captured payloads accumulate into.
    """
    import httpx2

    payloads: list[dict[str, Any]] = []

    def fake_post(url: str, *, json: dict[str, Any], **kwargs: Any) -> httpx2.Response:
        payloads.append(json)
        return httpx2.Response(200, request=httpx2.Request("POST", url))

    monkeypatch.setattr(httpx2, "post", fake_post)
    return payloads


def _record_run_failure(run_id: UUID, error: str) -> None:
    """Insert the ``run_failed`` event row that carries a run's error text."""
    with Session(engine_module.get_engine()) as session:
        session.add(
            EventRow(
                id=uuid4(),
                org_id=_ORG,
                run_id=run_id,
                event_type="run_failed",
                error=error,
                timestamp=dt.datetime.now(dt.timezone.utc),
            )
        )
        session.commit()


def _sweep(store: Store) -> HookController:
    controller = HookController(store=store, poll_interval=999)
    controller._tick()
    return controller


def _evaluated_at(run_id: UUID) -> dt.datetime | None:
    with Session(engine_module.get_engine()) as session:
        db_run = session.get(Run, run_id)
        assert db_run is not None
        return db_run.hooks_evaluated_at


class TestHookEvaluation:
    def test_trigger_hook_cascades_with_partition(self, store: Store):
        source = store.components.create(_ORG, kind="source", key="demo_source", name="Demo")
        leaf = next(c for c in source.children if c.key == "e")
        root = next(c for c in source.children if c.key == "a")
        hook = store.components.create(
            _ORG,
            kind="hook",
            key="trigger_hook",
            name="Cascade",
            config={"events": ["run_completed"]},
            relations={"watches": [leaf.id], "targets": [root.id]},
        )
        run = _terminal_run(store, leaf.id)

        _sweep(store)

        with Session(engine_module.get_engine()) as session:
            queued = session.exec(select(Run).where(Run.status == "queued")).all()
            assert len(queued) == 1
            assert queued[0].component_id == root.id
            assert queued[0].partition_key == run.partition_key
            events = session.exec(select(EventRow)).all()
            assert [e.event_type for e in events] == ["hook_fired"]
            assert events[0].run_id == run.id
            # The claim carries the firing hook's identity, queryable per hook.
            assert events[0].component_id == hook.id
            assert events[0].component_kind == "hook"
            assert events[0].component_key == "trigger_hook"

    def test_claim_prevents_refiring(self, store: Store):
        source = store.components.create(_ORG, kind="source", key="demo_source", name="Demo")
        leaf = next(c for c in source.children if c.key == "e")
        root = next(c for c in source.children if c.key == "a")
        store.components.create(
            _ORG,
            kind="hook",
            key="trigger_hook",
            name="Cascade",
            config={"events": ["run_completed"]},
            relations={"watches": [leaf.id], "targets": [root.id]},
        )
        run = _terminal_run(store, leaf.id)

        _sweep(store)
        # A crash between the claim and the stamp re-evaluates the run: the
        # claim is what keeps it from firing twice.
        with Session(engine_module.get_engine()) as session:
            db_run = session.get(Run, run.id)
            assert db_run is not None
            db_run.hooks_evaluated_at = None
            session.add(db_run)
            session.commit()
        _sweep(store)

        with Session(engine_module.get_engine()) as session:
            assert len(session.exec(select(Run).where(Run.status == "queued")).all()) == 1
            assert len(session.exec(select(EventRow)).all()) == 1

    def test_event_type_mismatch_does_not_fire(self, store: Store):
        source = store.components.create(_ORG, kind="source", key="demo_source", name="Demo")
        leaf = next(c for c in source.children if c.key == "e")
        root = next(c for c in source.children if c.key == "a")
        store.components.create(
            _ORG,
            kind="hook",
            key="trigger_hook",
            name="OnFailureOnly",
            config={"events": ["run_failed"]},
            relations={"watches": [leaf.id], "targets": [root.id]},
        )
        _terminal_run(store, leaf.id, status=RunStatus.SUCCESS)

        _sweep(store)

        with Session(engine_module.get_engine()) as session:
            assert session.exec(select(Run).where(Run.status == "queued")).all() == []
            assert session.exec(select(EventRow)).all() == []

    def test_watching_parent_matches_child_run(self, store: Store, monkeypatch: pytest.MonkeyPatch):
        import httpx2

        posted: list[str] = []

        def fake_post(url: str, **kwargs: Any) -> httpx2.Response:
            posted.append(url)
            return httpx2.Response(200, request=httpx2.Request("POST", url))

        monkeypatch.setattr(httpx2, "post", fake_post)

        source = store.components.create(_ORG, kind="source", key="demo_source", name="Demo")
        child = next(c for c in source.children if c.key == "a")
        store.components.create(
            _ORG,
            kind="hook",
            key="webhook_hook",
            name="OnAnyAsset",
            config={"events": ["run_completed"], "url": "https://example.test/n"},
            relations={"watches": [source.id]},
        )
        _terminal_run(store, child.id)

        _sweep(store)

        assert posted == ["https://example.test/n"]

    def test_failure_recorded_on_claim(self, store: Store, monkeypatch: pytest.MonkeyPatch):
        import httpx2

        def boom(*args: Any, **kwargs: Any) -> None:
            raise httpx2.ConnectError("no route")

        monkeypatch.setattr(httpx2, "post", boom)

        source = store.components.create(_ORG, kind="source", key="demo_source", name="Demo")
        store.components.create(
            _ORG,
            kind="hook",
            key="webhook_hook",
            name="Notify",
            config={"events": ["run_failed"], "url": "https://example.test/x"},
            relations={"watches": [source.id]},
        )
        run = _terminal_run(store, source.id, status=RunStatus.FAILED)

        _sweep(store)
        _sweep(store)  # failure claim is terminal: no retry

        with Session(engine_module.get_engine()) as session:
            events = session.exec(select(EventRow)).all()
            assert [e.event_type for e in events] == ["hook_failed"]
            assert events[0].error is not None and "no route" in events[0].error
            hook_row = session.exec(select(Component).where(Component.kind == "hook")).one()
            assert (hook_row.state or {}).get("last_run_id") == str(run.id)
            assert "no route" in (hook_row.state or {}).get("last_error", "")

    def test_a_failure_without_a_message_is_still_a_failure(self, store: Store, monkeypatch: pytest.MonkeyPatch):
        import httpx2

        def boom(*args: Any, **kwargs: Any) -> None:
            raise TimeoutError

        monkeypatch.setattr(httpx2, "post", boom)
        source = store.components.create(_ORG, kind="source", key="demo_source", name="Demo")
        store.components.create(
            _ORG,
            kind="hook",
            key="webhook_hook",
            name="Notify",
            config={"events": ["run_failed"], "url": "https://example.test/x"},
            relations={"watches": [source.id]},
        )
        _terminal_run(store, source.id, status=RunStatus.FAILED)

        _sweep(store)

        with Session(engine_module.get_engine()) as session:
            assert [e.event_type for e in session.exec(select(EventRow)).all()] == ["hook_failed"]
            hook_row = session.exec(select(Component).where(Component.kind == "hook")).one()
            assert (hook_row.state or {}).get("last_error") == "TimeoutError"

    def test_success_clears_last_error(self, store: Store, monkeypatch: pytest.MonkeyPatch):
        import httpx2

        def boom(*args: Any, **kwargs: Any) -> None:
            raise httpx2.ConnectError("no route")

        monkeypatch.setattr(httpx2, "post", boom)
        source = store.components.create(_ORG, kind="source", key="demo_source", name="Demo")
        store.components.create(
            _ORG,
            kind="hook",
            key="webhook_hook",
            name="Notify",
            config={"events": ["run_completed"], "url": "https://example.test/x"},
            relations={"watches": [source.id]},
        )
        _terminal_run(store, source.id)
        _sweep(store)
        _capture_posts(monkeypatch)
        run = _terminal_run(store, source.id)

        _sweep(store)

        with Session(engine_module.get_engine()) as session:
            hook_row = session.exec(select(Component).where(Component.kind == "hook")).one()
            assert hook_row.state is not None
            assert hook_row.state["last_run_id"] == str(run.id)
            assert "last_error" in hook_row.state
            assert hook_row.state["last_error"] is None

    def test_self_targeting_trigger_is_refused(self, store: Store):
        source = store.components.create(_ORG, kind="source", key="demo_source", name="Demo")
        # Watches the source AND targets it: would loop forever without the guard.
        store.components.create(
            _ORG,
            kind="hook",
            key="trigger_hook",
            name="Ouroboros",
            config={"events": ["run_completed"]},
            relations={"watches": [source.id], "targets": [source.id]},
        )
        _terminal_run(store, source.id)

        _sweep(store)

        with Session(engine_module.get_engine()) as session:
            assert session.exec(select(Run).where(Run.status == "queued")).all() == []
            events = session.exec(select(EventRow)).all()
            assert [e.event_type for e in events] == ["hook_failed"]
            assert events[0].error is not None and "loop" in events[0].error

    def test_metadata_carries_component_identity(self, store: Store, monkeypatch: pytest.MonkeyPatch):
        payloads = _capture_posts(monkeypatch)

        source = store.components.create(_ORG, kind="source", key="demo_source", name="Demo")
        store.components.create(
            _ORG,
            kind="hook",
            key="webhook_hook",
            name="Notify",
            config={"events": ["run_completed"], "url": "https://example.test/n"},
            relations={"watches": [source.id]},
        )
        _terminal_run(store, source.id)

        _sweep(store)

        assert payloads[0]["metadata"] == {
            "status": "success",
            "organisation_name": None,
            "component_name": "Demo",
            "component_key": "demo_source",
            "attempt": 1,
            "attempts": 1,
        }

    def test_metadata_names_the_organisation(self, store: Store, monkeypatch: pytest.MonkeyPatch):
        payloads = _capture_posts(monkeypatch)
        organisation = store.organisations.create("Swarovski")
        source = store.components.create(organisation.id, kind="source", key="demo_source", name="Demo")
        store.components.create(
            organisation.id,
            kind="hook",
            key="webhook_hook",
            name="Notify",
            config={"events": ["run_completed"], "url": "https://example.test/n"},
            relations={"watches": [source.id]},
        )
        run = store.runs.create(organisation.id, component_id=source.id)
        store.runs.complete(run.id, success=True)

        _sweep(store)

        assert payloads[0]["metadata"]["organisation_name"] == "Swarovski"

    def test_metadata_falls_back_to_key_when_unnamed(self, store: Store, monkeypatch: pytest.MonkeyPatch):
        payloads = _capture_posts(monkeypatch)

        source = store.components.create(_ORG, kind="source", key="demo_source", name="Demo")
        store.components.create(
            _ORG,
            kind="hook",
            key="webhook_hook",
            name="Notify",
            config={"events": ["run_completed"], "url": "https://example.test/n"},
            relations={"watches": [source.id]},
        )
        # components.create always derives a name, so the nullable column is
        # the only way the fallback is reachable.
        with Session(engine_module.get_engine()) as session:
            row = session.get(Component, source.id)
            assert row is not None
            row.name = None
            session.add(row)
            session.commit()
        _terminal_run(store, source.id)

        _sweep(store)

        assert payloads[0]["metadata"]["component_name"] == "demo_source"

    def test_failure_metadata_carries_the_run_error(self, store: Store, monkeypatch: pytest.MonkeyPatch):
        payloads = _capture_posts(monkeypatch)

        source = store.components.create(_ORG, kind="source", key="demo_source", name="Demo")
        store.components.create(
            _ORG,
            kind="hook",
            key="webhook_hook",
            name="Notify",
            config={"events": ["run_failed"], "url": "https://example.test/n"},
            relations={"watches": [source.id]},
        )
        run = _terminal_run(store, source.id, status=RunStatus.FAILED)
        # The error text lives on the run's event rows, not the run itself.
        _record_run_failure(run.id, "HTTPStatusError: 429 Too Many Requests")

        _sweep(store)

        assert payloads[0]["metadata"]["error"] == "HTTPStatusError: 429 Too Many Requests"

    def test_success_metadata_has_no_error(self, store: Store, monkeypatch: pytest.MonkeyPatch):
        payloads = _capture_posts(monkeypatch)

        source = store.components.create(_ORG, kind="source", key="demo_source", name="Demo")
        store.components.create(
            _ORG,
            kind="hook",
            key="webhook_hook",
            name="Notify",
            config={"events": ["run_completed"], "url": "https://example.test/n"},
            relations={"watches": [source.id]},
        )
        _terminal_run(store, source.id)

        _sweep(store)

        assert "error" not in payloads[0]["metadata"]

    def test_chain_to_unwatched_target_is_allowed(self, store: Store):
        source = store.components.create(_ORG, kind="source", key="demo_source", name="Demo")
        leaf = next(c for c in source.children if c.key == "e")
        root = next(c for c in source.children if c.key == "a")
        # Watches only the leaf asset; targets the root asset: no loop possible
        # through this hook (the triggered run never matches its watch set...
        # except via the shared parent — which is exactly what the guard checks).
        store.components.create(
            _ORG,
            kind="hook",
            key="trigger_hook",
            name="Chain",
            config={"events": ["run_completed"]},
            relations={"watches": [leaf.id], "targets": [root.id]},
        )
        _terminal_run(store, leaf.id)

        _sweep(store)

        with Session(engine_module.get_engine()) as session:
            queued = session.exec(select(Run).where(Run.status == "queued")).all()
            assert [q.component_id for q in queued] == [root.id]


class TestVerdictGating:
    """A hook observes a stack's verdict, never one of its attempts."""

    def _job(self, **retry: Any) -> UUID:
        """A job row declaring a retry policy, inserted directly.

        ``store.components.create`` would build the component; this only needs
        the row, which is all the run level and the hook sweep read.

        Returns:
            The component id.
        """
        with Session(engine_module.get_engine()) as session:
            row = Component(
                id=uuid4(),
                org_id=_ORG,
                kind="job",
                key="cron_job",
                name="Nightly",
                config={"cron": "0 6 * * *", **({"retry": retry} if retry else {})},
            )
            session.add(row)
            session.commit()
            assert row.id is not None
            return row.id

    def _watching_hook(self, store: Store, component_id: UUID) -> UUID:
        hook = store.components.create(
            _ORG,
            kind="hook",
            key="webhook_hook",
            name="Notify",
            config={"events": ["run_completed", "run_failed"], "url": "https://example.invalid/hook"},
            relations={"watches": [component_id]},
        )
        return hook.id

    def _fired(self, run_id: UUID) -> list[EventRow]:
        with Session(engine_module.get_engine()) as session:
            return list(
                session.exec(select(EventRow).where(EventRow.run_id == run_id, EventRow.event_type == "hook_fired"))
            )

    def _successor(self, run_id: UUID) -> Run:
        with Session(engine_module.get_engine()) as session:
            return session.exec(select(Run).where(Run.retry_of == run_id)).one()

    def test_a_failed_run_that_will_be_retried_does_not_fire(
        self, store: Store, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _capture_posts(monkeypatch)
        job = self._job(max_attempts=2, delay=0)
        self._watching_hook(store, job)
        run = store.runs.create(_ORG, component_id=job)
        store.runs.complete(run.id, success=False)

        _sweep(store)

        assert self._fired(run.id) == []

    def test_a_retried_run_is_stamped_without_firing(self, store: Store, monkeypatch: pytest.MonkeyPatch) -> None:
        # Not a verdict, so nothing fires; stamped anyway, so it leaves the
        # unevaluated set instead of being re-read every tick.
        _capture_posts(monkeypatch)
        job = self._job(max_attempts=2, delay=0)
        self._watching_hook(store, job)
        run = store.runs.create(_ORG, component_id=job)
        store.runs.complete(run.id, success=False)

        _sweep(store)

        assert self._fired(run.id) == []
        assert _evaluated_at(run.id) is not None

    def test_an_exhausted_stack_fires_once(self, store: Store, monkeypatch: pytest.MonkeyPatch) -> None:
        _capture_posts(monkeypatch)
        job = self._job(max_attempts=1)
        self._watching_hook(store, job)
        run = store.runs.create(_ORG, component_id=job)
        store.runs.complete(run.id, success=False)

        _sweep(store)

        assert len(self._fired(run.id)) == 1

    def test_a_healed_stack_fires_completed_on_the_successful_attempt(
        self, store: Store, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _capture_posts(monkeypatch)
        job = self._job(max_attempts=2, delay=0)
        self._watching_hook(store, job)
        first = store.runs.create(_ORG, component_id=job)
        store.runs.complete(first.id, success=False)
        successor = self._successor(first.id)
        store.runs.complete(successor.id, success=True)

        _sweep(store)

        assert self._fired(first.id) == []
        assert len(self._fired(successor.id)) == 1

    def test_the_context_carries_the_stacks_position(self, store: Store, monkeypatch: pytest.MonkeyPatch) -> None:
        payloads = _capture_posts(monkeypatch)
        job = self._job(max_attempts=1)
        self._watching_hook(store, job)
        run = store.runs.create(_ORG, component_id=job)
        store.runs.complete(run.id, success=False)

        _sweep(store)

        assert payloads[0]["metadata"]["attempt"] == 1
        assert payloads[0]["metadata"]["attempts"] == 1


class TestDeliveryCursor:
    """A terminal row is swept until it is stamped, whatever the clocks say."""

    def _watched_source(self, store: Store) -> UUID:
        source = store.components.create(_ORG, kind="source", key="demo_source", name="Demo")
        store.components.create(
            _ORG,
            kind="hook",
            key="webhook_hook",
            name="Notify",
            config={"events": ["run_completed", "run_failed"], "url": "https://example.invalid/hook"},
            relations={"watches": [source.id]},
        )
        return source.id

    def test_an_evaluated_run_is_stamped(self, store: Store, monkeypatch: pytest.MonkeyPatch):
        _capture_posts(monkeypatch)
        run = _terminal_run(store, self._watched_source(store))

        _sweep(store)

        assert _evaluated_at(run.id) is not None

    def test_a_run_that_finished_long_before_the_controller_started_is_evaluated(
        self, store: Store, monkeypatch: pytest.MonkeyPatch
    ):
        # No watermark: a verdict reached during a restart, or stamped by a
        # trailing clock, is delivered late rather than never.
        payloads = _capture_posts(monkeypatch)
        run = _terminal_run(store, self._watched_source(store))
        with Session(engine_module.get_engine()) as session:
            db_run = session.get(Run, run.id)
            assert db_run is not None
            db_run.completed_at = _PAST
            session.add(db_run)
            session.commit()

        _sweep(store)

        assert [payload["run_id"] for payload in payloads] == [str(run.id)]

    def test_a_stamped_run_is_not_evaluated_again(self, store: Store, monkeypatch: pytest.MonkeyPatch):
        payloads = _capture_posts(monkeypatch)
        run = _terminal_run(store, self._watched_source(store))
        _sweep(store)
        stamped = _evaluated_at(run.id)

        _sweep(store)

        assert len(payloads) == 1
        assert _evaluated_at(run.id) == stamped

    def test_a_run_another_scheduler_holds_is_left_to_it(self, store: Store, monkeypatch: pytest.MonkeyPatch):
        payloads = _capture_posts(monkeypatch)
        run = _terminal_run(store, self._watched_source(store))
        monkeypatch.setattr(store.runs, "claim_hooks", lambda run_id: False)

        _sweep(store)

        assert payloads == []
        assert _evaluated_at(run.id) is None


class TestBackfillEvents:
    """A backfill's verdict is a subject of its own, beside its runs'."""

    def _job(self, **retry: Any) -> UUID:
        with Session(engine_module.get_engine()) as session:
            row = Component(
                id=uuid4(),
                org_id=_ORG,
                kind="job",
                key="cron_job",
                name="Nightly",
                config={"cron": "0 6 * * *", **({"retry": retry} if retry else {})},
            )
            session.add(row)
            session.commit()
            assert row.id is not None
            return row.id

    def _hook(self, store: Store, component_id: UUID, *events: str) -> UUID:
        return store.components.create(
            _ORG,
            kind="hook",
            key="webhook_hook",
            name="Notify",
            config={"events": list(events), "url": "https://example.invalid/hook"},
            relations={"watches": [component_id]},
        ).id

    def _backfill(self, store: Store, job: UUID, days: int = 3) -> Backfill:
        return store.backfills.create(
            _ORG, component_id=job, start_key="2026-09-01", end_key=f"2026-09-{days:02d}", concurrency=days
        )

    def _runs(self, backfill_id: UUID) -> dict[str, Run]:
        with Session(engine_module.get_engine()) as session:
            runs = session.exec(select(Run).where(Run.backfill_id == backfill_id)).all()
            return {run.partition_key or "": run for run in runs}

    def _backfill_evaluated_at(self, backfill_id: UUID) -> dt.datetime | None:
        with Session(engine_module.get_engine()) as session:
            db_backfill = session.get(Backfill, backfill_id)
            assert db_backfill is not None
            return db_backfill.hooks_evaluated_at

    def test_a_failed_backfill_fires_once_with_its_partitions(self, store: Store, monkeypatch: pytest.MonkeyPatch):
        payloads = _capture_posts(monkeypatch)
        job = self._job()
        self._hook(store, job, "backfill_failed")
        backfill = self._backfill(store, job)
        runs = self._runs(backfill.id)
        store.runs.complete(runs["2026-09-01"].id, success=True)
        store.runs.complete(runs["2026-09-02"].id, success=False)
        _record_run_failure(runs["2026-09-02"].id, "rate limited")
        store.runs.complete(runs["2026-09-03"].id, success=False)

        _sweep(store)
        _sweep(store)

        assert [payload["event_type"] for payload in payloads] == ["backfill_failed"]
        payload = payloads[0]
        assert payload["backfill_id"] == str(backfill.id)
        assert payload["run_id"] is None
        assert (payload["start_key"], payload["end_key"]) == ("2026-09-01", "2026-09-03")
        assert payload["metadata"]["component_name"] == "Nightly"
        assert payload["metadata"]["organisation_name"] is None
        assert payload["metadata"]["partitions"] == 3
        assert payload["metadata"]["counts"] == {"success": 1, "failed": 2}
        assert payload["metadata"]["failed_partitions"] == [["2026-09-03", None], ["2026-09-02", "rate limited"]]
        assert self._backfill_evaluated_at(backfill.id) is not None

    def test_a_completed_backfill_fires_completed(self, store: Store, monkeypatch: pytest.MonkeyPatch):
        payloads = _capture_posts(monkeypatch)
        job = self._job()
        self._hook(store, job, "backfill_completed")
        backfill = self._backfill(store, job, days=2)
        for run in self._runs(backfill.id).values():
            store.runs.complete(run.id, success=True)

        _sweep(store)

        assert [payload["event_type"] for payload in payloads] == ["backfill_completed"]
        assert payloads[0]["metadata"]["counts"] == {"success": 2}
        assert "failed_partitions" not in payloads[0]["metadata"]

    def test_the_runs_of_a_backfill_still_fire_run_events(self, store: Store, monkeypatch: pytest.MonkeyPatch):
        payloads = _capture_posts(monkeypatch)
        job = self._job()
        self._hook(store, job, "run_failed")
        backfill = self._backfill(store, job, days=2)
        for run in self._runs(backfill.id).values():
            store.runs.complete(run.id, success=False)

        _sweep(store)

        assert sorted(payload["event_type"] for payload in payloads) == ["run_failed", "run_failed"]

    def test_a_backfill_waiting_on_a_retry_fires_only_once_it_closes(
        self, store: Store, monkeypatch: pytest.MonkeyPatch
    ):
        payloads = _capture_posts(monkeypatch)
        job = self._job(max_attempts=2, delay=0)
        self._hook(store, job, "backfill_completed", "backfill_failed")
        backfill = self._backfill(store, job, days=1)
        first = self._runs(backfill.id)["2026-09-01"]
        store.runs.complete(first.id, success=False)

        _sweep(store)
        assert payloads == []

        with Session(engine_module.get_engine()) as session:
            successor = session.exec(select(Run).where(Run.retry_of == first.id)).one()
        store.runs.complete(successor.id, success=True)
        _sweep(store)

        assert [payload["event_type"] for payload in payloads] == ["backfill_completed"]
        assert payloads[0]["metadata"]["counts"] == {"success": 1}

    def test_a_trigger_cascades_the_range_with_the_targets_concurrency(self, store: Store):
        job = self._job()
        downstream = store.components.create(
            _ORG, kind="job", key="cron_job", name="Downstream", config={"cron": "0 7 * * *", "concurrency": 2}
        )
        store.components.create(
            _ORG,
            kind="hook",
            key="trigger_hook",
            name="Cascade",
            config={"events": ["backfill_completed"]},
            relations={"watches": [job], "targets": [downstream.id]},
        )
        backfill = self._backfill(store, job, days=3)
        for run in self._runs(backfill.id).values():
            store.runs.complete(run.id, success=True)

        _sweep(store)

        with Session(engine_module.get_engine()) as session:
            cascaded = session.exec(select(Backfill).where(Backfill.component_id == downstream.id)).one()
        assert (cascaded.start_key, cascaded.end_key, cascaded.concurrency) == ("2026-09-01", "2026-09-03", 2)
        statuses = sorted(run.status for run in self._runs(cascaded.id).values())
        assert statuses == ["pending", "queued", "queued"]

    def test_a_canceled_backfill_is_stamped_without_firing(self, store: Store, monkeypatch: pytest.MonkeyPatch):
        payloads = _capture_posts(monkeypatch)
        job = self._job()
        self._hook(store, job, "backfill_completed", "backfill_failed")
        backfill = self._backfill(store, job, days=2)
        store.backfills.cancel(backfill.id)

        _sweep(store)

        assert payloads == []
        assert self._backfill_evaluated_at(backfill.id) is not None
        assert all(_evaluated_at(run.id) is not None for run in self._runs(backfill.id).values())


class TestSubjectUrl:
    """The context links to the subject's page when the app has a public URL."""

    @pytest.fixture
    def public_url(self) -> Iterator[None]:
        AppSettings.activate(AppSettings(server=ServerSettings(external_url="https://app.test/")))
        yield
        AppSettings.clear_active()

    def _watch(self, store: Store, component_id: UUID, event: str) -> None:
        store.components.create(
            _ORG,
            kind="hook",
            key="webhook_hook",
            name="Notify",
            config={"events": [event], "url": "https://example.invalid/hook"},
            relations={"watches": [component_id]},
        )

    def test_a_run_event_links_to_the_run(self, store: Store, monkeypatch: pytest.MonkeyPatch, public_url: None):
        payloads = _capture_posts(monkeypatch)
        source = store.components.create(_ORG, kind="source", key="demo_source", name="Demo")
        leaf = next(c for c in source.children if c.key == "e")
        self._watch(store, leaf.id, "run_completed")
        run = _terminal_run(store, leaf.id)

        _sweep(store)

        assert [payload["url"] for payload in payloads] == [f"https://app.test/executions/runs/{run.id}"]

    def test_a_backfill_event_links_to_the_backfill(
        self, store: Store, monkeypatch: pytest.MonkeyPatch, public_url: None
    ):
        payloads = _capture_posts(monkeypatch)
        job = store.components.create(_ORG, kind="job", key="cron_job", name="Nightly", config={"cron": "0 6 * * *"})
        self._watch(store, job.id, "backfill_completed")
        backfill = store.backfills.create(
            _ORG, component_id=job.id, start_key="2026-09-01", end_key="2026-09-02", concurrency=2
        )
        with Session(engine_module.get_engine()) as session:
            for run in session.exec(select(Run).where(Run.backfill_id == backfill.id)).all():
                store.runs.complete(run.id, success=True)

        _sweep(store)

        assert [payload["url"] for payload in payloads] == [f"https://app.test/executions/backfills/{backfill.id}"]

    def test_without_a_public_url_the_context_carries_none(self, store: Store, monkeypatch: pytest.MonkeyPatch):
        payloads = _capture_posts(monkeypatch)
        source = store.components.create(_ORG, kind="source", key="demo_source", name="Demo")
        leaf = next(c for c in source.children if c.key == "e")
        self._watch(store, leaf.id, "run_completed")
        _terminal_run(store, leaf.id)

        _sweep(store)

        assert [payload["url"] for payload in payloads] == [None]


class TestEvaluateGuards:
    """``_evaluate`` skips a subject it cannot react to."""

    def test_a_run_without_a_target_is_skipped(self, store: Store):
        run = store.runs.create(_ORG)
        store.runs.complete(run.id, success=True)

        HookController(store=store, poll_interval=999)._evaluate(store.runs.get(run.id))

    def test_a_non_terminal_run_is_skipped(self, store: Store):
        component_id = store.components.create(_ORG, kind="source", key="demo_source", name="Demo").id
        run = store.runs.create(_ORG, component_id=component_id)

        HookController(store=store, poll_interval=999)._evaluate(store.runs.get(run.id))

    def test_a_backfill_without_a_target_is_skipped(self, store: Store):
        with Session(engine_module.get_engine()) as session:
            orphan = Backfill(org_id=_ORG, status="success", start_key="2026-09-01", end_key="2026-09-01")
            session.add(orphan)
            session.commit()
            orphan_id = orphan.id

        HookController(store=store, poll_interval=999)._evaluate_backfill(store.backfills.get(orphan_id))

    def test_a_run_with_no_matching_hook_is_skipped(self, store: Store):
        component_id = store.components.create(_ORG, kind="source", key="demo_source", name="Demo").id
        run = _terminal_run(store, component_id)

        HookController(store=store, poll_interval=999)._evaluate(store.runs.get(run.id))
