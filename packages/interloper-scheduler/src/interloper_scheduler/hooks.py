"""Hook evaluator: fires hooks in reaction to terminal runs.

A single background loop (a singleton, running alongside the cron
controller) sweeps recently-terminal runs with a watermark and an overlap
window, matches them against hooks watching the run's target component (or
its parent source), and calls each matching hook's ``fire()``.

Delivery is **at-least-evaluated, at-most-fired-once**: every firing is
claimed by an ``events`` row whose id is deterministic (uuid5 of hook + run),
so the overlap window and restarts re-evaluate runs without re-firing hooks.
Failures are recorded on the same claim (``hook_failed``) and are not
retried. The watermark starts at boot, so runs that ended while the
scheduler was down are not replayed.
"""

from __future__ import annotations

import datetime as dt
import logging
import uuid
from uuid import UUID

import interloper as il
from interloper_db import Store, stamp_component_state
from interloper_db.models import Component, ComponentRelation, Run
from interloper_db.models import Event as EventRow
from sqlmodel import Session, col, select

from interloper_scheduler.controller import Controller

logger = logging.getLogger(__name__)

#: Namespace for deterministic firing-claim event ids.
_CLAIM_NAMESPACE = uuid.UUID("f6c1a9de-7b6e-4dbb-9f43-1a2b3c4d5e6f")

#: How far behind the watermark each sweep re-reads, so a run committing
#: just before a cycle's cutoff is still seen by the next cycle.
_OVERLAP = dt.timedelta(seconds=30)

#: Terminal run status → the hook event type it produces.
_TERMINAL_EVENT_TYPES = {"success": "run_completed", "failed": "run_failed"}
_TERMINAL_STATUSES = frozenset(_TERMINAL_EVENT_TYPES)


def _claim_id(hook_id: UUID, run_id: UUID) -> str:
    """Deterministic event id for one hook firing on one run.

    Returns:
        The uuid5-derived id string.
    """
    return str(uuid.uuid5(_CLAIM_NAMESPACE, f"hook:{hook_id}:{run_id}"))


class HookController(Controller):
    """Evaluates hooks against terminal runs.

    Each tick:
    1. sweep runs that reached a terminal status since the last watermark
       (minus an overlap window)
    2. match each run against enabled hooks watching its target component
       or the target's parent
    3. fire unclaimed matches, recording each firing as an ``events`` row
       and stamping the hook's machine-owned state
    """

    def __init__(self, store: Store | None = None, poll_interval: int = 5) -> None:
        """Initialize the hook controller.

        Args:
            store: The Store for hydration, run creation, and events.
                Defaults to the settings-configured one.
            poll_interval: Seconds between sweep cycles.
        """
        super().__init__(poll_interval=poll_interval)
        self._store = store or Store.from_settings()
        # The watermark starts at the first tick, so runs that ended while
        # the scheduler was down are not replayed.
        self._watermark: dt.datetime | None = None

    # -- Internals -------------------------------------------------------------

    def _tick(self) -> None:
        """Sweep one watermark window of terminal runs."""
        now = dt.datetime.now(dt.timezone.utc)
        if self._watermark is None:
            self._watermark = now
        since = self._watermark - _OVERLAP

        with Session(self._store.engine) as session:
            runs = session.exec(
                select(Run)
                .where(col(Run.status).in_(_TERMINAL_STATUSES))
                .where(col(Run.completed_at) > since)
                .order_by(col(Run.completed_at))
            ).all()

            for run in runs:
                self._evaluate(session, run)

        self._watermark = now

    def _evaluate(self, session: Session, run: Run) -> None:
        """Fire every unclaimed, matching hook for one terminal run."""
        if run.component_id is None or run.status not in _TERMINAL_STATUSES:
            return
        event_type = _TERMINAL_EVENT_TYPES[run.status]

        for hook_row in self._matching_hooks(session, run):
            claim = _claim_id(hook_row.id, run.id)
            if session.get(EventRow, UUID(claim)) is not None:
                continue

            hook = self._store.load(hook_row.id)
            if not isinstance(hook, il.Hook) or not hook.enabled or event_type not in hook.events:
                continue

            self._fire(session, hook_row, hook, run, event_type, claim)

    def _matching_hooks(self, session: Session, run: Run) -> list[Component]:
        """Hooks watching the run's target component or its parent.

        Returns:
            The matching hook rows.
        """
        target = session.get(Component, run.component_id)
        if target is None:
            return []
        watched_ids = [target.id] + ([target.parent_id] if target.parent_id else [])

        return list(
            session.exec(
                select(Component)
                .join(ComponentRelation, onclause=ComponentRelation.src_id == Component.id)  # ty: ignore[invalid-argument-type]
                .where(Component.kind == "hook")
                .where(Component.org_id == run.org_id)
                .where(ComponentRelation.type == "watch")
                .where(col(ComponentRelation.dst_id).in_(watched_ids))
                .distinct()
            ).all()
        )

    def _fire(
        self,
        session: Session,
        hook_row: Component,
        hook: il.Hook,
        run: Run,
        event_type: str,
        claim: str,
    ) -> None:
        """Fire one hook and record the outcome on its claim."""
        watched_ids = {str(w.id) for w in hook.watches}
        context = il.HookContext(
            event_type=event_type,
            component_id=str(run.component_id),
            run_id=str(run.id),
            partition_date=run.partition_date.isoformat() if run.partition_date else None,
            metadata={"status": run.status},
            trigger=lambda component_id: self._trigger(session, run, component_id, watched_ids),
        )

        error: str | None = None
        try:
            hook.fire(context)
            logger.info("Hook '%s' fired for run %s (%s)", hook_row.name, run.id, event_type)
        except Exception as e:
            error = str(e)
            logger.exception("Hook '%s' failed for run %s: %s", hook_row.name, run.id, e)

        outcome = il.EventType.HOOK_FAILED if error else il.EventType.HOOK_FIRED
        self._store.save_event(
            il.Event(
                id=claim,
                type=outcome,
                metadata={
                    "message": f"Hook '{hook_row.name}' ({hook_row.key}) reacted to {event_type}",
                    "error": error,
                },
            ),
            org_id=run.org_id,
            run_id=run.id,
        )

        stamp_component_state(
            hook_row,
            last_fired_at=dt.datetime.now(dt.timezone.utc),
            last_run_id=str(run.id),
        )
        session.add(hook_row)
        session.commit()

    def _trigger(self, session: Session, run: Run, component_id: str, watched_ids: set[str]) -> None:
        """The trigger capability handed to hooks: queue a run for a component.

        The originating run's partition date is propagated, so cascading
        pipelines stay on the same partition. Triggering a component the
        firing hook itself watches (directly or through the component's
        parent) is refused — each such run would fire the hook again with a
        fresh claim, an infinite loop. Cycles across *multiple* hooks remain
        the operator's responsibility, like any recursive schedule.

        Raises:
            ConfigError: If the trigger would re-enter the hook's own watch set.
        """
        from interloper.errors import ConfigError

        target = session.get(Component, UUID(component_id))
        target_closure = {component_id} | ({str(target.parent_id)} if target and target.parent_id else set())
        if target_closure & watched_ids:
            raise ConfigError(
                f"Refusing to trigger component {component_id}: the hook watches it "
                "(directly or via its parent), which would loop forever"
            )
        self._store.create_run(run.org_id, component_id=UUID(component_id), partition_date=run.partition_date)
