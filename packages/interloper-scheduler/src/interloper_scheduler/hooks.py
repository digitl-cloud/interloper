"""Hook evaluator: fires hooks in reaction to terminal runs and backfills.

A single background loop (a singleton, running alongside the cron
controller) sweeps terminal runs and backfills not yet evaluated, matches
them against hooks watching the subject's target component (or its parent
source), and calls each matching hook's ``fire()``.

A hook observes a **verdict**, never an attempt: a failed run whose next
attempt is already queued is not an outcome, so the sweep stamps it without
firing. Because the successor is created in the same transaction that marks
the run failed, there is no window in which a doomed attempt looks final,
and the rule needs no knowledge of budgets or backoff. A backfill is its own
subject: it finalises from each partition's latest attempt and stays open
while a retry waits, so its status is a verdict by construction. A canceled
run or backfill is not an outcome of the work and fires nothing.

The cursor is the row itself: ``hooks_evaluated_at`` is stamped once a
subject's hooks have been evaluated, so no clock is compared with another
and a verdict reached while the scheduler was down is delivered late rather
than never. Delivery is **at-least-evaluated, at-most-fired-once**: every
firing is claimed by an ``events`` row whose id is deterministic (uuid5 of
hook + subject), so a crash between a claim and its stamp re-evaluates the
subject and finds the claim. Failures are recorded on the same claim
(``hook_failed``) and are not retried.
"""

from __future__ import annotations

import datetime as dt
import logging
import uuid
from typing import Any
from uuid import UUID

import interloper as il
from interloper.settings import AppSettings
from interloper_db import Store
from interloper_db.models import Backfill, Component, ComponentRelation, Organisation, Run
from interloper_db.models import Event as EventRow
from sqlalchemy import func
from sqlalchemy.orm import aliased
from sqlmodel import Session, col, select

from interloper_scheduler.controller import Controller

logger = logging.getLogger(__name__)

#: Namespace for deterministic firing-claim event ids.
_CLAIM_NAMESPACE = uuid.UUID("f6c1a9de-7b6e-4dbb-9f43-1a2b3c4d5e6f")

#: Terminal run status → the hook event type it produces.
_TERMINAL_EVENT_TYPES = {"success": "run_completed", "failed": "run_failed"}
_TERMINAL_STATUSES = frozenset(_TERMINAL_EVENT_TYPES)

#: Terminal backfill status → the hook event type it produces.
_BACKFILL_EVENT_TYPES = {"success": "backfill_completed", "failed": "backfill_failed"}

#: Every status the sweep stamps, verdict or not.
_SWEPT_STATUSES = frozenset({"success", "failed", "canceled"})


def _claim_id(hook_id: UUID, subject_id: UUID) -> str:
    """Deterministic event id for one hook firing on one subject.

    Args:
        hook_id: The hook component's id.
        subject_id: The run or backfill that fired it.

    Returns:
        The uuid5-derived id string.
    """
    return str(uuid.uuid5(_CLAIM_NAMESPACE, f"hook:{hook_id}:{subject_id}"))


class HookController(Controller):
    """Evaluates hooks against terminal runs and backfills.

    Each tick, for runs and then for backfills:
    1. sweep terminal rows not yet stamped ``hooks_evaluated_at``, oldest
       first; a canceled row, or a failed run whose successor is queued, is
       stamped straight away
    2. match each subject against enabled hooks watching its target component
       or the target's parent
    3. fire unclaimed matches, recording each firing as an ``events`` row
       and stamping the hook's machine-owned state, then stamp the subject
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

    # -- Internals -------------------------------------------------------------

    def _tick(self) -> None:
        """Evaluate every terminal run and backfill not yet stamped, oldest first."""
        with Session(self._store.engine) as session:
            successor = aliased(Run)
            has_successor = select(successor.id).where(col(successor.retry_of) == Run.id).exists()
            retried = (col(Run.status) == "failed") & has_successor
            pending_runs = (
                select(Run)
                .where(col(Run.status).in_(_SWEPT_STATUSES))
                .where(col(Run.hooks_evaluated_at).is_(None))
                .order_by(col(Run.completed_at))
            )
            for run in session.exec(pending_runs.where((col(Run.status) == "canceled") | retried)).all():
                self._stamp(session, run)
            for run in session.exec(pending_runs.where(~((col(Run.status) == "canceled") | retried))).all():
                self._evaluate(session, run)
                self._stamp(session, run)

            pending_backfills = (
                select(Backfill)
                .where(col(Backfill.status).in_(_SWEPT_STATUSES))
                .where(col(Backfill.hooks_evaluated_at).is_(None))
                .order_by(col(Backfill.completed_at))
            )
            for backfill in session.exec(pending_backfills).all():
                if backfill.status in _BACKFILL_EVENT_TYPES:
                    self._evaluate_backfill(session, backfill)
                self._stamp(session, backfill)

    @staticmethod
    def _stamp(session: Session, subject: Run | Backfill) -> None:
        """Mark a run or backfill as evaluated, so no sweep reads it again.

        Args:
            session: Open session the stamp is written through.
            subject: The row whose hooks have been evaluated, or which is not a verdict.
        """
        subject.hooks_evaluated_at = dt.datetime.now(dt.timezone.utc)
        session.add(subject)
        session.commit()

    def _evaluate(self, session: Session, run: Run) -> None:
        """Fire every unclaimed, matching hook for one terminal run.

        Args:
            session: Open session the evaluation is made through.
            run: The terminal run being reacted to.
        """
        if run.component_id is None or run.status not in _TERMINAL_STATUSES:
            return
        event_type = _TERMINAL_EVENT_TYPES[run.status]

        target = session.get(Component, run.component_id)
        if target is None:
            return

        metadata = self._event_metadata(session, run, target, event_type)
        for hook_row, hook, claim in self._unclaimed_hooks(session, run.org_id, target, run.id, event_type):
            watched_ids = {str(watch.id) for watch in hook.watches}
            context = il.HookContext(
                event_type=event_type,
                component_id=str(run.component_id),
                run_id=str(run.id),
                partition_key=run.partition_key,
                url=self._subject_url(f"/executions/runs/{run.id}"),
                metadata=metadata,
                trigger=lambda component_id, watched_ids=watched_ids: self._trigger(
                    session, run, component_id, watched_ids
                ),
            )
            self._fire(hook_row, hook, context, claim, org_id=run.org_id, run_id=run.id, subject=f"run {run.id}")
            session.add(hook_row)
            session.commit()

    def _evaluate_backfill(self, session: Session, backfill: Backfill) -> None:
        """Fire every unclaimed, matching hook for one terminal backfill.

        Args:
            session: Open session the evaluation is made through.
            backfill: The terminal backfill being reacted to.
        """
        if backfill.component_id is None or backfill.status not in _BACKFILL_EVENT_TYPES:
            return
        event_type = _BACKFILL_EVENT_TYPES[backfill.status]

        target = session.get(Component, backfill.component_id)
        if target is None:
            return

        metadata = self._backfill_metadata(session, backfill, target, event_type)
        for hook_row, hook, claim in self._unclaimed_hooks(session, backfill.org_id, target, backfill.id, event_type):
            watched_ids = {str(watch.id) for watch in hook.watches}
            context = il.HookContext(
                event_type=event_type,
                component_id=str(backfill.component_id),
                backfill_id=str(backfill.id),
                start_key=backfill.start_key,
                end_key=backfill.end_key,
                url=self._subject_url(f"/executions/backfills/{backfill.id}"),
                metadata=metadata,
                trigger=lambda component_id, watched_ids=watched_ids: self._trigger_backfill(
                    session, backfill, component_id, watched_ids
                ),
            )
            self._fire(
                hook_row,
                hook,
                context,
                claim,
                org_id=backfill.org_id,
                run_id=None,
                subject=f"backfill {backfill.id}",
                data={"backfill_id": str(backfill.id)},
            )
            session.add(hook_row)
            session.commit()

    @staticmethod
    def _subject_url(path: str) -> str | None:
        """The app page for a run or backfill, when the deployment has a public URL.

        Args:
            path: The page's path under the app, such as ``/executions/runs/<id>``.

        Returns:
            The absolute URL, or ``None`` when ``server.external_url`` is unset.
        """
        base = AppSettings.get().server.external_url.rstrip("/")
        return f"{base}{path}" if base else None

    def _unclaimed_hooks(
        self, session: Session, org_id: UUID, target: Component, subject_id: UUID, event_type: str
    ) -> list[tuple[Component, il.Hook, str]]:
        """The enabled hooks watching *target* that subscribe to *event_type* and have not fired on *subject_id*.

        Args:
            session: Open session the hooks and claims are read through.
            org_id: The subject's organisation, scoping the hook search.
            target: The subject's component.
            subject_id: The run or backfill the claim is keyed on.
            event_type: The hook event type the subject's status produced.

        Returns:
            ``(row, hydrated hook, claim id)`` per hook still to fire.
        """
        matches: list[tuple[Component, il.Hook, str]] = []
        for hook_row in self._matching_hooks(session, org_id, target):
            claim = _claim_id(hook_row.id, subject_id)
            if session.get(EventRow, UUID(claim)) is not None:
                continue
            hook = self._store.components.load(hook_row.id)
            if not isinstance(hook, il.Hook) or not hook.enabled or event_type not in hook.events:
                continue
            matches.append((hook_row, hook, claim))
        return matches

    def _event_metadata(self, session: Session, run: Run, target: Component, event_type: str) -> dict[str, Any]:
        """Describe a run event for the hooks about to see it.

        Args:
            session: Open session the target's parent is resolved through.
            run: The terminal run the event describes.
            target: The run's component.
            event_type: The hook event type the run's status produced.

        The ids in the context are the machine-readable half; this is the half
        a hook addressing humans (a Slack message) renders, so it carries the
        organisation's and the component's display names, the stack's position (this attempt's number
        and how many the stack holds, so a message can say it succeeded on the
        second or failed after three) and, for a failure, the error the run
        recorded, which lives on the run's event rows rather than the run.

        Returns:
            The context metadata.
        """
        metadata: dict[str, Any] = {
            "status": run.status,
            "organisation_name": self._organisation_name(session, run.org_id),
            "component_name": target.name or target.key,
            "component_key": target.key,
            "attempt": run.attempt,
            "attempts": session.exec(
                select(func.count()).select_from(Run).where(Run.root_run_id == run.root_run_id)
            ).one(),
        }
        if event_type == "run_failed":
            error = session.exec(
                select(EventRow.error)
                .where(EventRow.run_id == run.id)
                .where(EventRow.event_type == "run_failed")
                .where(col(EventRow.error).is_not(None))
                .order_by(col(EventRow.timestamp).desc())
            ).first()
            if error:
                metadata["error"] = error
        return metadata

    def _backfill_metadata(
        self, session: Session, backfill: Backfill, target: Component, event_type: str
    ) -> dict[str, Any]:
        """Describe a backfill event for the hooks about to see it.

        Args:
            session: Open session the organisation is read through.
            backfill: The terminal backfill the event describes.
            target: The backfill's component.
            event_type: The hook event type the backfill's status produced.

        Beside the target's identity it carries the partitions per status,
        each read as its stack's latest attempt, and, for a failure, the
        failed partitions with their recorded errors, so a message can name
        what failed without a second lookup.

        Returns:
            The context metadata.
        """
        metadata: dict[str, Any] = {
            "status": backfill.status,
            "organisation_name": self._organisation_name(session, backfill.org_id),
            "component_name": target.name or target.key,
            "component_key": target.key,
            "partitions": backfill.partitions,
            "counts": self._store.runs.count_backfill_runs([backfill.id]).get(backfill.id, {}),
        }
        if event_type == "backfill_failed":
            metadata["failed_partitions"] = [
                [partition_key, error] for partition_key, error in self._store.runs.failed_partitions(backfill.id)
            ]
        return metadata

    @staticmethod
    def _organisation_name(session: Session, org_id: UUID) -> str | None:
        """The display name of the organisation a subject belongs to.

        Args:
            session: Open session the organisation is read through.
            org_id: The subject's organisation.

        Returns:
            The name, or ``None`` when the organisation row is gone.
        """
        organisation = session.get(Organisation, org_id)
        return organisation.name if organisation else None

    def _matching_hooks(self, session: Session, org_id: UUID, target: Component) -> list[Component]:
        """Hooks watching *target* (the subject's component) or its parent.

        Args:
            session: Open session the hooks are read through.
            org_id: The subject's organisation, scoping the search.
            target: The subject's component.

        Returns:
            The matching hook rows.
        """
        watched_ids = [target.id] + ([target.parent_id] if target.parent_id else [])

        return list(
            session.exec(
                select(Component)
                .join(ComponentRelation, onclause=ComponentRelation.src_id == Component.id)  # ty: ignore[invalid-argument-type]
                .where(Component.kind == "hook")
                .where(Component.org_id == org_id)
                .where(ComponentRelation.name == "watches")
                .where(col(ComponentRelation.dst_id).in_(watched_ids))
                .distinct()
            ).all()
        )

    def _fire(
        self,
        hook_row: Component,
        hook: il.Hook,
        context: il.HookContext,
        claim: str,
        *,
        org_id: UUID,
        run_id: UUID | None,
        subject: str,
        data: dict[str, Any] | None = None,
    ) -> None:
        """Fire one hook and record the outcome on its claim.

        The caller adds and commits ``hook_row`` afterwards: the state stamp
        joins its session.

        Args:
            hook_row: The hook's component row, carrying its state.
            hook: The hydrated hook to fire.
            context: What the hook sees.
            claim: Deterministic claim id, making the firing idempotent.
            org_id: The subject's organisation, owning the claim row.
            run_id: The run the claim hangs off, or ``None`` for a backfill.
            subject: How the subject reads in the log.
            data: Extra fields recorded on the claim, such as the backfill id.
        """
        error: str | None = None
        try:
            hook.fire(context)
            logger.info("Hook '%s' fired for %s (%s)", hook_row.name, subject, context.event_type)
        except Exception as e:
            error = str(e)
            logger.exception("Hook '%s' failed for %s", hook_row.name, subject)

        outcome = il.EventType.HOOK_FAILED if error else il.EventType.HOOK_FIRED
        self._store.events.save(
            il.Event(
                id=claim,
                type=outcome,
                metadata={
                    "component_id": str(hook_row.id),
                    "component_kind": "hook",
                    "component_key": hook_row.key,
                    "message": f"Hook '{hook_row.name}' ({hook_row.key}) reacted to {context.event_type}",
                    "error": error,
                    **(data or {}),
                },
            ),
            org_id=org_id,
            run_id=run_id,
        )

        hook_row.stamp_state(
            last_fired_at=dt.datetime.now(dt.timezone.utc),
            **({"last_run_id": str(run_id)} if run_id else {}),
        )

    def _trigger(self, session: Session, run: Run, component_id: str, watched_ids: set[str]) -> None:
        """The trigger capability handed to hooks on a run event: queue a run for a component.

        The originating run's partition is propagated, so cascading pipelines
        stay on the same partition.

        Args:
            session: Open session the target's parent is resolved through.
            run: The terminal run that triggered the hook.
            component_id: The component the hook asks to run.
            watched_ids: Ids the hook watches, which bound what it may trigger.
        """
        self._refuse_reentry(session, component_id, watched_ids)
        self._store.runs.create(run.org_id, component_id=UUID(component_id), partition_key=run.partition_key)

    def _trigger_backfill(self, session: Session, backfill: Backfill, component_id: str, watched_ids: set[str]) -> None:
        """The trigger capability handed to hooks on a backfill event: backfill a component over the same range.

        A job target runs the range the way its own firing would, gated by its
        ``concurrency``; any other target runs it one partition at a time.

        Args:
            session: Open session the target is resolved through.
            backfill: The terminal backfill that triggered the hook.
            component_id: The component the hook asks to backfill.
            watched_ids: Ids the hook watches, which bound what it may trigger.
        """
        self._refuse_reentry(session, component_id, watched_ids)
        target = session.get(Component, UUID(component_id))
        concurrency = (target.config or {}).get("concurrency", 1) if target and target.kind == "job" else 1
        self._store.runs.create_backfill(
            backfill.org_id,
            component_id=UUID(component_id),
            start_key=backfill.start_key,
            end_key=backfill.end_key,
            concurrency=concurrency,
        )

    @staticmethod
    def _refuse_reentry(session: Session, component_id: str, watched_ids: set[str]) -> None:
        """Refuse a trigger that would re-enter the firing hook's own watch set.

        Triggering a component the hook watches (directly or through the
        component's parent) would fire the hook again with a fresh claim, an
        infinite loop. Cycles across *multiple* hooks remain the operator's
        responsibility, like any recursive schedule.

        Args:
            session: Open session the target's parent is resolved through.
            component_id: The component the hook asks to run.
            watched_ids: Ids the hook watches.

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
