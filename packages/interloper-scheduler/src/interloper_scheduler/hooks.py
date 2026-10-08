"""Hook evaluator: fires hooks in reaction to terminal runs and backfills.

A background loop on every scheduler replica sweeps terminal runs and
backfills not yet evaluated, matches them against hooks watching the
subject's target component (or its parent source), and calls each matching
hook's ``fire()``. Each subject is evaluated in its own transaction under its
row lock, skipped when another replica holds it, so replicas split the work.

A hook observes a **verdict**, never an attempt: a failed run whose next
attempt is queued is not an outcome, and the store stamps it evaluated in
the transaction that queues the successor, so there is no window in which a
doomed attempt looks final and the sweep needs no knowledge of budgets or
backoff. A backfill is its own
subject: it finalises from each partition's latest attempt and stays open
while a retry waits, so its status is a verdict by construction. A canceled
run or backfill is not an outcome of the work: the store stamps it at
cancelation, and it fires nothing.

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
from interloper.errors import ConfigError, NotFoundError
from interloper.settings import AppSettings
from interloper_db import BackfillStatus, EventQuery, RelationQuery, RunStatus, Store
from interloper_db.models import Backfill, Component, Run

from interloper_scheduler.controller import Controller

logger = logging.getLogger(__name__)

#: Namespace for deterministic firing-claim event ids.
_CLAIM_NAMESPACE = uuid.UUID("f6c1a9de-7b6e-4dbb-9f43-1a2b3c4d5e6f")

#: Terminal run status → the hook event type it produces.
_TERMINAL_EVENT_TYPES = {RunStatus.SUCCESS: "run_completed", RunStatus.FAILED: "run_failed"}

#: Terminal backfill status → the hook event type it produces.
_BACKFILL_EVENT_TYPES = {BackfillStatus.SUCCESS: "backfill_completed", BackfillStatus.FAILED: "backfill_failed"}


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
    1. read the terminal rows whose hooks are pending, oldest completion first
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
        for run in self._store.runs.hooks_pending():
            with self._store.transaction():
                if not self._store.runs.claim_hooks(run.id):
                    continue
                self._evaluate(run)
                self._store.runs.mark_hooks_evaluated(run.id)
        for backfill in self._store.backfills.hooks_pending():
            with self._store.transaction():
                if not self._store.backfills.claim_hooks(backfill.id):
                    continue
                self._evaluate_backfill(backfill)
                self._store.backfills.mark_hooks_evaluated(backfill.id)

    def _evaluate(self, run: Run) -> None:
        """Fire every unclaimed, matching hook for one terminal run.

        Args:
            run: The terminal run being reacted to, its target loaded.
        """
        target = run.target
        if target is None or run.status not in _TERMINAL_EVENT_TYPES:
            return
        event_type = _TERMINAL_EVENT_TYPES[run.status]

        metadata = self._event_metadata(run, target, event_type)
        for hook_row, hook, claim in self._unclaimed_hooks(run.org_id, target, run.id, event_type):
            watched_ids = {str(watch.id) for watch in hook.watches}
            context = il.HookContext(
                event_type=event_type,
                component_id=str(run.component_id),
                run_id=str(run.id),
                partition_key=run.partition_key,
                url=self._subject_url(f"/executions/runs/{run.id}"),
                metadata=metadata,
                trigger=lambda component_id, watched_ids=watched_ids: self._trigger(run, component_id, watched_ids),
            )
            self._fire(hook_row, hook, context, claim, org_id=run.org_id, run_id=run.id, subject=f"run {run.id}")

    def _evaluate_backfill(self, backfill: Backfill) -> None:
        """Fire every unclaimed, matching hook for one terminal backfill.

        Args:
            backfill: The terminal backfill being reacted to, its target loaded.
        """
        target = backfill.target
        if target is None or backfill.status not in _BACKFILL_EVENT_TYPES:
            return
        event_type = _BACKFILL_EVENT_TYPES[backfill.status]

        metadata = self._backfill_metadata(backfill, target, event_type)
        for hook_row, hook, claim in self._unclaimed_hooks(backfill.org_id, target, backfill.id, event_type):
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
                    backfill, component_id, watched_ids
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
        self, org_id: UUID, target: Component, subject_id: UUID, event_type: str
    ) -> list[tuple[Component, il.Hook, str]]:
        """The enabled hooks watching *target* that subscribe to *event_type* and have not fired on *subject_id*.

        Args:
            org_id: The subject's organisation, scoping the hook search.
            target: The subject's component.
            subject_id: The run or backfill the claim is keyed on.
            event_type: The hook event type the subject's status produced.

        Returns:
            ``(row, hydrated hook, claim id)`` per hook still to fire.
        """
        matches: list[tuple[Component, il.Hook, str]] = []
        for hook_row in self._matching_hooks(org_id, target):
            claim = _claim_id(hook_row.id, subject_id)
            if self._claimed(claim, org_id):
                continue
            hook = self._store.components.load(hook_row.id)
            if not isinstance(hook, il.Hook) or not hook.enabled or event_type not in hook.events:
                continue
            matches.append((hook_row, hook, claim))
        return matches

    def _event_metadata(self, run: Run, target: Component, event_type: str) -> dict[str, Any]:
        """Describe a run event for the hooks about to see it.

        Args:
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
            "organisation_name": self._organisation_name(run.org_id),
            "component_name": target.name or target.key,
            "component_key": target.key,
            "attempt": run.attempt,
            "attempts": len(self._store.runs.attempts(run.root_run_id)),
        }
        if event_type == "run_failed":
            failures = EventQuery(event_type=[il.EventType.RUN_FAILED.value], has_error=True, limit=None)
            if errors := self._store.events.list(run.org_id, failures, run_id=run.id).items:
                metadata["error"] = errors[-1].error
        return metadata

    def _backfill_metadata(self, backfill: Backfill, target: Component, event_type: str) -> dict[str, Any]:
        """Describe a backfill event for the hooks about to see it.

        Args:
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
            "organisation_name": self._organisation_name(backfill.org_id),
            "component_name": target.name or target.key,
            "component_key": target.key,
            "partitions": backfill.partitions,
            "counts": self._store.backfills.run_counts([backfill.id]).get(backfill.id, {}),
        }
        if event_type == "backfill_failed":
            metadata["failed_partitions"] = [
                [partition_key, error] for partition_key, error in self._store.backfills.failed_partitions(backfill.id)
            ]
        return metadata

    def _organisation_name(self, org_id: UUID) -> str | None:
        """The display name of the organisation a subject belongs to.

        Args:
            org_id: The subject's organisation.

        Returns:
            The name, or ``None`` when the organisation is gone.
        """
        try:
            return self._store.organisations.get(org_id).name
        except NotFoundError:
            return None

    def _claimed(self, claim: str, org_id: UUID) -> bool:
        """Whether a firing's claim is already recorded.

        Args:
            claim: The deterministic claim id.
            org_id: The subject's organisation, which owns the claim.

        Returns:
            True when the claim event exists.
        """
        try:
            self._store.events.get(UUID(claim), org_id=org_id)
        except NotFoundError:
            return False
        return True

    def _matching_hooks(self, org_id: UUID, target: Component) -> list[Component]:
        """Hooks watching *target* (the subject's component) or its parent.

        Args:
            org_id: The subject's organisation, scoping the search.
            target: The subject's component.

        Returns:
            The matching hook rows.
        """
        watched_ids = [target.id] + ([target.parent_id] if target.parent_id else [])
        query = RelationQuery(name="watches", src_kind="hook", dst_id=watched_ids, limit=None)
        hook_ids = dict.fromkeys(relation.src_id for relation in self._store.relations.list(org_id, query).items)
        return [self._store.components.get(hook_id, org_id=org_id) for hook_id in hook_ids]

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
        """Fire one hook, record the outcome on its claim, and stamp the hook's state.

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
            error = str(e) or type(e).__name__
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

        self._store.components.stamp_state(
            hook_row.id,
            last_fired_at=dt.datetime.now(dt.timezone.utc),
            last_error=error,
            **({"last_run_id": str(run_id)} if run_id else {}),
        )

    def _trigger(self, run: Run, component_id: str, watched_ids: set[str]) -> None:
        """The trigger capability handed to hooks on a run event: queue a run for a component.

        The originating run's partition is propagated, so cascading pipelines
        stay on the same partition.

        Args:
            run: The terminal run that triggered the hook.
            component_id: The component the hook asks to run.
            watched_ids: Ids the hook watches, which bound what it may trigger.
        """
        self._refuse_reentry(run.org_id, component_id, watched_ids)
        self._store.runs.create(run.org_id, component_id=UUID(component_id), partition_key=run.partition_key)

    def _trigger_backfill(self, backfill: Backfill, component_id: str, watched_ids: set[str]) -> None:
        """The trigger capability handed to hooks on a backfill event: backfill a component over the same range.

        A job target runs the range the way its own firing would, gated by its
        ``concurrency``; any other target runs it one partition at a time.

        Args:
            backfill: The terminal backfill that triggered the hook.
            component_id: The component the hook asks to backfill.
            watched_ids: Ids the hook watches, which bound what it may trigger.
        """
        target = self._refuse_reentry(backfill.org_id, component_id, watched_ids)
        concurrency = (target.config or {}).get("concurrency", 1) if target.kind == "job" else 1
        self._store.backfills.create(
            backfill.org_id,
            component_id=UUID(component_id),
            start_key=backfill.start_key,
            end_key=backfill.end_key,
            concurrency=concurrency,
        )

    def _refuse_reentry(self, org_id: UUID, component_id: str, watched_ids: set[str]) -> Component:
        """Refuse a trigger that would re-enter the firing hook's own watch set.

        Triggering a component the hook watches (directly or through the
        component's parent) would fire the hook again with a fresh claim, an
        infinite loop. Cycles across *multiple* hooks remain the operator's
        responsibility, like any recursive schedule.

        Args:
            org_id: The organisation the hook fires in.
            component_id: The component the hook asks to run.
            watched_ids: Ids the hook watches.

        Returns:
            The component to trigger.

        Raises:
            ConfigError: If the trigger would re-enter the hook's own watch set.
        """
        target = self._store.components.get(UUID(component_id), org_id=org_id)
        target_closure = {component_id} | ({str(target.parent_id)} if target.parent_id else set())
        if target_closure & watched_ids:
            raise ConfigError(
                f"Refusing to trigger component {component_id}: the hook watches it "
                "(directly or via its parent), which would loop forever"
            )
        return target
