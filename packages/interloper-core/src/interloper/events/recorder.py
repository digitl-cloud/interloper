"""EventRecorder: a run's events, batched and delivered to the platform with no loss.

The runner hands the recorder every event of its run: it is the ``on_event``
handler `RunSubscription` wraps. A writer thread delivers them as
`EventBatch`es to ``ingest``, whatever reaches the platform from where the run
executes: the store's ingest in the scheduler's process, an `EventClient` from
a pod. A
batch is whatever accumulated while the previous delivery was in flight, so
a quiet run delivers each event as it comes and a burst goes out in a few
large batches.

Delivery has no deadline. Its only bound is the run's liveness rule: a
delivery that keeps failing for half the reaper's heartbeat timeout means the
run can no longer prove it is alive, and it is lost. A response saying the
run is no longer running means it was ended elsewhere: what is already held
is delivered, then the run is lost too. The run's own ending answers that
way as well, and is not a loss.
"""

from __future__ import annotations

import logging
import queue
import threading
import time
from collections.abc import Callable
from types import TracebackType
from typing import TYPE_CHECKING

from typing_extensions import Self

from interloper.errors import DeliveryError, RunLostError
from interloper.events.batch import EventBatch
from interloper.events.event import Event
from interloper.events.types import EventType

if TYPE_CHECKING:
    from interloper.operation.base import Effect
    from interloper.runner.results import RunResult

logger = logging.getLogger(__name__)

_RUN_ENDINGS = frozenset({EventType.RUN_COMPLETED, EventType.RUN_FAILED})
_LIFECYCLE = _RUN_ENDINGS | {
    EventType.RUN_STARTED,
    EventType.OPERATION_QUEUED,
    EventType.OPERATION_STARTED,
    EventType.OPERATION_RETRIED,
    EventType.OPERATION_COMPLETED,
    EventType.OPERATION_FAILED,
    EventType.OPERATION_CANCELED,
}


class EventRecorder:
    """Deliver a run's events to the platform in batches, with no loss while the run lives.

    ```py
    with EventRecorder(client.ingest, run_id=run_id, on_lost=exit_process) as recorder:
        result = await runner.model_copy(update={"on_event": recorder}).run(dag)
        recorder.finish(result)
    ```

    Leaving the block delivers everything still held, with the result's
    effects on the last batch, and raises `RunLostError` when the run was lost.
    """

    def __init__(
        self,
        ingest: Callable[[EventBatch], bool],
        *,
        run_id: str,
        heartbeat_interval: float = 10.0,
        heartbeat_timeout: float = 90.0,
        on_lost: Callable[[], None] | None = None,
        batch_size: int = 500,
        backoff: float = 1.0,
        component_id: str | None = None,
    ) -> None:
        """Bind the recorder to its run and its delivery.

        Args:
            ingest: Delivers one batch, the run's effects on the last;
                returns whether the run is still running. A `DeliveryError`
                says whether the delivery is worth retrying; any other
                exception is retried.
            run_id: The run whose events are recorded, for the log.
            heartbeat_interval: Seconds of silence after which an empty batch
                is delivered, since reporting is the run's proof of life.
            heartbeat_timeout: The reaper's heartbeat timeout; deliveries
                failing for half of it lose the run.
            on_lost: Called once, from the writer thread, when the run is
                lost; a pod exits its process here.
            batch_size: The most events one delivery carries.
            backoff: Seconds before the first retry; doubles up to ten times that.
            component_id: The one operation a child process attempts for its
                host's run, which owns the lifecycle: only what happens inside
                the operation is reported. ``None`` reports everything.
        """
        self._ingest = ingest
        self._run_id = run_id
        self._heartbeat_interval = heartbeat_interval
        self._liveness = heartbeat_timeout / 2
        self._on_lost = on_lost
        self._batch_size = batch_size
        self._backoff = backoff
        self._component_id = component_id
        self._queue: queue.Queue[Event | None] = queue.Queue()
        self._effects: dict[str, Effect] | None = None
        self._ended = False
        self._lost: str | None = None
        self._thread = threading.Thread(target=self._write, name=f"recorder-{run_id}", daemon=True)

    # -- The run's side ----------------------------------------------------------

    def __call__(self, event: Event) -> None:
        """Hold an event for delivery; never blocks the emitter.

        Args:
            event: The event the runner emitted.
        """
        if self._component_id is not None and (
            event.type in _LIFECYCLE or event.metadata.get("component_id") not in (None, self._component_id)
        ):
            return
        self._queue.put(event)

    def finish(self, result: RunResult) -> None:
        """Take the run's result, whose effects go out with the last batch.

        Args:
            result: The run's result.
        """
        self._effects = result.effects

    def __enter__(self) -> Self:
        """Start delivering.

        Returns:
            This recorder.
        """
        self._thread.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Deliver everything held, however long that takes, then stop.

        Args:
            exc_type: The exception type raised in the block, if any.
            exc: The exception raised in the block, if any; never masked.
            traceback: Its traceback, if any.

        Raises:
            RunLostError: If the run was lost while recording.
        """
        self._queue.put(None)
        self._thread.join()
        if self._lost is not None and exc is None:
            raise RunLostError(f"Run {self._run_id} {self._lost}")

    # -- The writer thread -------------------------------------------------------

    def _write(self) -> None:
        """Deliver batches until the last one, or until the run can no longer report."""
        while True:
            batch, last = self._collect()
            try:
                running = self._deliver(batch, self._effects if last else None)
            except DeliveryError as error:
                self._lose(f"could not deliver its events: {error}")
                return
            self._ended = self._ended or any(event.type in _RUN_ENDINGS for event in batch)
            if not running and not self._ended and self._lost is None and (last or self._queue.empty()):
                self._lose("was ended elsewhere")
            if last:
                return

    def _collect(self) -> tuple[list[Event], bool]:
        """Take what is queued, up to one batch, waiting at most one heartbeat interval for the first event.

        The run's own ending is held until the block exits, so it travels
        with the effects: the store folds the verdict and applies the effects
        in one transaction, and a success never exists without them.

        Returns:
            The batch, empty when nothing arrived in time, and whether it is
            the last one.
        """
        batch: list[Event] = []
        try:
            item = self._queue.get(timeout=self._heartbeat_interval)
        except queue.Empty:
            return batch, False
        while item is not None:
            batch.append(item)
            if item.type in _RUN_ENDINGS:
                return batch + self._rest(), True
            if len(batch) >= self._batch_size:
                return batch, False
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                return batch, False
        return batch, True

    def _rest(self) -> list[Event]:
        """Wait for the block to exit, taking whatever is queued until then.

        Returns:
            The events queued after the run's ending, normally none.
        """
        rest: list[Event] = []
        while (item := self._queue.get()) is not None:
            rest.append(item)
        return rest

    def _deliver(self, batch: list[Event], effects: dict[str, Effect] | None) -> bool:
        """Deliver one batch, retrying a failing delivery for the liveness window.

        Args:
            batch: The events to deliver.
            effects: The effects to deliver with them, on the last batch only.

        Returns:
            Whether the run is still running.

        Raises:
            DeliveryError: If the delivery cannot be retried, or kept failing
                for half the heartbeat timeout.
        """
        first_failure: float | None = None
        delay = self._backoff
        while True:
            try:
                return self._ingest(EventBatch(batch, effects))
            except DeliveryError as error:
                if not error.retryable:
                    raise
                failure = error
            except Exception as error:  # noqa: BLE001 - any other failure of the transport is retried too
                failure = DeliveryError(f"{type(error).__name__}: {error}")
            if first_failure is None:
                first_failure = time.monotonic()
            if time.monotonic() - first_failure >= self._liveness:
                raise failure
            logger.warning("Run %s could not deliver %d event(s); retrying: %s", self._run_id, len(batch), failure)
            time.sleep(delay)
            delay = min(delay * 2, self._backoff * 10)

    def _lose(self, reason: str) -> None:
        """Mark the run lost and tell whoever asked to know.

        Args:
            reason: Why, for the log and the error.
        """
        self._lost = reason
        logger.warning("Run %s %s", self._run_id, reason)
        if self._on_lost is not None:
            self._on_lost()
