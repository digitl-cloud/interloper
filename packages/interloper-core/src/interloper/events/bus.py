"""Thread-safe singleton event bus with background worker dispatch."""

from __future__ import annotations

import atexit
import queue
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from interloper.events.event import Event
from interloper.events.types import EventType


@dataclass
class _Sentinel:
    """Marker placed on the event queue to signal that all preceding events have been processed.

    Used by `EventBus.flush` to block until the queue is drained up to
    this point.  The ``done`` event is set by the worker thread when it
    dequeues the sentinel.  `EventBus.shutdown` places one with ``stop``
    set, on which the worker exits once everything ahead of it is delivered.
    """

    done: threading.Event
    stop: bool = False


class EventBus:
    """Thread-safe singleton event bus.

    Events are enqueued via `emit` and processed asynchronously by a
    background daemon thread.  Handlers are called in subscription order.

    All interaction goes through static methods — the singleton is an
    implementation detail:

    ```py
    EventBus.subscribe(my_handler)
    EventBus.emit(EventType.ASSET_DATA_STARTED)
    EventBus.flush()
    ```
    """

    _instance: EventBus | None = None
    _lock = threading.Lock()

    def __new__(cls) -> EventBus:  # noqa: PYI034
        """Create or return the singleton instance (double-checked locking).

        Returns:
            The process-wide EventBus instance.
        """
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    bus = super().__new__(cls)
                    bus._initialized = False
                    cls._instance = bus
        return cls._instance

    def __init__(self) -> None:
        """Initialize handlers, queue, and background worker (runs once)."""
        if self._initialized:
            return

        self._handlers: dict[Callable[[Event], None], list[EventType] | None] = {}
        self._handler_lock = threading.Lock()
        self._queue: queue.Queue[Event | _Sentinel] = queue.Queue()
        self._shutdown_flag = threading.Event()
        self._worker: threading.Thread | None = None
        self._initialized = True

        self._start_worker()
        atexit.register(EventBus.shutdown)

    # -- Public API ------------------------------------------------------------

    @staticmethod
    def emit(event_type: EventType, *, metadata: dict[str, Any] | None = None) -> None:
        """Emit an event on the global bus.

        Args:
            event_type: The type of event to emit.
            metadata: Optional key-value pairs attached to the event.
        """
        EventBus()._enqueue(Event(type=event_type, metadata=metadata or {}))

    @staticmethod
    def emit_event(event: Event) -> None:
        """Emit an existing `Event` as-is on the global bus.

        Unlike `emit`, this preserves the event's ``id`` and
        ``timestamp`` rather than creating a fresh event.  Use it to
        re-emit an event received from another process (e.g. parsed from a
        child container's stderr) so its identity survives end-to-end and
        the event persists idempotently.

        Args:
            event: The event to enqueue unchanged.
        """
        EventBus()._enqueue(event)

    @staticmethod
    def subscribe(
        handler: Callable[[Event], None],
        event_types: list[EventType] | None = None,
    ) -> None:
        """Register a handler.

        Args:
            handler: Callable invoked with each matching event.
            event_types: If provided, only events of these types are delivered.
                ``None`` means all event types.
        """
        bus = EventBus()
        with bus._handler_lock:
            bus._handlers[handler] = event_types

    @staticmethod
    def unsubscribe(handler: Callable[[Event], None]) -> None:
        """Remove a previously registered handler.

        Args:
            handler: Handler to remove.
        """
        bus = EventBus()
        with bus._handler_lock:
            bus._handlers.pop(handler, None)

    @staticmethod
    def flush(timeout: float | None = None) -> bool:
        """Block until all currently queued events have been processed.

        Places a sentinel on the queue and waits for the worker to reach it.

        Args:
            timeout: Maximum seconds to wait.  ``None`` waits indefinitely.

        Returns:
            ``True`` if the flush completed, ``False`` on timeout.
        """
        bus = EventBus()
        done = threading.Event()
        bus._queue.put(_Sentinel(done=done))
        return done.wait(timeout)

    @staticmethod
    def shutdown() -> None:
        """Drain the queue and stop the background worker.

        Safe to call multiple times (idempotent).
        """
        bus = EventBus()
        if bus._shutdown_flag.is_set():
            return

        bus._shutdown_flag.set()

        if bus._worker is not None and bus._worker.is_alive():
            bus._queue.put(_Sentinel(done=threading.Event(), stop=True))
            try:
                bus._queue.join()
            except Exception:  # noqa: BLE001, S110
                pass
            bus._worker.join(timeout=2.0)

    # -- Internals -------------------------------------------------------------

    def _enqueue(self, event: Event) -> None:
        """Place an event on the internal queue for worker dispatch.

        Args:
            event: The event to hand over to the background worker.
        """
        self._queue.put(event)

    def _start_worker(self) -> None:
        """Start the background daemon thread that drains the event queue."""
        if self._worker is None or not self._worker.is_alive():
            self._worker = threading.Thread(target=self._process_events, daemon=True)
            self._worker.start()

    def _process_events(self) -> None:
        """Worker loop: dequeue events and dispatch to handlers until a stop sentinel."""
        while True:
            item = self._queue.get()

            if isinstance(item, _Sentinel):
                item.done.set()
                self._queue.task_done()
                if item.stop:
                    return
                continue

            try:
                with self._handler_lock:
                    targets = [
                        handler for handler, types in self._handlers.items() if types is None or item.type in types
                    ]
                for handler in targets:
                    try:
                        handler(item)
                    except Exception:  # noqa: BLE001, S110
                        pass  # isolate handler errors
            finally:
                self._queue.task_done()
