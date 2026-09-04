"""Tests for ``interloper.events.bus``."""

from __future__ import annotations

import atexit
import threading
from collections.abc import Iterator

import pytest

from interloper.events import Event, EventBus, EventType


@pytest.fixture
def isolated_bus(monkeypatch: pytest.MonkeyPatch) -> Iterator[type[EventBus]]:
    """Swap the process-wide singleton for a fresh bus, restoring it after.

    Tests that shut the bus down would otherwise stop event delivery for
    every suite that runs later in the same process.

    Args:
        monkeypatch: Fixture used to stop the new bus registering an
            ``atexit`` hook of its own.

    Yields:
        The ``EventBus`` class, now backed by a fresh instance.
    """
    monkeypatch.setattr(atexit, "register", lambda *args, **kwargs: None)
    original = EventBus._instance
    EventBus._instance = None
    EventBus()
    yield EventBus
    EventBus.shutdown()
    EventBus._instance = original


class TestSubscription:
    """Handler registration and filtering."""

    def test_a_handler_receives_every_event_by_default(self, isolated_bus: type[EventBus]) -> None:
        received: list[Event] = []
        isolated_bus.subscribe(received.append)

        isolated_bus.emit(EventType.RUN_STARTED)
        isolated_bus.emit(EventType.RUN_COMPLETED)
        isolated_bus.flush(timeout=5.0)

        assert [event.type for event in received] == [EventType.RUN_STARTED, EventType.RUN_COMPLETED]

    def test_event_types_narrow_the_delivery(self, isolated_bus: type[EventBus]) -> None:
        received: list[Event] = []
        isolated_bus.subscribe(received.append, [EventType.RUN_FAILED])

        isolated_bus.emit(EventType.RUN_STARTED)
        isolated_bus.emit(EventType.RUN_FAILED)
        isolated_bus.flush(timeout=5.0)

        assert [event.type for event in received] == [EventType.RUN_FAILED]

    def test_metadata_travels_with_the_event(self, isolated_bus: type[EventBus]) -> None:
        received: list[Event] = []
        isolated_bus.subscribe(received.append)

        isolated_bus.emit(EventType.RUN_STARTED, metadata={"run_id": "r1"})
        isolated_bus.flush(timeout=5.0)

        assert received[0].metadata == {"run_id": "r1"}

    def test_an_unsubscribed_handler_stops_receiving(self, isolated_bus: type[EventBus]) -> None:
        received: list[Event] = []
        isolated_bus.subscribe(received.append)
        isolated_bus.unsubscribe(received.append)

        isolated_bus.emit(EventType.RUN_STARTED)
        isolated_bus.flush(timeout=5.0)

        assert received == []

    def test_unsubscribing_an_unknown_handler_is_a_no_op(self, isolated_bus: type[EventBus]) -> None:
        isolated_bus.unsubscribe(lambda event: None)

    def test_resubscribing_replaces_the_filter(self, isolated_bus: type[EventBus]) -> None:
        received: list[Event] = []
        isolated_bus.subscribe(received.append, [EventType.RUN_FAILED])
        isolated_bus.subscribe(received.append)

        isolated_bus.emit(EventType.RUN_STARTED)
        isolated_bus.flush(timeout=5.0)

        assert len(received) == 1


class TestHandlerIsolation:
    """One broken handler must not silence the others."""

    def test_a_raising_handler_does_not_block_delivery(self, isolated_bus: type[EventBus]) -> None:
        received: list[Event] = []

        def broken(event: Event) -> None:
            raise RuntimeError("handler bug")

        isolated_bus.subscribe(broken)
        isolated_bus.subscribe(received.append)

        isolated_bus.emit(EventType.RUN_STARTED)
        isolated_bus.flush(timeout=5.0)

        assert len(received) == 1

    def test_the_worker_survives_a_raising_handler(self, isolated_bus: type[EventBus]) -> None:
        received: list[Event] = []

        def broken(event: Event) -> None:
            raise RuntimeError("handler bug")

        isolated_bus.subscribe(broken)
        isolated_bus.emit(EventType.RUN_STARTED)
        isolated_bus.flush(timeout=5.0)

        isolated_bus.subscribe(received.append)
        isolated_bus.emit(EventType.RUN_COMPLETED)
        isolated_bus.flush(timeout=5.0)

        assert len(received) == 1


class TestSingleton:
    """All interaction goes through one instance."""

    def test_construction_returns_the_same_instance(self, isolated_bus: type[EventBus]) -> None:
        assert EventBus() is EventBus()

    def test_re_initialization_keeps_the_registered_handlers(self, isolated_bus: type[EventBus]) -> None:
        received: list[Event] = []
        isolated_bus.subscribe(received.append)

        EventBus()  # __init__ runs again on the existing instance
        isolated_bus.emit(EventType.RUN_STARTED)
        isolated_bus.flush(timeout=5.0)

        assert len(received) == 1


class TestShutdown:
    """Draining and stopping the background worker."""

    def test_drains_the_queue_before_stopping(self, isolated_bus: type[EventBus]) -> None:
        received: list[Event] = []
        isolated_bus.subscribe(received.append)
        for _ in range(20):
            isolated_bus.emit(EventType.RUN_STARTED)

        isolated_bus.shutdown()

        assert len(received) == 20
        worker = EventBus()._worker
        assert worker is not None
        assert worker.is_alive() is False

    def test_is_idempotent(self, isolated_bus: type[EventBus]) -> None:
        isolated_bus.shutdown()

        isolated_bus.shutdown()

    def test_a_flush_after_shutdown_times_out(self, isolated_bus: type[EventBus]) -> None:
        isolated_bus.shutdown()

        # No worker is left to dequeue the sentinel.
        assert isolated_bus.flush(timeout=0.2) is False


def test_emit_event_preserves_the_producers_identity(isolated_bus: type[EventBus]) -> None:
    """A re-emitted event keeps its id, so persistence stays idempotent."""
    received: list[Event] = []
    isolated_bus.subscribe(received.append)
    original = Event(type=EventType.RUN_STARTED, metadata={"run_id": "r1"})

    isolated_bus.emit_event(original)
    isolated_bus.flush(timeout=5.0)

    assert received[0].id == original.id
    assert received[0].timestamp == original.timestamp


def test_events_emitted_from_several_threads_all_arrive(isolated_bus: type[EventBus]) -> None:
    """The queue is the thread-safety boundary; every producer's event lands."""
    received: list[Event] = []
    isolated_bus.subscribe(received.append)

    def produce() -> None:
        for _ in range(10):
            isolated_bus.emit(EventType.RUN_STARTED)

    threads = [threading.Thread(target=produce) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)
    isolated_bus.flush(timeout=5.0)

    assert len(received) == 40
