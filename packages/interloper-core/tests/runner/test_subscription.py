"""Tests for ``interloper.runner.subscription``."""

from interloper.events import Event, EventBus, EventType
from interloper.runner.subscription import RunSubscription


async def test_delivers_only_its_runs_events_and_flushes_on_exit():
    """A subscription delivers events carrying its run id, all of them by the time it exits."""
    delivered: list[Event] = []
    async with RunSubscription(delivered.append, "run-1"):
        EventBus.emit(EventType.RUN_STARTED, metadata={"run_id": "run-1"})
        EventBus.emit(EventType.RUN_STARTED, metadata={"run_id": "run-2"})
    assert [event.metadata["run_id"] for event in delivered] == ["run-1"]


async def test_unsubscribes_on_exit():
    """After the block, the handler receives nothing."""
    delivered: list[Event] = []
    async with RunSubscription(delivered.append, "run-1"):
        pass
    EventBus.emit(EventType.RUN_STARTED, metadata={"run_id": "run-1"})
    EventBus.flush(timeout=5.0)
    assert delivered == []
