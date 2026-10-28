"""Tests for ``interloper.events.recorder``."""

from __future__ import annotations

import threading
import time
from typing import Any

import pytest

from interloper.errors import DeliveryError, RunLostError
from interloper.events import Event, EventBatch, EventRecorder, EventType
from interloper.operation.base import Effect, OperationResult
from interloper.runner import ExecutionInfo, ExecutionStatus, RunResult


class Sink:
    """Records every delivery; scripted to fail, refuse, report the run ended or hold a delivery."""

    def __init__(
        self, *, fail_times: int = 0, live: bool = True, final: bool = False, gate: threading.Event | None = None
    ) -> None:
        """Script the sink.

        Args:
            fail_times: How many deliveries raise before one succeeds.
            live: What every successful delivery answers.
            final: Whether the failures are final rather than retryable.
            gate: When set, the first delivery waits for it, and ``holding``
                tells when that delivery has arrived.
        """
        self.batches: list[tuple[list[Event], Any]] = []
        self.fail_times = fail_times
        self.live = live
        self.final = final
        self.gate = gate
        self.holding = threading.Event()
        self.lock = threading.Lock()

    def __call__(self, batch: EventBatch) -> bool:
        """Take one delivery.

        Args:
            batch: The events and the effects sent with them.

        Returns:
            Whether the run was live when the batch arrived.

        Raises:
            DeliveryError: While scripted to fail.
        """
        if self.gate is not None and not self.holding.is_set():
            self.holding.set()
            self.gate.wait()
        with self.lock:
            if self.fail_times:
                self.fail_times -= 1
                raise DeliveryError("down", retryable=not self.final)
            self.batches.append((list(batch.events), batch.effects))
            return self.live

    @property
    def events(self) -> list[Event]:
        """Every event delivered, in order."""
        return [event for batch, _ in self.batches for event in batch]


def _event(index: int) -> Event:
    return Event(type=EventType.LOG, metadata={"run_id": "run-1", "message": str(index)})


def _recorder(sink: Sink, **overrides: Any) -> EventRecorder:
    options: dict[str, Any] = {"run_id": "run-1", "heartbeat_interval": 0.05, "heartbeat_timeout": 0.4, "backoff": 0.01}
    options.update(overrides)
    return EventRecorder(sink, **options)


def _result(**config: Any) -> RunResult:
    info = ExecutionInfo("a", "a", ExecutionStatus.COMPLETED, result=OperationResult(config=config))
    return RunResult(executions={"a": info})


def _record(sink: Sink, count: int, **overrides: Any) -> None:
    """Record *count* events through a recorder over *sink*, then leave its block.

    Args:
        sink: Where the batches go.
        count: How many events the run emits.
        **overrides: Recorder options over the fast test defaults.
    """
    with _recorder(sink, **overrides) as recorder:
        for index in range(1, count + 1):
            recorder(_event(index))


class TestDelivery:
    """Every event reaches the platform, in order, in batches."""

    def test_delivers_every_event_in_order_and_the_effects_with_the_last_batch(self) -> None:
        sink = Sink()
        with _recorder(sink) as recorder:
            for index in range(1200):
                recorder(_event(index))
            recorder.finish(_result(k=1))
        assert [event.metadata["message"] for event in sink.events] == [str(index) for index in range(1200)]
        assert all(len(batch) <= 500 for batch, _ in sink.batches)
        assert sink.batches[-1][1] == {"a": Effect(config={"k": 1}, state={})}
        assert all(effects is None for _, effects in sink.batches[:-1])

    def test_closing_without_events_still_delivers_once(self) -> None:
        sink = Sink()
        _record(sink, 0, heartbeat_interval=10.0)
        assert sink.batches == [([], None)]

    def test_idle_it_delivers_an_empty_batch_as_a_heartbeat(self) -> None:
        sink = Sink()
        with _recorder(sink):
            time.sleep(0.2)
        assert len(sink.batches) >= 2
        assert all(batch == [] for batch, _ in sink.batches)

    def test_the_runs_ending_waits_to_travel_with_the_effects(self) -> None:
        sink = Sink()
        with _recorder(sink, heartbeat_interval=10.0) as recorder:
            recorder(Event(type=EventType.RUN_COMPLETED, metadata={"run_id": "run-1"}))
            time.sleep(0.1)
            recorder.finish(_result(k=1))
        (ending_batch,) = [(batch, effects) for batch, effects in sink.batches if batch]
        assert [event.type for event in ending_batch[0]] == [EventType.RUN_COMPLETED]
        assert ending_batch[1] == {"a": Effect(config={"k": 1}, state={})}

    def test_effects_travel_beside_the_batch_never_inside_events(self) -> None:
        sink = Sink()
        with _recorder(sink) as recorder:
            recorder(_event(1))
            recorder.finish(_result(refresh_token="rotated"))
        assert all("rotated" not in event.to_json() for event in sink.events)
        assert sink.batches[-1][1]["a"].config["refresh_token"] == "rotated"


class TestLiveness:
    """A delivery is retried while the run can still prove it is alive, and the run is lost past that."""

    def test_a_failed_delivery_is_retried(self) -> None:
        sink = Sink(fail_times=2)
        _record(sink, 1)
        assert [event.metadata["message"] for event in sink.events] == ["1"]

    def test_a_delivery_failing_for_half_the_timeout_loses_the_run(self) -> None:
        lost = threading.Event()
        with pytest.raises(RunLostError, match="could not deliver"):
            _record(Sink(fail_times=1000), 1, on_lost=lost.set)
        assert lost.is_set()

    def test_a_final_delivery_error_loses_the_run_at_once(self) -> None:
        lost = threading.Event()
        started = time.monotonic()
        with pytest.raises(RunLostError, match="could not deliver"):
            _record(Sink(fail_times=1, final=True), 1, on_lost=lost.set, heartbeat_timeout=60.0)
        assert lost.is_set()
        assert time.monotonic() - started < 1.0

    def test_a_run_ended_elsewhere_drains_then_is_lost(self) -> None:
        sink = Sink(live=False)
        lost = threading.Event()
        with pytest.raises(RunLostError, match="ended elsewhere"):
            _record(sink, 2, on_lost=lost.set, heartbeat_interval=10.0)
        assert [event.metadata["message"] for event in sink.events] == ["1", "2"]
        assert lost.is_set()

    def test_a_run_ended_before_its_own_ending_arrived_is_lost(self) -> None:
        lost = threading.Event()

        def record() -> None:
            with _recorder(Sink(live=False), on_lost=lost.set, heartbeat_interval=10.0) as recorder:
                recorder(_event(1))
                recorder(Event(type=EventType.RUN_COMPLETED, metadata={"run_id": "run-1"}))

        with pytest.raises(RunLostError, match="ended elsewhere"):
            record()
        assert lost.is_set()

    def test_a_run_ended_elsewhere_while_more_is_queued_is_still_lost(self) -> None:
        gate = threading.Event()
        sink = Sink(live=False, gate=gate)

        def record() -> None:
            with _recorder(sink, heartbeat_interval=10.0) as recorder:
                recorder(_event(1))
                assert sink.holding.wait(5)
                recorder(_event(2))
                recorder(Event(type=EventType.RUN_COMPLETED, metadata={"run_id": "run-1"}))
                gate.set()

        with pytest.raises(RunLostError, match="ended elsewhere"):
            record()
        assert [event.metadata.get("message") for event in sink.events][:2] == ["1", "2"]

    def test_an_exception_leaving_the_block_is_not_masked(self) -> None:
        def raise_inside() -> None:
            with _recorder(Sink(live=False), heartbeat_interval=10.0) as recorder:
                recorder(_event(1))
                raise ValueError("mine")

        with pytest.raises(ValueError, match="mine"):
            raise_inside()


class TestOperationFilter:
    """A child reports only what happens inside its operation: the lifecycle and its upstreams' reads are the host's."""

    def test_only_what_happens_inside_the_operation_is_delivered(self) -> None:
        sink = Sink()
        inside = Event(type=EventType.ASSET_DATA_STARTED, metadata={"run_id": "run-1", "component_id": "op-1"})
        lifecycle = Event(type=EventType.OPERATION_STARTED, metadata={"run_id": "run-1", "component_id": "op-1"})
        logged = Event(type=EventType.LOG, metadata={"run_id": "run-1"})
        theirs = Event(type=EventType.DEST_READ_COMPLETED, metadata={"run_id": "run-1", "component_id": "op-2"})
        started = Event(type=EventType.RUN_STARTED, metadata={"run_id": "run-1"})

        with _recorder(sink, component_id="op-1") as recorder:
            for event in (started, lifecycle, inside, logged, theirs):
                recorder(event)

        assert [event.type for event in sink.events] == [EventType.ASSET_DATA_STARTED, EventType.LOG]
