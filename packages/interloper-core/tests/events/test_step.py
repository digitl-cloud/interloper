"""Tests for ``interloper.events.step``."""

from __future__ import annotations

import pytest

from interloper.events import Event, EventBus, EventType
from interloper.events.step import Step, StepKind
from interloper.telemetry import attributes


def _capture(body: object) -> list[Event]:
    captured: list[Event] = []
    EventBus.subscribe(captured.append)
    try:
        body()  # ty: ignore[call-non-callable]
    finally:
        EventBus.flush(timeout=5.0)
        EventBus.unsubscribe(captured.append)
    return captured


class TestStepKind:
    def test_names_its_event_types(self):
        assert StepKind.DEST_WRITE.started is EventType.DEST_WRITE_STARTED
        assert StepKind.DEST_WRITE.completed is EventType.DEST_WRITE_COMPLETED
        assert StepKind.DEST_WRITE.failed is EventType.DEST_WRITE_FAILED
        assert StepKind.ASSET_DATA.started is EventType.ASSET_DATA_STARTED
        assert StepKind.DEST_READ.failed is EventType.DEST_READ_FAILED

    def test_names_its_span(self):
        assert StepKind.ASSET_DATA.span == "interloper.asset.data"
        assert StepKind.DEST_READ.span == "interloper.destination.read"
        assert StepKind.DEST_WRITE.span == "interloper.destination.write"

    def test_keeps_the_wording(self):
        assert StepKind.ASSET_DATA.message("started", "'orders'") == "Executing 'orders'"
        assert StepKind.ASSET_DATA.message("completed", "'orders'") == "Executed 'orders'"
        failed = StepKind.ASSET_DATA.message("failed", "'orders'", "ValueError: x")
        assert failed == "Execution of 'orders' failed: ValueError: x"
        assert StepKind.DEST_WRITE.message("started", "'orders'") == "Writing 'orders'"
        assert StepKind.DEST_WRITE.message("completed", "'orders'") == "Wrote 'orders'"
        assert StepKind.DEST_WRITE.message("failed", "'orders'", "boom") == "Failed to write 'orders': boom"
        assert StepKind.DEST_READ.message("started", "'orders'") == "Reading 'orders'"
        assert StepKind.DEST_READ.message("completed", "'orders'") == "Read 'orders'"
        assert StepKind.DEST_READ.message("failed", "'orders'", "boom") == "Failed to read 'orders': boom"


class TestStep:
    def test_a_completing_body_emits_started_then_completed_with_the_recorded_extra(self):
        metadata = {"run_id": "r1", "component_id": "c1"}

        def body() -> None:
            with Step(StepKind.DEST_WRITE, metadata, subject="'orders'") as step:
                step.record(rows=3)

        events = _capture(body)
        assert [event.type for event in events] == [EventType.DEST_WRITE_STARTED, EventType.DEST_WRITE_COMPLETED]
        assert events[0].metadata == {**metadata, "message": "Writing 'orders'"}
        assert events[1].metadata == {**metadata, "rows": 3, "message": "Wrote 'orders'"}

    def test_a_raising_body_emits_failed_and_re_raises(self):
        metadata = {"run_id": "r1", "component_id": "c1"}

        def body() -> None:
            with pytest.raises(ValueError, match="boom"), Step(StepKind.ASSET_DATA, metadata, subject="'orders'"):
                raise ValueError("boom")

        events = _capture(body)
        assert [event.type for event in events] == [EventType.ASSET_DATA_STARTED, EventType.ASSET_DATA_FAILED]
        failed = events[1].metadata
        assert failed["error"] == "ValueError: boom"
        assert failed["message"] == "Execution of 'orders' failed: ValueError: boom"
        assert "ValueError: boom" in failed["traceback"]
        assert EventType.ASSET_DATA_COMPLETED not in [event.type for event in events]

    def test_the_span_carries_the_metadata_attributes_and_the_extra_ones(self, span_exporter):
        with Step(StepKind.DEST_READ, {"run_id": "r1"}, subject="'x'", attributes={attributes.UPSTREAM_KEY: "x"}):
            pass
        [span] = [span for span in span_exporter.get_finished_spans() if span.name == "interloper.destination.read"]
        assert span.attributes[attributes.RUN_ID] == "r1"
        assert span.attributes[attributes.UPSTREAM_KEY] == "x"
