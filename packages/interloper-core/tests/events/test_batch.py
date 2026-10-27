"""Tests for ``interloper.events.batch``."""

from __future__ import annotations

from typing import Any

import pytest

from interloper.errors import EventError
from interloper.events import Event, EventBatch, EventType
from interloper.operation import Effect


class TestEventBatch:
    def test_a_batch_survives_its_json_shape(self) -> None:
        event = Event(type=EventType.LOG, metadata={"run_id": "run-1", "message": "hello"})
        batch = EventBatch([event], {"op-1": Effect(config={"token": "x"}, state={"cursor": 3})})

        parsed = EventBatch.from_dict(batch.to_dict())

        assert [parsed_event.to_dict() for parsed_event in parsed.events] == [event.to_dict()]
        assert parsed.effects == {"op-1": Effect(config={"token": "x"}, state={"cursor": 3})}

    def test_a_batch_without_effects_has_none(self) -> None:
        assert EventBatch.from_dict(EventBatch().to_dict()) == EventBatch()

    @pytest.mark.parametrize(
        "data",
        [
            {},
            {"events": [{"event_type": "no_such_type"}]},
            {"events": [], "effects": {"op-1": {"secrets": {}}}},
            {"events": [], "effects": ["op-1"]},
            {"events": [], "effects": {"op-1": {"config": "rotated"}}},
            {"events": [], "effects": {"op-1": {"state": ["cursor"]}}},
        ],
    )
    def test_a_malformed_batch_is_refused(self, data: dict[str, Any]) -> None:
        with pytest.raises(EventError):
            EventBatch.from_dict(data)
