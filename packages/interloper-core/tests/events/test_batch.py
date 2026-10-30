"""Tests for ``interloper.events.batch``."""

from __future__ import annotations

import json
from typing import Any

import httpx2
import pytest

from interloper.errors import DeliveryError, EventError
from interloper.events import Event, EventBatch, EventType
from interloper.operation import Effect


def _client(handler: Any) -> httpx2.Client:
    """A client of the API over a scripted transport, as a run's process makes one.

    Args:
        handler: Answers every request.

    Returns:
        The client.
    """
    return httpx2.Client(
        base_url="http://api.internal",
        headers={"Authorization": "Bearer ilr_secret"},
        transport=httpx2.MockTransport(handler),
    )


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


class TestPost:
    """One batch per request to the run's ingest route, the answer as whether the run was live."""

    def test_posts_the_batch_to_the_runs_route_and_reads_whether_the_run_was_live(self) -> None:
        seen: dict[str, Any] = {}

        def handler(request: httpx2.Request) -> httpx2.Response:
            seen["url"] = str(request.url)
            seen["auth"] = request.headers["authorization"]
            seen["body"] = json.loads(request.content)
            return httpx2.Response(200, json={"live": False})

        event = Event(type=EventType.LOG, metadata={"message": "hi"})
        live = EventBatch([event], {"a": Effect(config={"k": 1})}).post(_client(handler), "run-1")

        assert live is False
        assert seen["url"] == "http://api.internal/api/runs/run-1/events"
        assert seen["auth"] == "Bearer ilr_secret"
        assert seen["body"] == {"events": [event.to_dict()], "effects": {"a": {"config": {"k": 1}, "state": {}}}}

    @pytest.mark.parametrize(
        ("status", "retryable"), [(404, True), (500, True), (503, True), (401, False), (422, False)]
    )
    def test_404_is_retryable_401_and_422_are_final(self, status: int, retryable: bool) -> None:
        with pytest.raises(DeliveryError) as raised:
            EventBatch().post(_client(lambda request: httpx2.Response(status)), "run-1")
        assert raised.value.retryable is retryable

    def test_a_connection_error_is_retryable(self) -> None:
        def handler(request: httpx2.Request) -> httpx2.Response:
            raise httpx2.ConnectError("refused")

        with pytest.raises(DeliveryError) as raised:
            EventBatch().post(_client(handler), "run-1")
        assert raised.value.retryable is True
