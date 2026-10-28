"""Tests for ``interloper.events.client``."""

from __future__ import annotations

import json
from typing import Any

import httpx2
import pytest

from interloper.errors import DeliveryError
from interloper.events import Event, EventBatch, EventClient, EventType
from interloper.operation import Effect


def _client(handler: Any) -> EventClient:
    """A client over a scripted transport.

    Args:
        handler: Answers every request.

    Returns:
        The client.
    """
    return EventClient("http://api.internal", "ilr_secret", "run-1", transport=httpx2.MockTransport(handler))


class TestIngest:
    """One batch per request, the run's token as the bearer, the answer as whether the run was live."""

    def test_posts_the_batch_as_json_with_the_bearer_and_reads_whether_the_run_was_live(self) -> None:
        seen: dict[str, Any] = {}

        def handler(request: httpx2.Request) -> httpx2.Response:
            seen["url"] = str(request.url)
            seen["auth"] = request.headers["authorization"]
            seen["body"] = json.loads(request.content)
            return httpx2.Response(200, json={"live": False})

        event = Event(type=EventType.LOG, metadata={"message": "hi"})
        live = _client(handler).ingest(EventBatch([event], {"a": Effect(config={"k": 1})}))

        assert live is False
        assert seen["url"] == "http://api.internal/api/runs/run-1/events"
        assert seen["auth"] == "Bearer ilr_secret"
        assert seen["body"] == {"events": [event.to_dict()], "effects": {"a": {"config": {"k": 1}, "state": {}}}}

    @pytest.mark.parametrize(
        ("status", "retryable"), [(404, True), (500, True), (503, True), (401, False), (422, False)]
    )
    def test_404_is_retryable_401_and_422_are_final(self, status: int, retryable: bool) -> None:
        with pytest.raises(DeliveryError) as raised:
            _client(lambda request: httpx2.Response(status)).ingest(EventBatch())
        assert raised.value.retryable is retryable

    def test_a_connection_error_is_retryable(self) -> None:
        def handler(request: httpx2.Request) -> httpx2.Response:
            raise httpx2.ConnectError("refused")

        with pytest.raises(DeliveryError) as raised:
            _client(handler).ingest(EventBatch())
        assert raised.value.retryable is True
