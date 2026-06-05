"""Tests for :class:`HttpEventSink`."""

from __future__ import annotations

import json
from collections.abc import Callable
from uuid import uuid4

import httpx

from interloper.events import Event, EventType, HttpEventSink


def _sink(handler: Callable[[httpx.Request], httpx.Response]) -> HttpEventSink:
    client = httpx.Client(transport=httpx.MockTransport(handler))
    return HttpEventSink(
        base_url="http://api",
        token="tok",
        run_id=str(uuid4()),
        client=client,
        flush_interval=0.02,
    )


def test_posts_buffered_events_and_excludes_host_owned() -> None:
    """Asset lifecycle/log events ship; host-owned events (run-level, queued) don't."""
    posted: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["authorization"] == "Bearer tok"
        posted.append(json.loads(request.content))
        return httpx.Response(200, json={"accepted": 1, "rejected": 0})

    sink = _sink(handler)
    sink(Event(type=EventType.LOG, metadata={"message": "a"}))
    sink(Event(type=EventType.RUN_STARTED))
    sink(Event(type=EventType.ASSET_QUEUED, metadata={"asset_key": "x"}))
    sink(Event(type=EventType.ASSET_STARTED, metadata={"asset_key": "x"}))
    sink.close()

    types = [e["type"] for batch in posted for e in batch["events"]]
    assert "log" in types
    assert "asset_started" in types
    assert "run_started" not in types
    assert "asset_queued" not in types


def test_targets_run_scoped_ingest_url() -> None:
    """Events POST to /api/internal/runs/<run_id>/events."""
    seen: dict[str, str] = {}
    run_id = str(uuid4())

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        return httpx.Response(200, json={})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    sink = HttpEventSink(base_url="http://api/", token="t", run_id=run_id, client=client, flush_interval=0.02)
    sink(Event(type=EventType.LOG))
    sink.close()

    assert seen["url"] == f"http://api/api/internal/runs/{run_id}/events"


def test_retries_transient_failure_then_succeeds() -> None:
    """A 5xx is retried until it succeeds."""
    attempts = {"n": 0}
    delivered: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts["n"] += 1
        if attempts["n"] == 1:
            return httpx.Response(503, json={})
        delivered.append(json.loads(request.content))
        return httpx.Response(200, json={})

    sink = _sink(handler)
    sink(Event(type=EventType.LOG, metadata={"message": "boom"}))
    sink.close()

    assert attempts["n"] >= 2
    assert delivered


def test_drops_on_client_error_without_spinning() -> None:
    """A 4xx (e.g. bad token) drops the batch instead of retrying forever."""
    attempts = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        attempts["n"] += 1
        return httpx.Response(401, json={})

    sink = _sink(handler)
    sink(Event(type=EventType.LOG))
    sink.close()

    assert attempts["n"] == 1
