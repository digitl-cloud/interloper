"""Tests for ``interloper.hook.webhook``."""

from typing import Any

import httpx
import pytest

import interloper as il


class TestWebhookHook:
    def test_posts_fixed_payload(self, monkeypatch: pytest.MonkeyPatch):
        calls: list[tuple[str, dict[str, Any]]] = []

        def fake_post(url: str, *, json: dict[str, Any], timeout: float) -> httpx.Response:
            calls.append((url, json))
            return httpx.Response(200, request=httpx.Request("POST", url))

        monkeypatch.setattr(httpx, "post", fake_post)
        hook = il.WebhookHook(id="h1", url="https://example.test/hook")
        hook.fire(
            il.HookContext(
                event_type="run_failed", component_id="c1", run_id="r1", metadata={"error": "boom"}
            )
        )

        url, payload = calls[0]
        assert url == "https://example.test/hook"
        assert payload == {
            "event_type": "run_failed",
            "component_id": "c1",
            "run_id": "r1",
            "partition_date": None,
            "hook_id": "h1",
            "metadata": {"error": "boom"},
        }

    def test_error_status_raises(self, monkeypatch: pytest.MonkeyPatch):
        def fake_post(url: str, *, json: dict[str, Any], timeout: float) -> httpx.Response:
            return httpx.Response(500, request=httpx.Request("POST", url))

        monkeypatch.setattr(httpx, "post", fake_post)
        with pytest.raises(httpx.HTTPStatusError):
            il.WebhookHook(url="https://example.test/hook").fire(
                il.HookContext(event_type="run_failed", component_id="c1")
            )
