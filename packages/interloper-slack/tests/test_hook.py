"""Tests for ``interloper_slack.hook``."""

from typing import Any

import httpx
import interloper as il
import pytest
from interloper.errors import ConfigError

from interloper_slack import SlackAPIError, SlackConnection, SlackHook


@pytest.fixture
def posted(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Capture ``chat.postMessage`` payloads, answering every call with ``ok``."""
    calls: list[dict[str, Any]] = []

    def fake_post(url: str, *, json: dict[str, Any], **kwargs) -> httpx.Response:
        calls.append(json)
        return httpx.Response(200, json={"ok": True}, request=httpx.Request("POST", url))

    monkeypatch.setattr(httpx, "post", fake_post)
    return calls


def _hook(**kwargs: Any) -> SlackHook:
    return SlackHook(connection=SlackConnection(bot_token="xoxb-t"), channel="C123", **kwargs)


def _text(payload: dict[str, Any]) -> str:
    return payload["blocks"][0]["text"]["text"]


class TestFire:
    def test_posts_failure_message(self, posted: list[dict[str, Any]]):
        _hook().fire(
            il.HookContext(
                event_type="run_failed",
                component_id="c1",
                run_id="r1",
                partition_date="2026-07-30",
                metadata={"status": "failed", "component_name": "Facebook Ads", "error": "boom"},
            )
        )

        payload = posted[0]
        assert payload["channel"] == "C123"
        assert payload["text"] == "Facebook Ads failed"
        assert _text(payload) == (
            ":x: *Facebook Ads* failed\nRun `r1` · partition `2026-07-30`\n```boom```"
        )

    def test_posts_success_message(self, posted: list[dict[str, Any]]):
        _hook().fire(
            il.HookContext(
                event_type="run_completed",
                component_id="c1",
                run_id="r1",
                metadata={"component_name": "Facebook Ads"},
            )
        )

        assert _text(posted[0]) == ":white_check_mark: *Facebook Ads* completed\nRun `r1`"

    def test_falls_back_to_component_id_without_a_name(self, posted: list[dict[str, Any]]):
        _hook().fire(il.HookContext(event_type="run_failed", component_id="c1"))

        assert _text(posted[0]) == ":x: *c1* failed"
        assert posted[0]["text"] == "c1 failed"

    def test_unknown_event_type_still_posts(self, posted: list[dict[str, Any]]):
        # The evaluator only produces the two run outcomes today; an added
        # event type must not silence the notification.
        _hook().fire(il.HookContext(event_type="run_cancelled", component_id="c1"))

        assert _text(posted[0]) == ":bell: *c1* run_cancelled"

    def test_partition_without_run_id(self, posted: list[dict[str, Any]]):
        _hook().fire(il.HookContext(event_type="run_failed", component_id="c1", partition_date="2026-07-30"))

        assert _text(posted[0]) == ":x: *c1* failed\npartition `2026-07-30`"

    def test_forwards_configured_timeout(self, monkeypatch: pytest.MonkeyPatch):
        seen: dict[str, Any] = {}

        def fake_post(url: str, **kwargs) -> httpx.Response:
            seen.update(kwargs)
            return httpx.Response(200, json={"ok": True}, request=httpx.Request("POST", url))

        monkeypatch.setattr(httpx, "post", fake_post)
        _hook(timeout=2.5).fire(il.HookContext(event_type="run_failed", component_id="c1"))

        assert seen["timeout"] == 2.5
        assert seen["headers"] == {"Authorization": "Bearer xoxb-t"}

    def test_without_connection_raises(self):
        with pytest.raises(ConfigError, match="without a Slack connection"):
            SlackHook(id="h1", channel="C123").fire(  # ty: ignore[missing-argument]
                il.HookContext(event_type="run_failed", component_id="c1")
            )

    def test_slack_rejection_propagates(self, monkeypatch: pytest.MonkeyPatch):
        # The evaluator records the failure on the firing claim, so fire()
        # must raise rather than swallow a rejected post.
        def fake_post(url: str, **kwargs) -> httpx.Response:
            return httpx.Response(
                200, json={"ok": False, "error": "not_in_channel"}, request=httpx.Request("POST", url)
            )

        monkeypatch.setattr(httpx, "post", fake_post)

        with pytest.raises(SlackAPIError, match="not_in_channel"):
            _hook().fire(il.HookContext(event_type="run_failed", component_id="c1"))


class TestDefinition:
    def test_declares_a_connection_slot(self):
        assert SlackHook.resource_types["connection"] is SlackConnection

    def test_defaults_to_failures_only(self):
        assert _hook().events == ["run_failed"]

    def test_is_a_catalogued_hook(self):
        definition = SlackHook.definition()
        assert (definition.kind, definition.key, definition.name) == ("hook", "slack_hook", "Slack")
        assert definition.relations["resource"].slots["connection"].key == "slack_connection"
