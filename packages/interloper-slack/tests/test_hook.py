"""Tests for ``interloper_slack.hook``."""

from typing import Any

import httpx
import interloper as il
import pytest
from interloper.errors import ConfigError

from interloper_slack import SlackConnection, SlackHook


def _hook(**kwargs: Any) -> SlackHook:
    return SlackHook(connection=SlackConnection(bot_token="xoxb-t"), channel="C123", **kwargs)


def _text(slack) -> str:
    return slack.json_body()["blocks"][0]["text"]["text"]


class TestFire:
    def test_posts_failure_message(self, slack):
        _hook().fire(
            il.HookContext(
                event_type="run_failed",
                component_id="c1",
                run_id="r1",
                partition_key="2026-07-30",
                metadata={"status": "failed", "component_name": "Facebook Ads", "error": "boom"},
            )
        )

        assert slack.endpoints == ["chat.postMessage"]
        assert slack.auth() == "Bearer xoxb-t"
        assert slack.json_body()["channel"] == "C123"
        assert slack.json_body()["text"] == "Facebook Ads failed"
        assert _text(slack) == ":x: *Facebook Ads* failed\nRun `r1` · partition `2026-07-30`\n```boom```"

    def test_posts_success_message(self, slack):
        _hook().fire(
            il.HookContext(
                event_type="run_completed",
                component_id="c1",
                run_id="r1",
                metadata={"component_name": "Facebook Ads"},
            )
        )

        assert _text(slack) == ":white_check_mark: *Facebook Ads* completed\nRun `r1`"

    def test_falls_back_to_component_id_without_a_name(self, slack):
        _hook().fire(il.HookContext(event_type="run_failed", component_id="c1"))

        assert _text(slack) == ":x: *c1* failed"
        assert slack.json_body()["text"] == "c1 failed"

    def test_unknown_event_type_still_posts(self, slack):
        # The evaluator only produces the two run outcomes today; an added
        # event type must not silence the notification.
        _hook().fire(il.HookContext(event_type="run_cancelled", component_id="c1"))

        assert _text(slack) == ":bell: *c1* run_cancelled"

    def test_partition_without_run_id(self, slack):
        _hook().fire(il.HookContext(event_type="run_failed", component_id="c1", partition_key="2026-07-30"))

        assert _text(slack) == ":x: *c1* failed\npartition `2026-07-30`"

    def test_posts_backfill_failure_with_its_partitions(self, slack):
        _hook().fire(
            il.HookContext(
                event_type="backfill_failed",
                component_id="c1",
                backfill_id="b1",
                start_key="2026-09-22",
                end_key="2026-09-28",
                metadata={
                    "status": "failed",
                    "component_name": "Facebook Ads",
                    "partitions": 7,
                    "counts": {"success": 5, "failed": 2},
                    "failed_partitions": [["2026-09-26", "rate limited"], ["2026-09-23", None]],
                },
            )
        )

        assert slack.json_body()["text"] == "Facebook Ads backfill failed"
        assert _text(slack) == (
            ":x: *Facebook Ads* backfill failed\n"
            "`2026-09-22` → `2026-09-28` · 5 succeeded, 2 failed\n"
            "• `2026-09-26`: rate limited\n"
            "• `2026-09-23`"
        )

    def test_caps_the_listed_partitions_at_ten(self, slack):
        failed = [[f"2026-09-{day:02d}", "boom"] for day in range(30, 18, -1)]
        _hook().fire(
            il.HookContext(
                event_type="backfill_failed",
                component_id="c1",
                backfill_id="b1",
                start_key="2026-09-19",
                end_key="2026-09-30",
                metadata={"component_name": "Facebook Ads", "counts": {"failed": 12}, "failed_partitions": failed},
            )
        )

        lines = _text(slack).split("\n")
        assert lines[1] == "`2026-09-19` → `2026-09-30` · 12 failed"
        assert len([line for line in lines if line.startswith("•")]) == 10
        assert lines[-1] == "and 2 more"

    def test_posts_backfill_completion_without_errors(self, slack):
        _hook().fire(
            il.HookContext(
                event_type="backfill_completed",
                component_id="c1",
                backfill_id="b1",
                start_key="2026-09-22",
                end_key="2026-09-28",
                metadata={"component_name": "Facebook Ads", "counts": {"success": 7}, "failed_partitions": []},
            )
        )

        assert _text(slack) == (
            ":white_check_mark: *Facebook Ads* backfill completed\n`2026-09-22` → `2026-09-28` · 7 succeeded"
        )

    def test_names_the_organisation_before_the_component(self, slack):
        _hook().fire(
            il.HookContext(
                event_type="run_failed",
                component_id="c1",
                run_id="r1",
                metadata={"organisation_name": "Swarovski", "component_name": "Facebook Ads"},
            )
        )

        assert slack.json_body()["text"] == "Swarovski · Facebook Ads failed"
        assert _text(slack) == ":x: *Swarovski · Facebook Ads* failed\nRun `r1`"

    def test_links_to_the_app_when_the_context_knows_its_page(self, slack):
        _hook().fire(
            il.HookContext(
                event_type="run_failed", component_id="c1", run_id="r1", url="https://app.test/executions/runs/r1"
            )
        )

        blocks = slack.json_body()["blocks"]
        assert [block["type"] for block in blocks] == ["section", "actions"]
        assert blocks[1]["elements"] == [
            {
                "type": "button",
                "text": {"type": "plain_text", "text": "View in interloper"},
                "url": "https://app.test/executions/runs/r1",
            }
        ]

    def test_without_a_url_there_is_no_button(self, slack):
        _hook().fire(il.HookContext(event_type="run_failed", component_id="c1", run_id="r1"))

        assert [block["type"] for block in slack.json_body()["blocks"]] == ["section"]

    def test_request_carries_the_configured_timeout(self, slack):
        _hook(timeout=2.5).fire(il.HookContext(event_type="run_failed", component_id="c1"))

        # Per-request override of the connection client's default.
        assert slack.timeout() == {"connect": 2.5, "read": 2.5, "write": 2.5, "pool": 2.5}

    def test_without_connection_raises(self, slack):
        with pytest.raises(ConfigError, match="without a Slack connection"):
            SlackHook(id="h1", channel="C123").fire(  # ty: ignore[missing-argument]
                il.HookContext(event_type="run_failed", component_id="c1")
            )

        assert slack.requests == []

    def test_slack_rejection_propagates(self, slack):
        # Slack refuses with 200, and the evaluator records the failure on the
        # firing claim — so fire() must raise rather than swallow it.
        slack.error("not_in_channel")

        with pytest.raises(RuntimeError, match="Slack API error: not_in_channel"):
            _hook().fire(il.HookContext(event_type="run_failed", component_id="c1"))

    def test_http_error_propagates(self, slack):
        slack.raw(httpx.Response(500))

        with pytest.raises(httpx.HTTPStatusError):
            _hook().fire(il.HookContext(event_type="run_failed", component_id="c1"))


class TestDefinition:
    def test_declares_a_connection_relation(self):
        assert SlackHook.relations["connection"].target is SlackConnection

    def test_defaults_to_failures_only(self):
        assert _hook().events == ["run_failed"]

    def test_is_a_catalogued_hook(self):
        definition = SlackHook.definition()
        assert (definition.kind, definition.key, definition.name) == ("hook", "slack_hook", "Slack")
        assert definition.relations["connection"].key == "slack_connection"
