"""Tests for ``interloper_slack.api``."""

import httpx
import pytest

from interloper_slack.api import SlackAPIError, apost, post


class TestPost:
    def test_returns_payload_on_ok(self, slack):
        slack.ok(ts="1.2")

        with httpx.Client() as client:
            assert post(client, "chat.postMessage", "xoxb-t")["ts"] == "1.2"

    def test_ok_false_raises_with_slack_error_code(self, slack):
        # Slack rejects with HTTP 200, so the `ok` check is the only signal.
        slack.error("channel_not_found")

        with httpx.Client() as client, pytest.raises(SlackAPIError) as excinfo:
            post(client, "chat.postMessage", "xoxb-t")

        assert excinfo.value.error == "channel_not_found"
        assert excinfo.value.endpoint == "chat.postMessage"
        assert "channel_not_found" in str(excinfo.value)

    def test_ok_false_without_error_key(self, slack):
        slack.raw(httpx.Response(200, json={"ok": False}))

        with httpx.Client() as client, pytest.raises(SlackAPIError) as excinfo:
            post(client, "chat.postMessage", "xoxb-t")

        assert excinfo.value.error == "unknown_error"

    def test_transport_failure_raises(self, slack):
        slack.raw(httpx.Response(500))

        with httpx.Client() as client, pytest.raises(httpx.HTTPStatusError):
            post(client, "chat.postMessage", "xoxb-t")

    def test_addresses_the_endpoint_with_a_bearer_token(self, slack):
        with httpx.Client() as client:
            post(client, "chat.postMessage", "xoxb-secret", json={"channel": "C1"})

        assert str(slack.requests[0].url) == "https://slack.com/api/chat.postMessage"
        assert slack.requests[0].method == "POST"
        assert slack.auth() == "Bearer xoxb-secret"
        assert slack.json_body() == {"channel": "C1"}

    def test_data_is_form_encoded(self, slack):
        # conversations.list and friends take form-encoded bodies, not JSON.
        with httpx.Client() as client:
            post(client, "conversations.list", "xoxb-t", data={"limit": "1000"})

        assert slack.form_body() == {"limit": "1000"}
        assert "application/x-www-form-urlencoded" in slack.requests[0].headers["content-type"]


class TestApost:
    """``apost`` mirrors ``post`` — same arguments, same result, same failures."""

    async def test_returns_payload_on_ok(self, slack):
        slack.ok(ts="1.2")

        async with httpx.AsyncClient() as client:
            assert (await apost(client, "chat.postMessage", "xoxb-t"))["ts"] == "1.2"

    async def test_ok_false_raises_with_slack_error_code(self, slack):
        slack.error("invalid_auth")

        async with httpx.AsyncClient() as client:
            with pytest.raises(SlackAPIError) as excinfo:
                await apost(client, "auth.test", "xoxb-t")

        assert (excinfo.value.endpoint, excinfo.value.error) == ("auth.test", "invalid_auth")

    async def test_transport_failure_raises(self, slack):
        slack.raw(httpx.Response(500))

        async with httpx.AsyncClient() as client:
            with pytest.raises(httpx.HTTPStatusError):
                await apost(client, "auth.test", "xoxb-t")

    async def test_addresses_the_endpoint_with_a_bearer_token(self, slack):
        async with httpx.AsyncClient() as client:
            await apost(client, "conversations.list", "xoxb-secret", data={"limit": "1000"})

        assert str(slack.requests[0].url) == "https://slack.com/api/conversations.list"
        assert slack.requests[0].method == "POST"
        assert slack.auth() == "Bearer xoxb-secret"
        assert slack.form_body() == {"limit": "1000"}
