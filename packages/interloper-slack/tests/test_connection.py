"""Tests for ``interloper_slack.connection``."""

import pytest

from interloper_slack import SlackAPIError, SlackConnection


class TestChannels:
    async def test_pages_and_sorts(self, slack):
        slack.ok(
            channels=[{"id": "C2", "name": "ops"}, {"id": "C1", "name": "Alerts"}],
            response_metadata={"next_cursor": "page2"},
        ).ok(
            channels=[{"id": "C3", "name": "data"}],
            response_metadata={"next_cursor": ""},
        )

        channels = await SlackConnection(bot_token="xoxb-t").channels()

        # Sorted case-insensitively by display name, '#'-prefixed.
        assert channels == [
            {"id": "C1", "name": "#Alerts"},
            {"id": "C3", "name": "#data"},
            {"id": "C2", "name": "#ops"},
        ]
        assert slack.endpoints == ["conversations.list", "conversations.list"]
        assert slack.form_body(1)["cursor"] == "page2"

    async def test_requests_both_visibilities_without_archived(self, slack):
        slack.ok(channels=[])

        await SlackConnection(bot_token="xoxb-t").channels()

        body = slack.form_body()
        assert body["types"] == "public_channel,private_channel"
        assert body["exclude_archived"] == "true"
        assert "cursor" not in body

    async def test_missing_cursor_key_terminates(self, slack):
        # A single-page response omits response_metadata entirely.
        slack.ok(channels=[{"id": "C1", "name": "a"}])

        assert await SlackConnection(bot_token="xoxb-t").channels() == [{"id": "C1", "name": "#a"}]
        assert len(slack.requests) == 1

    async def test_missing_scope_raises(self, slack):
        slack.error("missing_scope")

        with pytest.raises(SlackAPIError):
            await SlackConnection(bot_token="xoxb-t").channels()


class TestCheck:
    async def test_valid_token(self, slack):
        slack.ok(team="Digitl")

        assert await SlackConnection(bot_token="xoxb-t").check() is True
        assert slack.endpoints == ["auth.test"]
        assert slack.auth() == "Bearer xoxb-t"

    async def test_invalid_token_raises(self, slack):
        slack.error("invalid_auth")

        with pytest.raises(SlackAPIError):
            await SlackConnection(bot_token="xoxb-t").check()

    def test_is_checkable(self):
        assert SlackConnection.checkable()
