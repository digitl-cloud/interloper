"""Tests for ``interloper_slack.connection``."""

import pytest

from interloper_slack import SlackAPIError, SlackConnection


class TestClient:
    def test_carries_base_url_and_bearer_auth(self, slack):
        SlackConnection(bot_token="xoxb-secret").check()

        assert str(slack.requests[0].url) == "https://slack.com/api/auth.test"
        assert slack.auth() == "Bearer xoxb-secret"

    def test_is_cached_across_calls(self, slack):
        conn = SlackConnection(bot_token="xoxb-t")

        assert conn.client is conn.client


class TestChannels:
    def test_pages_and_sorts(self, slack):
        slack.ok(
            channels=[{"id": "C2", "name": "ops"}, {"id": "C1", "name": "Alerts"}],
            response_metadata={"next_cursor": "page2"},
        ).ok(
            channels=[{"id": "C3", "name": "data"}],
            response_metadata={"next_cursor": ""},
        )

        channels = SlackConnection(bot_token="xoxb-t").channels()

        # Sorted case-insensitively by display name, '#'-prefixed.
        assert channels == [
            {"id": "C1", "name": "#Alerts"},
            {"id": "C3", "name": "#data"},
            {"id": "C2", "name": "#ops"},
        ]
        assert slack.endpoints == ["conversations.list", "conversations.list"]
        assert slack.params(1)["cursor"] == "page2"

    def test_requests_both_visibilities_without_archived(self, slack):
        slack.ok(channels=[])

        SlackConnection(bot_token="xoxb-t").channels()

        params = slack.params()
        assert params["types"] == "public_channel,private_channel"
        assert params["exclude_archived"] == "true"
        assert "cursor" not in params

    def test_missing_cursor_key_terminates(self, slack):
        # A single-page response omits response_metadata entirely.
        slack.ok(channels=[{"id": "C1", "name": "a"}])

        assert SlackConnection(bot_token="xoxb-t").channels() == [{"id": "C1", "name": "#a"}]
        assert len(slack.requests) == 1

    def test_missing_scope_raises(self, slack):
        # paginate only raises for HTTP status, so the selector owns the ok check.
        slack.error("missing_scope")

        with pytest.raises(SlackAPIError, match="missing_scope"):
            SlackConnection(bot_token="xoxb-t").channels()


class TestCheck:
    def test_valid_token(self, slack):
        slack.ok(team="Digitl")

        assert SlackConnection(bot_token="xoxb-t").check() is True
        assert slack.endpoints == ["auth.test"]

    def test_invalid_token_raises(self, slack):
        slack.error("invalid_auth")

        with pytest.raises(SlackAPIError, match="invalid_auth"):
            SlackConnection(bot_token="xoxb-t").check()

    def test_is_checkable(self):
        assert SlackConnection.checkable()
