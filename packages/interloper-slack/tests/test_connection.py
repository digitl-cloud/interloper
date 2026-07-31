"""Tests for ``interloper_slack.connection``."""

from collections.abc import Callable

import httpx
import pytest

from interloper_slack import SlackAPIError, SlackConnection


def _mock_slack(monkeypatch: pytest.MonkeyPatch, handler: Callable[[httpx.Request], httpx.Response]) -> list[httpx.URL]:
    """Route every AsyncClient through *handler*, recording the URLs requested."""
    seen: list[httpx.URL] = []
    real_client = httpx.AsyncClient

    def recording(request: httpx.Request) -> httpx.Response:
        seen.append(request.url)
        return handler(request)

    def fake_client(**kwargs) -> httpx.AsyncClient:
        # Bind the real class before patching, or constructing one here
        # re-enters this factory.
        return real_client(transport=httpx.MockTransport(recording))

    monkeypatch.setattr(httpx, "AsyncClient", fake_client)
    return seen


class TestChannels:
    async def test_pages_and_sorts(self, monkeypatch: pytest.MonkeyPatch):
        pages = [
            {
                "ok": True,
                "channels": [{"id": "C2", "name": "ops"}, {"id": "C1", "name": "Alerts"}],
                "response_metadata": {"next_cursor": "page2"},
            },
            {"ok": True, "channels": [{"id": "C3", "name": "data"}], "response_metadata": {"next_cursor": ""}},
        ]
        calls = iter(pages)
        seen = _mock_slack(monkeypatch, lambda request: httpx.Response(200, json=next(calls)))

        channels = await SlackConnection(bot_token="xoxb-t").channels()

        # Sorted case-insensitively by display name, '#'-prefixed.
        assert channels == [
            {"id": "C1", "name": "#Alerts"},
            {"id": "C3", "name": "#data"},
            {"id": "C2", "name": "#ops"},
        ]
        assert len(seen) == 2
        assert "cursor=page2" in str(seen[1])

    async def test_requests_both_visibilities_without_archived(self, monkeypatch: pytest.MonkeyPatch):
        seen = _mock_slack(monkeypatch, lambda request: httpx.Response(200, json={"ok": True, "channels": []}))

        await SlackConnection(bot_token="xoxb-t").channels()

        url = str(seen[0])
        assert "public_channel" in url and "private_channel" in url
        assert "exclude_archived=true" in url

    async def test_missing_cursor_key_terminates(self, monkeypatch: pytest.MonkeyPatch):
        # A single-page response omits response_metadata entirely.
        seen = _mock_slack(
            monkeypatch,
            lambda request: httpx.Response(200, json={"ok": True, "channels": [{"id": "C1", "name": "a"}]}),
        )

        assert await SlackConnection(bot_token="xoxb-t").channels() == [{"id": "C1", "name": "#a"}]
        assert len(seen) == 1

    async def test_missing_scope_raises(self, monkeypatch: pytest.MonkeyPatch):
        _mock_slack(monkeypatch, lambda request: httpx.Response(200, json={"ok": False, "error": "missing_scope"}))

        with pytest.raises(SlackAPIError):
            await SlackConnection(bot_token="xoxb-t").channels()


class TestCheck:
    async def test_valid_token(self, monkeypatch: pytest.MonkeyPatch):
        seen = _mock_slack(monkeypatch, lambda request: httpx.Response(200, json={"ok": True, "team": "Digitl"}))

        assert await SlackConnection(bot_token="xoxb-t").check() is True
        assert seen[0].path.endswith("/auth.test")

    async def test_invalid_token_raises(self, monkeypatch: pytest.MonkeyPatch):
        _mock_slack(monkeypatch, lambda request: httpx.Response(200, json={"ok": False, "error": "invalid_auth"}))

        with pytest.raises(SlackAPIError):
            await SlackConnection(bot_token="xoxb-t").check()

    def test_is_checkable(self):
        assert SlackConnection.checkable()
