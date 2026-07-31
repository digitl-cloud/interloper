"""Tests for ``interloper_slack.api``."""

import httpx
import pytest

from interloper_slack.api import SlackAPIError, call


def _responder(monkeypatch: pytest.MonkeyPatch, response: httpx.Response):
    def fake_post(url: str, **kwargs) -> httpx.Response:
        response.request = httpx.Request("POST", url)
        return response

    monkeypatch.setattr(httpx, "post", fake_post)


class TestCall:
    def test_returns_payload_on_ok(self, monkeypatch: pytest.MonkeyPatch):
        _responder(monkeypatch, httpx.Response(200, json={"ok": True, "ts": "1.2"}))
        assert call("chat.postMessage", "xoxb-t")["ts"] == "1.2"

    def test_ok_false_raises_with_slack_error_code(self, monkeypatch: pytest.MonkeyPatch):
        # Slack rejects with HTTP 200, so the `ok` check is the only signal.
        _responder(monkeypatch, httpx.Response(200, json={"ok": False, "error": "channel_not_found"}))

        with pytest.raises(SlackAPIError) as excinfo:
            call("chat.postMessage", "xoxb-t")

        assert excinfo.value.error == "channel_not_found"
        assert excinfo.value.method == "chat.postMessage"
        assert "channel_not_found" in str(excinfo.value)

    def test_ok_false_without_error_key(self, monkeypatch: pytest.MonkeyPatch):
        _responder(monkeypatch, httpx.Response(200, json={"ok": False}))

        with pytest.raises(SlackAPIError) as excinfo:
            call("chat.postMessage", "xoxb-t")

        assert excinfo.value.error == "unknown_error"

    def test_transport_error_raises(self, monkeypatch: pytest.MonkeyPatch):
        _responder(monkeypatch, httpx.Response(500))

        with pytest.raises(httpx.HTTPStatusError):
            call("chat.postMessage", "xoxb-t")

    def test_sends_bearer_token(self, monkeypatch: pytest.MonkeyPatch):
        seen: dict[str, object] = {}

        def fake_post(url: str, **kwargs) -> httpx.Response:
            seen.update(kwargs)
            seen["url"] = url
            return httpx.Response(200, json={"ok": True}, request=httpx.Request("POST", url))

        monkeypatch.setattr(httpx, "post", fake_post)
        call("chat.postMessage", "xoxb-secret", json={"channel": "C1"}, timeout=3.0)

        assert seen["url"] == "https://slack.com/api/chat.postMessage"
        assert seen["headers"] == {"Authorization": "Bearer xoxb-secret"}
        assert seen["json"] == {"channel": "C1"}
        assert seen["timeout"] == 3.0
