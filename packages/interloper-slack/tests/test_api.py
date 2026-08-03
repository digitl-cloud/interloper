"""Tests for ``interloper_slack.api``."""

from typing import Any

import httpx
import pytest

from interloper_slack.api import SlackAPIError, unwrap


def _response(payload: dict[str, Any] | None = None, *, status: int = 200, endpoint: str = "chat.postMessage"):
    return httpx.Response(
        status,
        json=payload,
        request=httpx.Request("POST", f"https://slack.com/api/{endpoint}"),
    )


class TestUnwrap:
    def test_returns_payload_on_ok(self):
        assert unwrap(_response({"ok": True, "ts": "1.2"}))["ts"] == "1.2"

    def test_ok_false_raises_with_slack_error_code(self):
        # Slack rejects with HTTP 200, so the `ok` check is the only signal.
        with pytest.raises(SlackAPIError) as excinfo:
            unwrap(_response({"ok": False, "error": "channel_not_found"}))

        assert excinfo.value.error == "channel_not_found"
        assert "channel_not_found" in str(excinfo.value)

    def test_names_the_endpoint_from_the_request(self):
        with pytest.raises(SlackAPIError) as excinfo:
            unwrap(_response({"ok": False, "error": "invalid_auth"}, endpoint="auth.test"))

        assert excinfo.value.endpoint == "auth.test"
        assert "auth.test" in str(excinfo.value)

    def test_ok_false_without_error_key(self):
        with pytest.raises(SlackAPIError) as excinfo:
            unwrap(_response({"ok": False}))

        assert excinfo.value.error == "unknown_error"

    def test_transport_failure_raises(self):
        with pytest.raises(httpx.HTTPStatusError):
            unwrap(_response({"ok": False}, status=500))
