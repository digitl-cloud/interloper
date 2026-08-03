"""Slack's response contract: a call can fail with HTTP 200.

Slack answers a rejected call with ``200`` and ``{"ok": false, "error":
"..."}``, so ``raise_for_status()`` alone lets failures pass silently. Every
response in this package goes through :func:`unwrap`, which is the only thing
Slack needs beyond what :class:`~interloper.rest.RESTClient` already provides.
"""

from __future__ import annotations

from typing import Any

import httpx
from interloper.errors import InterloperError


class SlackAPIError(InterloperError):
    """A Slack Web API call returned ``ok: false``.

    Carries Slack's own ``error`` code (e.g. ``channel_not_found``,
    ``invalid_auth``, ``not_in_channel``) — those codes are the actionable
    part of a failure, so they stay verbatim in the message.
    """

    def __init__(self, endpoint: str, error: str) -> None:
        """Initialize with the Slack endpoint and its error code."""
        super().__init__(f"Slack API '{endpoint}' failed: {error}")
        self.endpoint = endpoint
        self.error = error


def unwrap(response: httpx.Response) -> dict[str, Any]:
    """Raise on transport failure and on ``ok: false``, else return the payload.

    Returns:
        The decoded response body.

    Raises:
        SlackAPIError: If Slack rejected the call.
    """
    response.raise_for_status()
    payload: dict[str, Any] = response.json()
    if not payload.get("ok"):
        endpoint = response.request.url.path.removeprefix("/api/")
        raise SlackAPIError(endpoint, str(payload.get("error", "unknown_error")))
    return payload
