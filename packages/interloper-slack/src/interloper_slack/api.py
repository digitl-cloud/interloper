"""Slack Web API access: one call shape, sync and async.

Slack answers a rejected call with HTTP 200 and ``{"ok": false, "error":
"..."}``, so ``raise_for_status()`` alone lets failures pass silently. Every
call in this package goes through :func:`post` / :func:`apost` so the ``ok``
check happens exactly once.

The two are the same function twice, once per colour: same argument order,
same keywords, same return. The client is always the caller's — it knows the
timeout and how many calls should share a connection — and the verb is always
POST, which every Slack method accepts, so there is no per-endpoint verb to
remember.

Whether the body is JSON or form-encoded is Slack's choice per method, not
ours: ``chat.postMessage`` takes ``application/json``, while
``conversations.list`` takes ``application/x-www-form-urlencoded``. The
``json`` / ``data`` split mirrors httpx's own, so each call passes whichever
its endpoint documents.
"""

from __future__ import annotations

from typing import Any

import httpx
from interloper.errors import InterloperError

API_BASE = "https://slack.com/api"


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


def _headers(token: str) -> dict[str, str]:
    """Bearer-auth headers for a Slack call.

    Returns:
        The request headers.
    """
    return {"Authorization": f"Bearer {token}"}


def _unwrap(endpoint: str, response: httpx.Response) -> dict[str, Any]:
    """Raise on transport failure and on ``ok: false``, else return the payload.

    Returns:
        The decoded response body.

    Raises:
        SlackAPIError: If Slack rejected the call.
    """
    response.raise_for_status()
    payload: dict[str, Any] = response.json()
    if not payload.get("ok"):
        raise SlackAPIError(endpoint, str(payload.get("error", "unknown_error")))
    return payload


def post(
    client: httpx.Client,
    endpoint: str,
    token: str,
    *,
    json: dict[str, Any] | None = None,
    data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Call a Slack Web API *endpoint* (e.g. ``chat.postMessage``) and unwrap it.

    Returns:
        The decoded response body.
    """
    response = client.post(f"{API_BASE}/{endpoint}", json=json, data=data, headers=_headers(token))
    return _unwrap(endpoint, response)


async def apost(
    client: httpx.AsyncClient,
    endpoint: str,
    token: str,
    *,
    json: dict[str, Any] | None = None,
    data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Call a Slack Web API *endpoint* on an async client and unwrap it.

    Returns:
        The decoded response body.
    """
    response = await client.post(f"{API_BASE}/{endpoint}", json=json, data=data, headers=_headers(token))
    return _unwrap(endpoint, response)
