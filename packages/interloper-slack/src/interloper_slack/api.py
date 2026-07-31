"""The one Slack Web API call shape: POST/GET JSON, then check ``ok``.

Slack answers a rejected call with HTTP 200 and ``{"ok": false, "error":
"..."}``, so ``raise_for_status()`` alone lets failures pass silently. Every
call in this package goes through :func:`call` / :func:`acall` so the ``ok``
check happens exactly once.
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

    def __init__(self, method: str, error: str) -> None:
        """Initialize with the API method and Slack's error code."""
        super().__init__(f"Slack API '{method}' failed: {error}")
        self.method = method
        self.error = error


def _unwrap(method: str, response: httpx.Response) -> dict[str, Any]:
    """Raise on transport and on ``ok: false``, else return the payload.

    Returns:
        The decoded response body.

    Raises:
        SlackAPIError: If Slack rejected the call.
    """
    response.raise_for_status()
    payload: dict[str, Any] = response.json()
    if not payload.get("ok"):
        raise SlackAPIError(method, str(payload.get("error", "unknown_error")))
    return payload


def call(method: str, token: str, *, json: dict[str, Any] | None = None, timeout: float = 10.0) -> dict[str, Any]:
    """POST to a Slack Web API *method* and unwrap the response.

    Returns:
        The decoded response body.
    """
    response = httpx.post(
        f"{API_BASE}/{method}",
        json=json or {},
        headers={"Authorization": f"Bearer {token}"},
        timeout=timeout,
    )
    return _unwrap(method, response)


async def acall(
    client: httpx.AsyncClient,
    method: str,
    token: str,
    *,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """GET a Slack Web API *method* on an existing client and unwrap the response.

    The async form serves the fetch providers and ``check()``, which run
    inside the API process and may page through several requests on one
    client.

    Returns:
        The decoded response body.
    """
    response = await client.get(
        f"{API_BASE}/{method}",
        params=params,
        headers={"Authorization": f"Bearer {token}"},
    )
    return _unwrap(method, response)
