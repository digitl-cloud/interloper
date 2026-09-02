"""Slack connection resource holding a bot token."""

from __future__ import annotations

from functools import cached_property
from typing import Any

import httpx
from interloper.connection import Connection, connection
from interloper.resource.fields import SecretField, fetch_field_provider
from interloper.rest import HTTPBearerAuth, JSONCursorPaginator, RESTClient
from pydantic_settings import SettingsConfigDict

API_BASE = "https://slack.com/api"

#: Slack caps ``conversations.list`` at 1000 per page.
_PAGE_LIMIT = 1000

#: Both channel visibilities a bot can post to once invited.
_CHANNEL_TYPES = "public_channel,private_channel"

#: Every call here serves an operator waiting on a form or a firing hook;
#: neither is worth blocking on longer than this.
_TIMEOUT = 30.0


def _channels(response: httpx.Response) -> list[dict[str, Any]]:
    """Select one page of channels, checking Slack's in-body ``ok`` flag.

    ``paginate`` only raises for HTTP status, so the selector is where a
    rejected page surfaces instead of a confusing miss on the ``channels``
    key — Slack answers a refusal with 200 and ``ok: false``.

    Args:
        response: One page of the ``conversations.list`` response.

    Returns:
        The page's raw channel objects.

    Raises:
        RuntimeError: If Slack rejected the page (``ok: false``).
    """
    response.raise_for_status()
    body = response.json()
    if not body.get("ok"):
        raise RuntimeError(f"Slack API error: {body.get('error')}")
    return body.get("channels", [])


@connection(
    key="slack_connection",
    name="Slack",
    icon="devicon:slack",
    tags=["Communication"],
)
class SlackConnection(Connection):
    """Connection resource holding a Slack bot token.

    The token is the whole credential, so one connection serves every hook in
    the workspace and each hook picks its own channel. Create a Slack app,
    give its bot the ``chat:write`` scope (plus ``channels:read`` /
    ``groups:read`` for the channel picker), install it, and paste the
    ``xoxb-`` token.
    """

    model_config = SettingsConfigDict(env_prefix="slack_")

    bot_token: str = SecretField(
        label="Bot token",
        description="Slack bot user OAuth token (xoxb-…)",
        info=(
            "From your Slack app's OAuth & Permissions page. Needs the chat:write scope, "
            "plus channels:read and groups:read for the channel picker."
        ),
    )

    @cached_property
    def client(self) -> RESTClient:
        """The Slack Web API client every caller shares.

        Sync, unlike most connections' clients: the consumers are a hook
        firing (sync by contract) and a form lookup, neither of which has
        independent requests to overlap. The API process runs sync providers
        in a thread, so this does not block its event loop.

        Returns:
            The bearer-authenticated client, cached per connection instance.
        """
        return RESTClient(API_BASE, auth=HTTPBearerAuth(self.bot_token), timeout=_TIMEOUT)

    @fetch_field_provider
    def channels(self) -> list[dict[str, str]]:
        """List the workspace channels this token can see.

        Backs the hook's ``channel`` ``FetchField``. Archived channels are
        excluded — posting to one fails — and each option is labelled with a
        leading ``#`` so the picker reads the way the channel does in Slack.

        Returns:
            Channel options with ``id`` and a display ``name``.
        """
        pages = self.client.paginate(
            "/conversations.list",
            JSONCursorPaginator(cursor_path="response_metadata.next_cursor", cursor_param="cursor"),
            params={
                "types": _CHANNEL_TYPES,
                "limit": str(_PAGE_LIMIT),
                "exclude_archived": "true",
            },
            data_selector=_channels,
        )
        results = [{"id": channel["id"], "name": f"#{channel['name']}"} for page in pages for channel in page]
        return sorted(results, key=lambda c: c["name"].lower())

    def check(self) -> bool:
        """Prove the token works via ``auth.test``.

        ``auth.test`` needs no scope beyond a valid token, so it isolates a
        bad credential from a missing ``channels:read`` grant.

        Returns:
            True — an invalid token raises instead.

        Raises:
            RuntimeError: If Slack rejects the token.
        """
        response = self.client.post("/auth.test")
        response.raise_for_status()
        body = response.json()
        if not body.get("ok"):
            raise RuntimeError(f"Slack API error: {body.get('error')}")
        return True
