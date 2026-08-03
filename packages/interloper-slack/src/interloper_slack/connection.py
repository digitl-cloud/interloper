"""Slack connection resource holding a bot token."""

from __future__ import annotations

import httpx
from interloper.connection import Connection, connection
from interloper.resource.fields import SecretField, fetch_field_provider
from pydantic_settings import SettingsConfigDict

from interloper_slack.api import apost

#: Slack caps ``conversations.list`` at 1000 per page.
_PAGE_LIMIT = 1000

#: Both channel visibilities a bot can post to once invited.
_CHANNEL_TYPES = "public_channel,private_channel"

#: These calls run inside the API process, serving a form; a page that takes
#: longer than this is not worth making the operator wait for.
_TIMEOUT = 30.0


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

    @fetch_field_provider
    async def channels(self) -> list[dict[str, str]]:
        """List the workspace channels this token can see.

        Backs the hook's ``channel`` ``FetchField``. Pages through
        ``conversations.list`` (archived channels excluded — posting to one
        fails) and labels each option with a leading ``#``, so the picker
        reads the way the channel does in Slack.

        Returns:
            Channel options with ``id`` and a display ``name``.
        """
        results: list[dict[str, str]] = []
        cursor: str | None = None
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            while True:
                params: dict[str, str] = {
                    "types": _CHANNEL_TYPES,
                    "limit": str(_PAGE_LIMIT),
                    "exclude_archived": "true",
                }
                if cursor:
                    params["cursor"] = cursor
                payload = await apost(client, "conversations.list", self.bot_token, data=params)
                results.extend(
                    {"id": channel["id"], "name": f"#{channel['name']}"} for channel in payload.get("channels", [])
                )
                cursor = payload.get("response_metadata", {}).get("next_cursor") or None
                if not cursor:
                    break
        return sorted(results, key=lambda c: c["name"].lower())

    async def check(self) -> bool:
        """Prove the token works via ``auth.test``.

        ``auth.test`` needs no scope beyond a valid token, so it isolates a
        bad credential from a missing ``channels:read`` grant.

        Returns:
            True — an invalid token raises out of the call.
        """
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            await apost(client, "auth.test", self.bot_token)
        return True
