from functools import cached_property

import httpx
import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.instagram_insights import constants


@il.connection(
    name="Instagram Insights",
    icon="skill-icons:instagram",
    tags=["Social"],
    oauth=il.OAuthConfig(
        "facebook",
        scope="instagram_basic,instagram_manage_insights,pages_show_list,pages_read_engagement",
    ),
)
class InstagramInsightsConnection(il.RefreshTokenOAuthConnection):
    """Instagram Insights API connection with OAuth2 refresh token auth via Facebook Graph API.

    Uses the standard ``client_id`` / ``client_secret`` / ``refresh_token``
    trio from ``OAuthConnection``; ``refresh_token`` holds a long-lived access
    token.
    """

    model_config = SettingsConfigDict(env_prefix="instagram_insights_")

    @cached_property
    def client(self) -> il.AsyncRESTClient:
        """Async REST client for the Facebook Graph API.

        The ``refresh_token`` field holds a long-lived access token, used
        directly as the bearer.
        """
        return il.AsyncRESTClient(constants.BASE_URL, auth=il.HTTPBearerAuth(self.refresh_token))

    @il.fetch_field_provider
    async def accounts(self) -> list[dict[str, str]]:
        """List the Instagram Business/Creator accounts reachable by this connection.

        Backs the source's ``account_id`` ``FetchField``. Walks the Facebook Pages
        the token administers and flattens each Page's connected
        ``instagram_business_account`` (Pages without one are skipped). Talks to the
        Graph API over the lightweight ``AsyncRESTClient`` (not the SDK) so it runs
        in the API process.
        """
        params = {
            "fields": "instagram_business_account{id,username,name},name",
            "access_token": self.refresh_token,
            "limit": "100",
        }

        accounts: list[dict[str, str]] = []
        path: str | None = "/v21.0/me/accounts"

        while path:
            response = await self.client.get(path, params=params)
            response.raise_for_status()
            data = response.json()

            for page in data.get("data", []):
                ig_account = page.get("instagram_business_account")
                if not ig_account:
                    continue
                name = ig_account.get("username") or ig_account.get("name") or page.get("name", ig_account["id"])
                accounts.append({
                    "id": str(ig_account["id"]),
                    "name": name,
                })

            # Cursor pagination: follow the absolute `next` URL (already carries params).
            path = data.get("paging", {}).get("next")
            params = {}

        return accounts

    async def check(self) -> bool:
        """Prove the credentials work by running the ``accounts`` lookup.

        Returns:
            True — any credential failure raises out of the lookup.
        """
        await self.accounts()
        return True

    async def renew(self) -> il.Renewal:
        """Exchange the long-lived access token for a fresh one.

        Facebook has no refresh-token grant (the ``refresh_token`` field
        holds a long-lived access token): renewal is the
        ``fb_exchange_token`` grant, which issues a new long-lived token
        as long as the current one is still valid.

        Returns:
            The fresh token (into ``refresh_token``) and its validity when
            reported.
        """
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(
                f"{constants.BASE_URL}/oauth/access_token",
                params={
                    "grant_type": "fb_exchange_token",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "fb_exchange_token": self.refresh_token,
                },
            )
            response.raise_for_status()
            data = response.json()
        return il.Renewal(fields={"refresh_token": data["access_token"]}, expires_in=data.get("expires_in"))
