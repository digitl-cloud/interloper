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
class InstagramInsightsConnection(il.Connection):
    """Instagram Insights API connection with OAuth2 refresh token auth via Facebook Graph API."""

    model_config = SettingsConfigDict(env_prefix="instagram_insights_")

    client_id: str = il.InputField(description="Facebook App ID")
    client_secret: str = il.SecretField(description="Facebook App Secret")
    refresh_token: str = il.SecretField(description="OAuth2 long-lived access token")

    @cached_property
    def client(self) -> il.RESTClient:
        return il.RESTClient(
            constants.BASE_URL,
            auth=il.OAuth2RefreshTokenAuth(
                base_url=constants.BASE_URL,
                token_endpoint="/v21.0/oauth/access_token",
                client_id=self.client_id,
                client_secret=self.client_secret,
                refresh_token=self.refresh_token,
            ),
        )

    @il.fetch_field_provider
    async def accounts(self) -> list[dict[str, str]]:
        """List the Instagram Business/Creator accounts reachable by this connection.

        Backs the source's ``account_id`` ``FetchField``. Walks the Facebook Pages
        the token administers and flattens each Page's connected
        ``instagram_business_account`` (Pages without one are skipped). Talks to the
        Graph API over httpx (not the SDK) so it runs in the API process.
        """
        params = {
            "fields": "instagram_business_account{id,username,name},name",
            "access_token": self.refresh_token,
            "limit": "100",
        }

        accounts: list[dict[str, str]] = []
        url: str | None = f"{constants.BASE_URL}/v21.0/me/accounts"

        async with httpx.AsyncClient(timeout=30) as client:
            while url:
                response = await client.get(url, params=params)
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
                url = data.get("paging", {}).get("next")
                params = {}

        return accounts
