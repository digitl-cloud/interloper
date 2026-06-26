from functools import cached_property

import httpx
import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.facebook_insights import constants


@il.connection(
    name="Facebook Insights",
    icon="logos:facebook",
    tags=["Social"],
    oauth=il.OAuthConfig(
        "facebook",
        scope="pages_show_list,pages_read_engagement,pages_read_user_content,read_insights",
    ),
)
class FacebookInsightsConnection(il.Connection):
    """Facebook Insights API connection with OAuth2 refresh token auth."""

    model_config = SettingsConfigDict(env_prefix="facebook_insights_")

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
    async def pages(self) -> list[dict[str, str]]:
        """List the Facebook Pages reachable by this connection.

        Backs the source's ``page_id`` ``FetchField``. Talks to the Graph API
        over httpx (not the SDK) so it runs in the API process. The
        ``refresh_token`` field holds a long-lived access token, used directly
        as the bearer for ``GET /me/accounts``.
        """
        headers = {"Authorization": f"Bearer {self.refresh_token}"}

        pages: list[dict[str, str]] = []
        url: str | None = f"{constants.BASE_URL}/v21.0/me/accounts"
        params: dict[str, str] | None = {"fields": "id,name", "limit": "100"}

        async with httpx.AsyncClient(timeout=30, headers=headers) as client:
            while url:
                response = await client.get(url, params=params)
                response.raise_for_status()
                data = response.json()

                for page in data.get("data", []):
                    pages.append({
                        "id": page["id"],
                        "name": page.get("name", page["id"]),
                    })

                # The "next" link already carries the cursor + fields params.
                url = data.get("paging", {}).get("next")
                params = None

        return pages
