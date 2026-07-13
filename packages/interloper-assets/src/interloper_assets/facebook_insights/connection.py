from functools import cached_property

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
class FacebookInsightsConnection(il.RefreshTokenOAuthConnection):
    """Facebook Insights API connection with OAuth2 refresh token auth.

    Uses the standard ``client_id`` / ``client_secret`` / ``refresh_token``
    trio from ``OAuthConnection``; ``refresh_token`` holds a long-lived access
    token used directly as the bearer.
    """

    model_config = SettingsConfigDict(env_prefix="facebook_insights_")

    @cached_property
    def client(self) -> il.AsyncRESTClient:
        """Async REST client for the Facebook Graph API.

        The ``refresh_token`` field holds a long-lived access token, used
        directly as the bearer.
        """
        return il.AsyncRESTClient(constants.BASE_URL, auth=il.HTTPBearerAuth(self.refresh_token))

    @il.fetch_field_provider
    async def pages(self) -> list[dict[str, str]]:
        """List the Facebook Pages reachable by this connection.

        Backs the source's ``page_id`` ``FetchField``. Talks to the Graph API
        over the lightweight ``AsyncRESTClient`` (not the SDK) so it runs in the
        API process. The ``refresh_token`` field holds a long-lived access token,
        used directly as the bearer for ``GET /me/accounts``.
        """
        pages: list[dict[str, str]] = []
        path: str | None = "/v21.0/me/accounts"
        params: dict[str, str] | None = {"fields": "id,name", "limit": "100"}

        while path:
            response = await self.client.get(path, params=params)
            response.raise_for_status()
            data = response.json()

            for page in data.get("data", []):
                pages.append({
                    "id": page["id"],
                    "name": page.get("name", page["id"]),
                })

            # The "next" link already carries the cursor + fields params.
            path = data.get("paging", {}).get("next")
            params = None

        return pages

    async def check(self) -> bool:
        """Prove the credentials work by running the ``pages`` lookup.

        Returns:
            True — any credential failure raises out of the lookup.
        """
        await self.pages()
        return True
