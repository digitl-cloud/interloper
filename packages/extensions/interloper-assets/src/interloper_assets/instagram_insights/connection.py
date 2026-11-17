from functools import cached_property

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.instagram_insights import constants
from interloper_assets.instagram_insights.api import InstagramInsightsApi


@il.connection(
    name="Instagram Insights",
    icon="skill-icons:instagram",
    tags=["Social"],
    oauth=il.OAuthConfig(
        "facebook",
        scope="instagram_basic,instagram_manage_insights,pages_show_list,pages_read_engagement",
    ),
    maturity="beta",
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

        Returns:
            The client, pinned to ``constants.API_VERSION`` and cached per connection instance.
        """
        return il.AsyncRESTClient(
            f"{constants.BASE_URL}/{constants.API_VERSION}",
            auth=il.HTTPBearerAuth(self.refresh_token),
        )

    @cached_property
    def api(self) -> InstagramInsightsApi:
        """The Instagram Graph API engine every caller shares, built on ``client``.

        Returns:
            The engine, cached per connection instance.
        """
        return InstagramInsightsApi(self.client)

    @il.fetch_field_provider
    async def accounts(self) -> list[dict[str, str]]:
        """List the Instagram Business/Creator accounts reachable by this connection.

        Backs the source's ``account_id`` ``FetchField``. Walks the Facebook Pages
        the token administers and flattens each Page's connected
        ``instagram_business_account`` (Pages without one are skipped). Talks to the
        Graph API over the lightweight ``AsyncRESTClient`` (not the SDK) so it runs
        in the API process.

        Returns:
            The options for the field's dropdown.

        """
        pages = await self.api.rows(
            "/me/accounts",
            fields="instagram_business_account{id,username,name},name",
            limit=100,
        )

        accounts: list[dict[str, str]] = []
        for page in pages:
            account = page.get("instagram_business_account")
            if not account:
                continue
            name = account.get("username") or account.get("name") or page.get("name", account["id"])
            accounts.append({"id": str(account["id"]), "name": name})
        return accounts

    async def check(self) -> bool:
        """Prove the credentials work by running the ``accounts`` lookup.

        Returns:
            True — any credential failure raises out of the lookup.
        """
        await self.accounts()
        return True
