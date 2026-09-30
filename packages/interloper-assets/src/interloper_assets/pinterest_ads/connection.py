import base64
from functools import cached_property

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.pinterest_ads import constants


class PinterestRefreshTokenAuth(il.OAuth2RefreshTokenAuth):
    """Refresh-token auth that also sends the client credentials as HTTP Basic.

    Pinterest's token endpoint authenticates the app with an HTTP Basic
    ``Authorization`` header; credentials in the form body alone are rejected.
    The ``pinterest`` OAuth provider applies the same dialect to its renewal
    grant.
    """

    @property
    def auth_headers(self) -> dict[str, str]:
        """The Basic ``Authorization`` header carried by every token request.

        Returns:
            The headers added to the token request.
        """
        credentials = base64.b64encode(f"{self._client_id}:{self._client_secret}".encode()).decode()
        return {"Authorization": f"Basic {credentials}"}


@il.connection(
    name="Pinterest Ads",
    icon="logos:pinterest",
    tags=["Advertising"],
    oauth=il.OAuthConfig("pinterest", scope="ads:read"),
)
class PinterestAdsConnection(il.RefreshTokenOAuthConnection):
    """Pinterest Ads API connection with OAuth2 refresh token auth."""

    model_config = SettingsConfigDict(env_prefix="pinterest_ads_")

    @cached_property
    def client(self) -> il.AsyncRESTClient:
        """The Pinterest v5 API client every caller shares.

        Returns:
            The authenticated client, cached per connection instance.
        """
        return il.AsyncRESTClient(
            constants.BASE_URL,
            auth=PinterestRefreshTokenAuth(
                base_url=constants.BASE_URL,
                token_endpoint="/oauth/token",
                client_id=self.client_id,
                client_secret=self.client_secret,
                refresh_token=self.refresh_token,
            ),
        )

    @il.fetch_field_provider
    async def accounts(self) -> list[dict[str, str]]:
        """List the ad accounts reachable by this connection.

        Backs the source's ``account_id`` ``FetchField``.

        Returns:
            The options for the field's dropdown.
        """
        paginator = il.JSONCursorPaginator(cursor_path="bookmark", cursor_param="bookmark")
        pages = self.client.paginate(
            "/ad_accounts",
            paginator,
            params={"page_size": constants.PAGE_SIZE},
            data_selector="items",
        )
        return [
            {"id": account["id"], "name": account.get("name") or account["id"]}
            async for page in pages
            for account in page
        ]

    async def check(self) -> bool:
        """Prove the credentials work by running the ``accounts`` lookup.

        Returns:
            True — any credential failure raises out of the lookup.
        """
        await self.accounts()
        return True
