from functools import cached_property

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.linkedin_ads import constants


@il.connection(
    name="LinkedIn Ads",
    icon="devicon:linkedin",
    tags=["Advertising"],
    oauth=il.OAuthConfig("linkedin", scope="r_ads,r_ads_reporting"),
)
class LinkedinAdsConnection(il.OAuthConnection):
    """LinkedIn Ads API connection with OAuth2 refresh token auth."""

    model_config = SettingsConfigDict(env_prefix="linkedin_ads_")

    @cached_property
    def client(self) -> il.AsyncRESTClient:
        client = il.AsyncRESTClient(
            constants.BASE_URL,
            auth=il.OAuth2RefreshTokenAuth(
                base_url=constants.AUTH_BASE_URL,
                token_endpoint="/oauth/v2/accessToken",
                client_id=self.client_id,
                client_secret=self.client_secret,
                refresh_token=self.refresh_token,
            ),
        )
        client.headers.update(
            {
                "LinkedIn-Version": constants.LINKEDIN_VERSION,
                "X-Restli-Protocol-Version": constants.RESTLI_PROTOCOL_VERSION,
                "Accept-Encoding": "gzip, deflate, br",
            }
        )
        return client

    @il.fetch_field_provider
    async def accounts(self) -> list[dict[str, str]]:
        """List the ad accounts reachable by this connection.

        Backs the source's ``account_id`` ``FetchField``. Reuses ``self.client``
        (same base URL, version and Rest.li headers as the assets) and paginates
        the Marketing API over offset/count.
        """
        paginator = il.OffsetPaginator(
            limit=100,
            offset_param="start",
            limit_param="count",
            data_selector="elements",
        )
        pages = self.client.paginate(
            "/adAccounts",
            paginator,
            params={"q": "search"},
            data_selector="elements",
        )
        return [
            {"id": str(account["id"]), "name": account.get("name", str(account["id"]))}
            async for page in pages
            for account in page
        ]
