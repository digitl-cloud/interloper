from functools import cached_property

import interloper as il
from interloper.errors import ConnectionCheckError
from pydantic_settings import SettingsConfigDict

from interloper_assets.linkedin.api import LinkedinApi, LinkedinApiError
from interloper_assets.linkedin_ads import constants


@il.connection(
    name="LinkedIn Ads",
    icon="devicon:linkedin",
    tags=["Advertising"],
    oauth=il.OAuthConfig("linkedin", scope="r_ads,r_ads_reporting"),
    maturity="beta",
)
class LinkedinAdsConnection(il.RefreshTokenOAuthConnection):
    """LinkedIn Ads API connection with OAuth2 refresh token auth."""

    model_config = SettingsConfigDict(env_prefix="linkedin_ads_")

    @cached_property
    def client(self) -> il.AsyncRESTClient:
        """The LinkedIn Ads API client every caller shares.

        Returns:
            The authenticated client, cached per connection instance.
        """
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

    @cached_property
    def api(self) -> LinkedinApi:
        """The LinkedIn REST API engine every caller shares, built on ``client``.

        Returns:
            The engine, cached per connection instance.
        """
        return LinkedinApi(self.client)

    @il.fetch_field_provider
    async def accounts(self) -> list[dict[str, str]]:
        """List the ad accounts reachable by this connection.

        Backs the source's ``account_id`` ``FetchField``, through the same
        engine as the assets.

        Returns:
            The options for the field's dropdown.

        Raises:
            ConnectionCheckError: If LinkedIn refuses the lookup, carrying LinkedIn's code and message, which name
                no credential.
        """
        try:
            accounts = await self.api.cursor_rows("/adAccounts", q="search")
        except LinkedinApiError as error:
            raise ConnectionCheckError(str(error)) from error
        return [{"id": str(account["id"]), "name": account.get("name", str(account["id"]))} for account in accounts]

    async def check(self) -> bool:
        """Prove the credentials work by running the ``accounts`` lookup.

        Returns:
            True — any credential failure raises out of the lookup, LinkedIn's refusal as a `ConnectionCheckError`.
        """
        await self.accounts()
        return True
