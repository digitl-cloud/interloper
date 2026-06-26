from functools import cached_property

import httpx
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
    def client(self) -> il.RESTClient:
        client = il.RESTClient(
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

    async def _get_access_token(self) -> str:
        """Exchange the refresh token for an access token."""
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                f"{constants.AUTH_BASE_URL}/oauth/v2/accessToken",
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": self.refresh_token,
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                },
            )
            response.raise_for_status()
            return response.json()["access_token"]

    @il.fetch_field_provider
    async def accounts(self) -> list[dict[str, str]]:
        """List the ad accounts reachable by this connection.

        Backs the source's ``account_id`` ``FetchField``. Talks to the
        Marketing API over httpx (not a SDK) so it runs in the API process,
        reusing the same base URL, version and Rest.li headers as the assets.
        """
        token = await self._get_access_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "LinkedIn-Version": constants.LINKEDIN_VERSION,
            "X-Restli-Protocol-Version": constants.RESTLI_PROTOCOL_VERSION,
        }

        accounts: list[dict[str, str]] = []
        start = 0
        count = 100

        async with httpx.AsyncClient(timeout=30, headers=headers) as client:
            while True:
                response = await client.get(
                    f"{constants.BASE_URL}/adAccounts",
                    params={"q": "search", "start": start, "count": count},
                )
                response.raise_for_status()
                data = response.json()

                elements = data.get("elements", [])
                for account in elements:
                    account_id = str(account["id"])
                    accounts.append({
                        "id": account_id,
                        "name": account.get("name", account_id),
                    })

                if len(elements) < count:
                    break
                start += count

        return accounts
