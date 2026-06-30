from functools import cached_property

import httpx
import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.pinterest_ads import constants


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
        """Async REST client for the Pinterest v5 API.

        Note: this client's ``OAuth2RefreshTokenAuth`` sends the client
        credentials in the request body, whereas Pinterest's token endpoint
        actually requires HTTP **Basic** auth (see ``_get_access_token``, which
        the ``accounts`` provider relies on). Reconcile this Basic-vs-body
        discrepancy when the source's assets are implemented and start using
        this client.
        """
        return il.AsyncRESTClient(
            constants.BASE_URL,
            auth=il.OAuth2RefreshTokenAuth(
                base_url=constants.BASE_URL,
                token_endpoint="/oauth/token",
                client_id=self.client_id,
                client_secret=self.client_secret,
                refresh_token=self.refresh_token,
            ),
        )

    async def _get_access_token(self) -> str:
        """Exchange the refresh token for an access token using HTTP Basic auth."""
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                f"{constants.BASE_URL}/oauth/token",
                auth=(self.client_id, self.client_secret),
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": self.refresh_token,
                },
            )
            response.raise_for_status()
            return response.json()["access_token"]

    @il.fetch_field_provider
    async def accounts(self) -> list[dict[str, str]]:
        """List the ad accounts reachable by this connection.

        Backs the source's ``account_id`` ``FetchField``. Talks to the v5 API
        over httpx (not the SDK) so it runs in the API process.
        """
        token = await self._get_access_token()
        headers = {"Authorization": f"Bearer {token}"}

        accounts: list[dict[str, str]] = []
        bookmark: str | None = None

        async with httpx.AsyncClient(timeout=30, headers=headers) as client:
            while True:
                params: dict[str, str] = {"page_size": "100"}
                if bookmark:
                    params["bookmark"] = bookmark

                response = await client.get(f"{constants.BASE_URL}/ad_accounts", params=params)
                response.raise_for_status()
                data = response.json()

                for account in data.get("items", []):
                    accounts.append({
                        "id": account["id"],
                        "name": account.get("name", account["id"]),
                    })

                bookmark = data.get("bookmark")
                if not bookmark:
                    break

        return accounts
