from functools import cached_property

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.criteo import constants
from interloper_assets.criteo.api import CriteoApi


@il.connection(
    name="Criteo",
    icon="icon:criteo",
    tags=["Advertising"],
    oauth=il.OAuthConfig("criteo"),
)
class CriteoConnection(il.RefreshTokenOAuthConnection):
    """Criteo API connection with OAuth2 refresh token auth."""

    model_config = SettingsConfigDict(env_prefix="criteo_")

    @cached_property
    def client(self) -> il.AsyncRESTClient:
        """Async REST client with OAuth2 refresh-token auth.

        The token exchange is async-native (yielded into this client's flow), so
        no blocking refresh runs on the event loop — no up-front token fetch needed.

        Returns:
            The authenticated client, cached per connection instance.
        """
        return il.AsyncRESTClient(
            constants.BASE_URL,
            auth=il.OAuth2RefreshTokenAuth(
                constants.BASE_URL, self.client_id, self.client_secret, refresh_token=self.refresh_token
            ),
            retry=il.HttpRetryPolicy(),
        )

    @cached_property
    def api(self) -> CriteoApi:
        """The Criteo API engine every caller shares, built on ``client``.

        Returns:
            The engine, cached per connection instance.
        """
        return CriteoApi(self.client)

    @il.fetch_field_provider
    async def advertisers(self) -> list[dict[str, str]]:
        """Fetch the Criteo advertisers accessible by this connection, sorted by name.

        Returns:
            The options for the field's dropdown.

        """
        advertisers = await self.api.get("/advertisers/me")
        results = [
            {"id": advertiser["id"], "name": advertiser.get("advertiserName") or advertiser["id"]}
            for advertiser in advertisers
        ]
        return sorted(results, key=lambda advertiser: advertiser["name"].lower())

    async def check(self) -> bool:
        """Prove the credentials work by running the ``advertisers`` lookup.

        Returns:
            True — any credential failure raises out of the lookup.
        """
        await self.advertisers()
        return True
