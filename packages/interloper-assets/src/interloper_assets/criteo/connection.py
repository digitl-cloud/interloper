from functools import cached_property
from typing import Any

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.criteo import constants


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
        """
        return il.AsyncRESTClient(
            constants.BASE_URL,
            auth=il.OAuth2RefreshTokenAuth(
                constants.BASE_URL, self.client_id, self.client_secret, refresh_token=self.refresh_token
            ),
        )

    @il.fetch_field_provider
    async def advertisers(self) -> list[dict[str, str]]:
        """Fetch the Criteo advertisers accessible by this connection, sorted by name.

        Returns:
            The options for the field's dropdown.

        """
        response = await self.client.get(f"/{constants.API_VERSION}/advertisers/me")
        response.raise_for_status()
        advertisers = response.json().get("data", [])

        results = [
            {"id": a["id"], "name": a.get("attributes", {}).get("advertiserName") or a["id"]} for a in advertisers
        ]
        return sorted(results, key=lambda a: a["name"].lower())

    async def advertiser(self, advertiser_id: str) -> dict[str, Any]:
        """Fetch an advertiser's name, currency and time zone.

        Of Criteo's advertiser endpoints, only the Marketplace Performance
        Outcomes one carries the currency and the time zone; ``/advertisers/me``
        names the advertiser only.

        Args:
            advertiser_id: The advertiser to look up.

        Returns:
            The advertiser as Criteo sends it: ``id``, ``advertiserName``,
            ``currencyName`` (ISO 4217) and ``timeZoneId`` (IANA, or ``None``).
        """
        response = await self.client.get(
            f"/{constants.API_VERSION}/marketing-solutions/marketplace-performance-outcomes/advertisers/{advertiser_id}"
        )
        response.raise_for_status()
        return response.json()

    async def check(self) -> bool:
        """Prove the credentials work by running the ``advertisers`` lookup.

        Returns:
            True — any credential failure raises out of the lookup.
        """
        await self.advertisers()
        return True
