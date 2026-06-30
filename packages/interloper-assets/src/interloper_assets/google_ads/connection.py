from functools import cached_property
from typing import TYPE_CHECKING, Any

import httpx
import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.google_ads.constants import API_VERSION

if TYPE_CHECKING:
    from google.ads.googleads.client import GoogleAdsClient

_TOKEN_URL = "https://oauth2.googleapis.com/token"
_BASE_URL = "https://googleads.googleapis.com/v20"


@il.connection(
    name="Google Ads",
    icon="logos:google-ads",
    tags=["Advertising"],
)
class GoogleAdsConnection(il.RefreshTokenOAuthConnection):
    """Google Ads API connection using the Google Ads Python client library."""

    model_config = SettingsConfigDict(env_prefix="google_ads_")

    developer_token: str = il.SecretField(description="Google Ads API developer token")

    @cached_property
    def client(self) -> "GoogleAdsClient":
        from google.ads.googleads.client import GoogleAdsClient

        config = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
            "developer_token": self.developer_token,
            "use_proto_plus": True,
            "api_version": API_VERSION,
        }
        return GoogleAdsClient.load_from_dict(config)

    def to_dict(self, message: Any) -> dict:
        """Convert a GoogleAdsRow (proto-plus) into a flat dictionary."""
        return type(message).to_dict(
            message,
            preserving_proto_field_name=True,
            use_integers_for_enums=False,
            including_default_value_fields=False,
        )

    @il.fetch_field_provider
    async def customers(self) -> list[dict[str, str]]:
        """Fetch Google Ads customer accounts accessible by this connection."""
        async with httpx.AsyncClient(timeout=30) as client:
            # Exchange the refresh token for an access token.
            token_resp = await client.post(
                _TOKEN_URL,
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": self.refresh_token,
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                },
            )
            token_resp.raise_for_status()
            access_token = token_resp.json()["access_token"]

            headers = {
                "Authorization": f"Bearer {access_token}",
                "developer-token": self.developer_token,
            }

            # Step 1: List accessible customer resource names.
            list_resp = await client.get(
                f"{_BASE_URL}/customers:listAccessibleCustomers",
                headers=headers,
            )
            list_resp.raise_for_status()
            resource_names: list[str] = list_resp.json().get("resourceNames", [])

            # Step 2: Fetch descriptive name for each customer.
            results: list[dict[str, str]] = []
            for rn in resource_names:
                # rn is like "customers/1234567890"
                customer_id = rn.split("/")[-1]
                query = "SELECT customer.id, customer.descriptive_name, customer.status FROM customer LIMIT 1"
                try:
                    search_resp = await client.post(
                        f"{_BASE_URL}/{rn}/googleAds:searchStream",
                        headers=headers,
                        json={"query": query},
                    )
                    search_resp.raise_for_status()
                    batches = search_resp.json()
                    for batch in batches:
                        for row in batch.get("results", []):
                            customer = row.get("customer", {})
                            name = customer.get("descriptiveName", customer_id)
                            results.append({
                                "customer_id": customer_id,
                                "name": f"{name} ({customer_id})",
                            })
                except httpx.HTTPStatusError:
                    # Some customers may not be queryable (suspended, etc.)
                    results.append({
                        "customer_id": customer_id,
                        "name": customer_id,
                    })

        return results
