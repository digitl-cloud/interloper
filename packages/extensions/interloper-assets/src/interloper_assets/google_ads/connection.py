import os
from functools import cached_property
from typing import TYPE_CHECKING, Any

import httpx2
import interloper as il
from interloper.errors import ConnectionCheckError
from pydantic import model_validator
from pydantic_settings import SettingsConfigDict

from interloper_assets.google.errors import GoogleApiError
from interloper_assets.google_ads.api import GoogleAdsApi
from interloper_assets.google_ads.constants import API_VERSION

if TYPE_CHECKING:
    from google.ads.googleads.client import GoogleAdsClient

_TOKEN_URL = "https://oauth2.googleapis.com/token"
_BASE_URL = f"https://googleads.googleapis.com/{API_VERSION}"
_NAME_QUERY = "SELECT customer.id, customer.descriptive_name, customer.status FROM customer LIMIT 1"


@il.connection(
    name="Google Ads",
    icon="logos:google-ads",
    tags=["Advertising"],
    oauth=il.OAuthConfig(
        "google",
        scope="https://www.googleapis.com/auth/adwords",
        fields={
            "client_id": "client_id",
            "client_secret": "client_secret",
            "refresh_token": "refresh_token",
            "developer_token": "developer_token",
        },
    ),
    maturity="beta",
)
class GoogleAdsConnection(il.RefreshTokenOAuthConnection):
    """Google Ads API connection using the Google Ads Python client library."""

    model_config = SettingsConfigDict(env_prefix="google_ads_")

    developer_token: str = il.SecretField(description="Google Ads API developer token")

    @model_validator(mode="before")
    @classmethod
    def resolve_credentials(cls, data: Any) -> Any:
        """Inject blank credentials from the in-house env before validation.

        Overrides the base to also resolve ``developer_token``, a Google-Ads-
        specific token read from ``INTERLOPER_GOOGLE_ADS_DEVELOPER_TOKEN`` (the
        OAuth trio comes from the provider-scoped ``INTERLOPER_GOOGLE_*``). An
        explicit value overrides the in-house one; when neither the caller nor
        env supplies one, the required check fails.

        Args:
            data: The raw connection input, before field validation.

        Returns:
            The (possibly augmented) input data.
        """
        if isinstance(data, dict):
            cls.resolve_field(data, "client_id", os.environ.get("INTERLOPER_GOOGLE_ADS_CLIENT_ID"))
            cls.resolve_field(data, "client_secret", os.environ.get("INTERLOPER_GOOGLE_ADS_CLIENT_SECRET"))
            cls.resolve_field(data, "developer_token", os.environ.get("INTERLOPER_GOOGLE_ADS_DEVELOPER_TOKEN"))
        return data

    @cached_property
    def client(self) -> "GoogleAdsClient":
        """The Google Ads SDK client every caller shares.

        Returns:
            The initialised client, cached per connection instance.
        """
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

    @cached_property
    def api(self) -> GoogleAdsApi:
        """The Google Ads API engine every report shares, built on ``client``.

        Returns:
            The engine, cached per connection instance.
        """
        return GoogleAdsApi(self.client)

    @il.fetch_field_provider
    async def customers(self) -> list[dict[str, str]]:
        """Fetch Google Ads customer accounts accessible by this connection.

        Runs over the REST API rather than through ``api``: the lookup runs in
        the API process, which carries no Google Ads SDK. A customer that
        refuses to be named (suspended, cancelled) is offered under its id.

        Returns:
            The options for the field's dropdown.

        Raises:
            ConnectionCheckError: If Google refuses the token exchange or the
                listing, lists no customer, or refuses to name every customer
                it lists, carrying Google's reason, which names no credential.
        """
        try:
            return await self._customers()
        except GoogleApiError as error:
            raise ConnectionCheckError(str(error)) from error

    async def _customers(self) -> list[dict[str, str]]:
        """List and name the accessible customers, raising Google's refusals.

        Returns:
            Each accessible customer's id and name, its id standing in for a name it refuses to give.

        Raises:
            ConnectionCheckError: If Google lists no customer these credentials can access.
        """
        async with httpx2.AsyncClient(timeout=30) as client:
            token = await self._send(
                client,
                "POST",
                _TOKEN_URL,
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": self.refresh_token,
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                },
            )
            headers = {"Authorization": f"Bearer {token['access_token']}", "developer-token": self.developer_token}
            listing = await self._send(client, "GET", f"{_BASE_URL}/customers:listAccessibleCustomers", headers=headers)
            resource_names: list[str] = listing.get("resourceNames") or []
            if not resource_names:
                raise ConnectionCheckError("Google Ads lists no customer these credentials can access")

            results: list[dict[str, str]] = []
            refusals: list[GoogleApiError] = []
            for resource_name in resource_names:
                customer_id = resource_name.split("/")[-1]
                try:
                    batches = await self._send(
                        client,
                        "POST",
                        f"{_BASE_URL}/{resource_name}/googleAds:searchStream",
                        headers=headers,
                        json={"query": _NAME_QUERY},
                    )
                except GoogleApiError as refusal:
                    refusals.append(refusal)
                    results.append({"customer_id": customer_id, "name": customer_id})
                    continue
                for batch in batches:
                    for row in batch.get("results", []):
                        name = row.get("customer", {}).get("descriptiveName", customer_id)
                        results.append({"customer_id": customer_id, "name": name})

        # credentials every customer refuses can run no report, whatever the listing says
        if len(refusals) == len(resource_names):
            raise refusals[0]
        return results

    @staticmethod
    async def _send(client: httpx2.AsyncClient, method: str, url: str, **kwargs: Any) -> Any:
        """Send one request of the lookup and read its answer.

        A refusal raises as `GoogleApiError`, with Google's reason.

        Args:
            client: The client the lookup's requests share.
            method: The HTTP method.
            url: The request's URL.
            **kwargs: The request's ``data``, ``json`` or ``headers``.

        Returns:
            The answer's JSON body.
        """
        response = await client.request(method, url, **kwargs)
        if response.is_error:
            refusal = GoogleApiError.from_response(response)
            raise refusal
        return response.json()

    async def check(self) -> bool:
        """Prove the credentials work by running the ``customers`` lookup.

        Returns:
            True — any credential failure raises out of the lookup, Google's refusal as a `ConnectionCheckError`.
        """
        await self.customers()
        return True
