from collections.abc import Generator
from enum import Enum
from functools import cached_property

import httpx
import interloper as il
from pydantic_settings import SettingsConfigDict


class AmazonSellingPartnerLocation(Enum):
    NORTH_AMERICA = "NA"
    EUROPE = "EU"
    FAR_EAST = "FE"

    @property
    def api_url(self) -> str:
        return {
            AmazonSellingPartnerLocation.EUROPE: "https://sellingpartnerapi-eu.amazon.com",
            AmazonSellingPartnerLocation.FAR_EAST: "https://sellingpartnerapi-fe.amazon.com",
            AmazonSellingPartnerLocation.NORTH_AMERICA: "https://sellingpartnerapi-na.amazon.com",
        }[self]

    @property
    def auth_url(self) -> str:
        return {
            AmazonSellingPartnerLocation.EUROPE: "https://api.amazon.co.uk",
            AmazonSellingPartnerLocation.FAR_EAST: "https://api.amazon.co.jp",
            AmazonSellingPartnerLocation.NORTH_AMERICA: "https://api.amazon.com",
        }[self]


# Marketplaces grouped by the region whose API host serves them. A connection is
# bound to one region (its client host), so the source's marketplace picker is
# scoped to the region's entries via the ``marketplaces`` fetch-field provider —
# a request for an out-of-region marketplace is rejected by SP-API.
MARKETPLACES_BY_LOCATION: dict[AmazonSellingPartnerLocation, list[tuple[str, str]]] = {
    AmazonSellingPartnerLocation.NORTH_AMERICA: [
        ("ATVPDKIKX0DER", "United States"),
        ("A2EUQ1WTGCTBG2", "Canada"),
        ("A1AM78C64UM0Y8", "Mexico"),
        ("A2Q3Y263D00KWC", "Brazil"),
    ],
    AmazonSellingPartnerLocation.EUROPE: [
        ("A1F83G8C2ARO7P", "United Kingdom"),
        ("A1PA6795UKMFR9", "Germany"),
        ("A13V1IB3VIYZZH", "France"),
        ("APJ6JRA9NG5V4", "Italy"),
        ("A1RKKUPIHCS9HS", "Spain"),
        ("A1805IZSGTT6HS", "Netherlands"),
        ("A2NODRKZP88ZB9", "Sweden"),
        ("A1C3SOZRARQ6R3", "Poland"),
        ("AMEN7PMS3EDWL", "Belgium"),
        ("A21TJRUUN4KGV", "India"),
        ("A33AVAJ2PDY3EV", "Turkey"),
        ("A17E79C6D8DWNP", "Saudi Arabia"),
        ("A2VIGQ35RCS4UG", "United Arab Emirates"),
        ("ARBP9OOSHTCHU", "Egypt"),
        ("AE08WJ6YKNBMC", "South Africa"),
    ],
    AmazonSellingPartnerLocation.FAR_EAST: [
        ("A1VC38T7YXB528", "Japan"),
        ("A39IBJ37TRP1C6", "Australia"),
        ("A19VAU5U5O7RUS", "Singapore"),
    ],
}


class LWATokenAuth(il.OAuth2RefreshTokenAuth):
    """LWA refresh-token auth that carries the token in ``x-amz-access-token``.

    SP-API authenticates with the Login-with-Amazon access token supplied in the
    ``x-amz-access-token`` header (request signing with AWS SigV4 is no longer
    required), not the ``Authorization`` header the base OAuth2 flow sets — so
    override the flow to place the token there and refresh on 401/403.
    """

    def auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, httpx.Response, None]:
        if self._access_token is None:
            self._store_token((yield self._token_request()))

        request.headers["x-amz-access-token"] = self.access_token
        response = yield request

        if response.status_code in (401, 403):
            self._store_token((yield self._token_request()))
            request.headers["x-amz-access-token"] = self.access_token
            yield request


@il.connection(
    name="Amazon Selling Partner",
    icon="icon:amazon",
    tags=["E-commerce"],
)
class AmazonSellingPartnerConnection(il.Connection):
    """Amazon Selling Partner API connection with OAuth2 refresh token auth (LWA)."""

    model_config = SettingsConfigDict(env_prefix="amazon_sp_")

    location: str = il.SelectField(
        options=[
            {"label": "Europe", "value": "EU"},
            {"label": "North America", "value": "NA"},
            {"label": "Far East", "value": "FE"},
        ],
        description="API region",
        discriminator=True,
    )
    client_id: str = il.InputField(label="Client ID", description="LWA client ID")
    client_secret: str = il.SecretField(label="Client Secret", description="LWA client secret")
    refresh_token: str = il.SecretField(label="Refresh Token", description="LWA refresh token")

    @cached_property
    def api_location(self) -> AmazonSellingPartnerLocation:
        return AmazonSellingPartnerLocation(self.location)

    @il.fetch_field_provider
    async def marketplaces(self) -> list[dict[str, str]]:
        """Marketplaces served by this connection's region.

        A static region→marketplaces lookup (no API call): SP-API exposes no
        vendor-accessible endpoint to enumerate a connection's marketplaces, so
        this scopes the picker to the region's valid ids rather than detecting
        access. Requests for an out-of-region marketplace are rejected by SP-API.
        """
        return [{"label": label, "value": value} for value, label in MARKETPLACES_BY_LOCATION[self.api_location]]

    @cached_property
    def client(self) -> il.AsyncRESTClient:
        """Async REST client for the Amazon Selling Partner API."""
        location = self.api_location
        return il.AsyncRESTClient(
            location.api_url,
            auth=LWATokenAuth(
                base_url=location.auth_url,
                token_endpoint="/auth/o2/token",
                client_id=self.client_id,
                client_secret=self.client_secret,
                refresh_token=self.refresh_token,
            ),
            headers={"user-agent": "interloper (Language=Python)"},
            timeout=60,
        )

    async def check(self) -> bool:
        """Prove the credentials work by exchanging the LWA refresh token.

        Returns:
            True — an invalid client id/secret or refresh token raises out of the
            token exchange.
        """
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                f"{self.api_location.auth_url}/auth/o2/token",
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": self.refresh_token,
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                },
            )
            response.raise_for_status()
        return True
