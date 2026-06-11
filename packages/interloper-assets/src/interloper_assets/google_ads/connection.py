from functools import cached_property
from typing import TYPE_CHECKING, Any

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.google_ads.constants import API_VERSION

if TYPE_CHECKING:
    from google.ads.googleads.client import GoogleAdsClient


@il.connection(
    name="Google Ads",
    icon="logos:google-ads",
    tags=["Advertising"],
)
class GoogleAdsConnection(il.OAuthConnection):
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
