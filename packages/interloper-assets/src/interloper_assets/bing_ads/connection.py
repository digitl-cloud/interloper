from functools import cached_property
from typing import Any

import interloper as il
from pydantic_settings import SettingsConfigDict


@il.connection(
    name="Bing Ads",
    icon="icon:bing",
    tags=["Advertising"],
)
class BingAdsConnection(il.OAuthConnection):
    """Bing Ads API connection with OAuth2 refresh token auth."""

    model_config = SettingsConfigDict(env_prefix="bing_ads_")

    developer_token: str = il.SecretField(description="Bing Ads developer token")
    customer_id: str = il.InputField(description="Bing Ads customer ID")
    account_id: str = il.InputField(description="Bing Ads account ID")

    @cached_property
    def client(self) -> il.RESTClient:
        # Not used directly -- Bing Ads uses the bingads SDK
        return il.RESTClient("https://bingads.microsoft.com")

    @cached_property
    def authorization_data(self) -> Any:
        """Authenticate and return AuthorizationData for the Bing Ads SDK."""
        from bingads import AuthorizationData, OAuthAuthorization, OAuthWebAuthCodeGrant

        oauth_web_auth_code_grant = OAuthWebAuthCodeGrant(
            client_id=self.client_id,
            client_secret=self.client_secret,
            redirection_uri=None,
        )
        oauth_tokens = oauth_web_auth_code_grant.request_oauth_tokens_by_refresh_token(self.refresh_token)

        auth_data = AuthorizationData(
            developer_token=self.developer_token,
            authentication=OAuthAuthorization(
                client_id=oauth_web_auth_code_grant.client_id,
                oauth_tokens=oauth_tokens,
            ),
        )
        auth_data.account_id = self.account_id
        auth_data.customer_id = self.customer_id
        return auth_data

    @cached_property
    def reporting_service(self) -> Any:
        """Return a ReportingService client for building report requests."""
        from bingads import ServiceClient

        return ServiceClient("ReportingService", 13, self.authorization_data)

    @cached_property
    def reporting_service_manager(self) -> Any:
        """Return a ReportingServiceManager for downloading reports."""
        from bingads.v13.reporting.reporting_service_manager import ReportingServiceManager

        return ReportingServiceManager(self.authorization_data)
