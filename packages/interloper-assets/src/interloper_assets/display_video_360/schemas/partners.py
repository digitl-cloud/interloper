from interloper.schema import Schema
from pydantic import Field


class Partners(Schema):
    """DV360 partner metadata including display name, status, and exchange configuration."""

    partner_id: str | None = Field(..., description="The unique identifier for the partner")
    display_name: str | None = Field(..., description="The display name of the partner")
    entity_status: str | None = Field(..., description="The entity status of the partner")
    update_time: str | None = Field(..., description="The last update time of the partner")
    general_config_domain_url: str | None = Field(..., description="The domain URL from general config")
    general_config_currency_code: str | None = Field(..., description="The currency code from general config")
    general_config_time_zone: str | None = Field(..., description="The timezone from general config")
    exchange_config_enabled_exchanges: str | None = Field(
        ..., description="The enabled exchanges from exchange config"
    )
    ad_server_config_third_party_only_config_pixels_enabled: bool | None = Field(
        ..., description="Whether pixels are enabled in ad server config"
    )
    data_access_config_sdf_config_sdf_config_admin_email: str | None = Field(
        ..., description="The SDF config admin email"
    )
    data_access_config_sdf_config_sdf_config_sdf_version: str | None = Field(
        ..., description="The SDF config version"
    )
    billing_config_billing_profile_id: str | None = Field(
        ..., description="The billing profile ID from billing config"
    )
