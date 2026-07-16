import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Partners(Schema):
    """DV360 partners and their configuration."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    name: str | None = Field(default=None, description="The name of the partner")
    partner_id: str | None = Field(default=None, description="The ID of the partner")
    update_time: str | None = Field(default=None, description="The last update time of the partner")
    display_name: str | None = Field(default=None, description="The display name of the partner")
    entity_status: str | None = Field(default=None, description="The status of the entity")
    general_config_time_zone: str | None = Field(default=None, description="The time zone of the partner")
    general_config_currency_code: str | None = Field(default=None, description="Currency of the partner")
    data_access_config_sdf_config_version: str | None = Field(default=None, description="SDF config version")
    exchange_config_enabled_exchanges: str | None = Field(default=None, description="Enabled exchanges")
    billing_config_billing_profile_id: str | None = Field(default=None, description="The ID of the billing profile")
    ad_server_config_measurement_config_dv_360_to_cm_cost_reporting_enabled: bool | None = Field(
        default=None, description="Flag indicating if cost reporting is enabled from DV360 to CM"
    )
    ad_server_config_measurement_config_dv_360_to_cm_data_sharing_enabled: bool | None = Field(
        default=None, description="Flag indicating if data sharing is enabled from DV360 to CM"
    )
    data_access_config_sdf_config_admin_email: str | None = Field(
        default=None, description="Admin email for SDF config"
    )

