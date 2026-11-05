import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Advertisers(Schema):
    """The partner's advertisers with their currency, category, attribution windows and vetting status."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    id: str | None = Field(default=None, description="The ID of the advertiser")
    name: str | None = Field(default=None, description="The name of the advertiser")
    description: str | None = Field(default=None, description="The description of the advertiser")
    currency_id: str | None = Field(
        default=None, description="ISO 4217 code of the advertiser's currency, the currency of its budgets and costs"
    )
    is_archived: bool | None = Field(default=None, description="Whether the advertiser is archived")
    domain_address: str | None = Field(default=None, description="The advertiser's website domain")
    industry_category_id: str | None = Field(default=None, description="The ID of the advertiser's industry category")
    industry_category_name: str | None = Field(
        default=None, description="The name of the advertiser's industry category"
    )
    country_short_name: str | None = Field(default=None, description="ISO code of the advertiser's country")
    attribution_click_lookback_window_in_seconds: int | None = Field(
        default=None, description="Click lookback window for conversion attribution, in seconds"
    )
    attribution_impression_lookback_window_in_seconds: int | None = Field(
        default=None, description="Impression lookback window for conversion attribution, in seconds"
    )
    click_dedupe_window_in_seconds: int | None = Field(
        default=None, description="Window within which repeated clicks are deduplicated, in seconds"
    )
    conversion_de_dupe_window_in_seconds: int | None = Field(
        default=None, description="Window within which repeated conversions are deduplicated, in seconds"
    )
    ignore_referral_url_in_conversion: bool | None = Field(
        default=None, description="Whether the referral URL is ignored when attributing conversions"
    )
    logo_url: str | None = Field(default=None, description="URL of the advertiser's logo")
    vetting_status: str | None = Field(default=None, description="The advertiser's vetting status")
    name_dsa: str | None = Field(
        default=None, description="Advertiser name disclosed under the EU Digital Services Act"
    )
    payer_name_dsa: str | None = Field(
        default=None, description="Payer name disclosed under the EU Digital Services Act"
    )
