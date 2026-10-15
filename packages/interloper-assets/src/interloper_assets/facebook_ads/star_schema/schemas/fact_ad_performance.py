import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class FactAdPerformance(Schema):
    """Ad performance of every Facebook Ads account, one row per ad, day, publisher platform, position and device."""

    date: dt.date | None = Field(default=None, description="The report day.")
    account_id: str | None = Field(default=None, description="Ad account id.")
    account_name: str | None = Field(default=None, description="Ad account name.")
    account_currency: str | None = Field(default=None, description="Currency of the account's amounts.")
    campaign_id: str | None = Field(default=None, description="Campaign id.")
    campaign_name: str | None = Field(default=None, description="Campaign name as reported with the metrics.")
    adset_id: str | None = Field(default=None, description="Ad set id.")
    adset_name: str | None = Field(default=None, description="Ad set name as reported with the metrics.")
    ad_id: str | None = Field(default=None, description="Ad id.")
    ad_name: str | None = Field(default=None, description="Ad name as reported with the metrics.")
    publisher_platform: str | None = Field(
        default=None, description="Platform the ad was shown on (facebook, instagram, ...)."
    )
    platform_position: str | None = Field(default=None, description="Placement on the platform (feed, story, ...).")
    impression_device: str | None = Field(default=None, description="Device the ad was shown on.")
    impressions: int | None = Field(default=None, description="Times the ad was shown.")
    clicks: int | None = Field(default=None, description="Clicks on the ad.")
    reach: int | None = Field(default=None, description="People who saw the ad.")
    spend: float | None = Field(default=None, description="Amount spent, in the account currency.")
