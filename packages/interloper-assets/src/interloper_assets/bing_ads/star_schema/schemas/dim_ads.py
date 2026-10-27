import datetime as dt

from interloper import Schema
from pydantic import Field


class DimAds(Schema):
    ad_id: str | None = Field(default=None, description="Ad id, the key the facts join on.")
    ad_group_id: str | None = Field(default=None, description="Ad group the ad belongs to.")
    campaign_id: str | None = Field(default=None, description="Campaign the ad belongs to.")
    account_id: str | None = Field(default=None, description="Account the ad belongs to.")
    ad_name: str | None = Field(default=None, description="NULL: Bing ads have no name; see headline.")
    status: str | None = Field(default=None, description="Ad status as the latest report states it.")
    created_at: dt.datetime | None = Field(default=None, description="NULL: Bing reports no creation time.")
    updated_at: dt.datetime | None = Field(default=None, description="NULL: Bing reports no change time.")
    as_of_date: dt.date | None = Field(default=None, description="Day of the row this dimension was taken from.")
    ad_type: str | None = Field(default=None, description="Ad type (Responsive search, Expanded text...).")
    headline: str | None = Field(default=None, description="The ad's headline, as the latest report states it.")
