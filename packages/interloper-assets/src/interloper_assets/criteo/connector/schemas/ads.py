import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Ads(Schema):
    """Criteo ad snapshot. One row per ad and day, as the advertiser's ad list returns it. An ad names its ad set only; the campaign is its ad set's. id is the join key to ad_id in the reports."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    id: str | None = Field(default=None, description="Ad id.")
    type: str | None = Field(default=None, description="Entity type Criteo names the resource with.")
    name: str | None = Field(default=None, description="Ad name.")
    description: str | None = Field(default=None, description="Ad description.")
    creative_id: str | None = Field(default=None, description="Creative the ad shows.")
    ad_set_id: str | None = Field(default=None, description="Ad set the ad belongs to.")
    inventory_type: str | None = Field(default=None, description="Inventory the ad serves on: Display or Native.")
    start_date: dt.date | None = Field(default=None, description="Day the ad is launched.")
    end_date: dt.date | None = Field(default=None, description="Day the ad stops showing; empty when it runs on.")
