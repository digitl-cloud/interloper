import datetime as dt

from interloper import Schema
from pydantic import Field


class DimAdGroups(Schema):
    ad_group_id: str | None = Field(default=None, description="Ad set id, the key the facts join on.")
    campaign_id: str | None = Field(default=None, description="Campaign the ad set belongs to.")
    account_id: str | None = Field(default=None, description="Advertiser the ad set belongs to.")
    ad_group_name: str | None = Field(default=None, description="Ad set name, from the entity or the ad report.")
    status: str | None = Field(
        default=None, description="Configured status, Criteo's activation status (on, off); entity rows only."
    )
    budget: float | None = Field(
        default=None,
        description="Budget per renewal period, in the advertiser's currency; NULL when uncapped; entity rows only.",
    )
    budget_type: str | None = Field(
        default=None,
        description="Budget renewal period: daily, weekly, monthly, lifetime, or undefined when uncapped; "
        "entity rows only.",
    )
    start_date: dt.date | None = Field(
        default=None,
        description="Scheduled start, in the time zone the advertiser's source sets (UTC unless set); entity rows "
        "only.",
    )
    end_date: dt.date | None = Field(
        default=None,
        description="Scheduled end, in the time zone the advertiser's source sets (UTC unless set); entity rows "
        "only, NULL when the ad set runs on.",
    )
    as_of_date: dt.date | None = Field(default=None, description="Day of the row this dimension was taken from.")
    effective_status: str | None = Field(
        default=None, description="Criteo's delivery status (live, paused, ended...); entity rows only."
    )
