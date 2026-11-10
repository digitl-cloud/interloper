import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Adsets(Schema):
    """Facebook Ads ad set snapshot. One row per ad set and day: its schedule, budget, bidding, learning stage and targeting. id is the join key to adset_id in the insights tables. Budgets are integers in the currency's minor units; creation and update times are UTC instants; start_time and end_time keep the vendor's ISO 8601 string with the account's offset. Nested objects (targeting, promoted_object, bid_constraints...) are kept whole as JSON."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    account_id: str | None = Field(default=None, description="Ad account the ad set belongs to.")
    adlabels: str | None = Field(default=None, description="JSON array of ad labels attached to the ad set.")
    bid_amount: int | None = Field(
        default=None, description="Bid cap or target, in the currency's minor units, when the bid strategy uses one."
    )
    bid_constraints: str | None = Field(default=None, description="JSON of the bid constraints, such as a ROAS floor.")
    bid_info: str | None = Field(default=None, description="JSON map of bid objective to amount.")
    bid_strategy: str | None = Field(
        default=None,
        description="LOWEST_COST_WITHOUT_CAP, LOWEST_COST_WITH_BID_CAP, COST_CAP or LOWEST_COST_WITH_MIN_ROAS.",
    )
    budget_remaining: int | None = Field(
        default=None, description="Remaining budget, in the currency's minor units (cents for EUR and USD)."
    )
    campaign_id: str | None = Field(default=None, description="Campaign the ad set belongs to.")
    created_time: dt.datetime | None = Field(default=None, description="When the ad set was created, in UTC.")
    daily_budget: int | None = Field(
        default=None,
        description="Daily budget; NULL when the ad set has none, in the currency's minor units (cents for EUR and USD).",
    )
    effective_status: str | None = Field(
        default=None,
        description="Delivery status after Facebook's own checks: ACTIVE, PAUSED, CAMPAIGN_PAUSED, IN_PROCESS, WITH_ISSUES...",
    )
    end_time: str | None = Field(
        default=None,
        description=(
            "Scheduled end as the vendor sends it, ISO 8601 with the account's UTC offset; NULL when open-ended. "
            "A string because a timestamp would store it in UTC and lose the offset; its date part is the local day."
        ),
    )
    id: str | None = Field(default=None, description="Ad set id. Join key to adset_id in the insights tables.")
    learning_stage_info: str | None = Field(default=None, description="JSON of the ad set's learning-phase status.")
    lifetime_budget: int | None = Field(
        default=None,
        description="Lifetime budget; NULL when the ad set has none, in the currency's minor units (cents for EUR and USD).",
    )
    name: str | None = Field(default=None, description="Ad set name.")
    promoted_object: str | None = Field(
        default=None, description="JSON of what the ad set promotes: pixel and event, app, page..."
    )
    start_time: str | None = Field(
        default=None,
        description=(
            "Scheduled start as the vendor sends it, ISO 8601 with the account's UTC offset. "
            "A string because a timestamp would store it in UTC and lose the offset; its date part is the local day."
        ),
    )
    status: str | None = Field(
        default=None, description="Status set by the advertiser: ACTIVE, PAUSED, DELETED, ARCHIVED."
    )
    targeting: str | None = Field(default=None, description="JSON of the full targeting specification.")
    updated_time: dt.datetime | None = Field(default=None, description="When the ad set was last changed, in UTC.")
