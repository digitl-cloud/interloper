import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class AdAccounts(Schema):
    """LinkedIn ad account snapshot. One row per ad account and day: the whole account as LinkedIn returns it, with its name, currency, status, type, serving statuses, notification settings and audit stamps."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    id: str | None = Field(default=None, description="The unique identifier of the ad account.")
    name: str | None = Field(default=None, description="The name of the ad account.")
    currency: str | None = Field(default=None, description="The ISO 4217 currency code the ad account is billed in.")
    status: str | None = Field(
        default=None,
        description="The status of the ad account (ACTIVE, CANCELED, DRAFT, PENDING_DELETION or REMOVED).",
    )
    type: str | None = Field(default=None, description="The type of the ad account (BUSINESS or ENTERPRISE).")
    reference: str | None = Field(
        default=None,
        description="The URN of the organization or person the ad account advertises for (e.g. urn:li:organization:123).",
    )
    serving_statuses: str | None = Field(
        default=None,
        description=(
            "JSON array of the account's serving statuses: RUNNABLE when it can serve, otherwise the reasons it "
            "cannot (STOPPED, BILLING_HOLD, ACCOUNT_TOTAL_BUDGET_HOLD, ACCOUNT_END_DATE_HOLD, RESTRICTED_HOLD, "
            "INTERNAL_HOLD)."
        ),
    )
    test: bool | None = Field(default=None, description="Whether the ad account is a test account, which never serves.")
    notified_on_campaign_optimization: bool | None = Field(
        default=None, description="Whether the campaign contact is notified about campaign optimization opportunities."
    )
    notified_on_creative_approval: bool | None = Field(
        default=None, description="Whether the creative contact is notified when a creative is approved."
    )
    notified_on_creative_rejection: bool | None = Field(
        default=None, description="Whether the creative contact is notified when a creative is rejected."
    )
    notified_on_end_of_campaign: bool | None = Field(
        default=None, description="Whether the campaign contact is notified when a campaign completes."
    )
    notified_on_new_features_enabled: bool | None = Field(
        default=None, description="Whether the account owner is notified about new Campaign Manager features."
    )
    version_version_tag: str | None = Field(
        default=None, description="The version tag of the ad account, which changes with every update."
    )
    change_audit_stamps_created_actor: str | None = Field(
        default=None, description="The URN of the member who created the ad account."
    )
    change_audit_stamps_created_time: dt.datetime | None = Field(
        default=None, description="When the ad account was created (UTC)."
    )
    change_audit_stamps_last_modified_actor: str | None = Field(
        default=None, description="The URN of the member who last modified the ad account."
    )
    change_audit_stamps_last_modified_time: dt.datetime | None = Field(
        default=None, description="When the ad account was last modified (UTC)."
    )
