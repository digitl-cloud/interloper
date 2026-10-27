import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class AdAccounts(Schema):
    """Facebook Ads ad account snapshot. One row per ad account and day: the account's name, currency, timezone, status and billing settings. account_id is the join key to every other Facebook Ads table. Amounts are integers in the currency's minor units. Nested objects (business, capabilities, rf_spec...) are kept whole as JSON."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    account_id: str | None = Field(
        default=None, description="Numeric ad account id, without the act_ prefix. Join key to account_id everywhere."
    )
    account_status: int | None = Field(
        default=None, description="Account status code: 1 ACTIVE, 2 DISABLED, 3 UNSETTLED, 7 PENDING_RISK_REVIEW, ..."
    )
    ad_account_promotable_objects: str | None = Field(
        default=None, description="JSON of the objects the account can promote."
    )
    age: float | None = Field(default=None, description="How long the account has been open.")
    amount_spent: int | None = Field(
        default=None,
        description="Amount spent by the account, relative to spend_cap when one is set, in the currency's minor units (cents for EUR and USD).",
    )
    balance: int | None = Field(
        default=None, description="Bill amount currently due, in the currency's minor units (cents for EUR and USD)."
    )
    brand_safety_content_filter_levels: str | None = Field(
        default=None, description="JSON array of the brand-safety content filter levels applied."
    )
    business: str | None = Field(default=None, description="JSON of the Business that owns the account: {id, name}.")
    business_city: str | None = Field(default=None, description="City of the account's business address.")
    business_country_code: str | None = Field(default=None, description="Country of the account's business address.")
    business_name: str | None = Field(
        default=None, description="Business name the account is billed to; may differ from business.name."
    )
    business_state: str | None = Field(default=None, description="State of the account's business address.")
    can_create_brand_lift_study: bool | None = Field(
        default=None, description="Whether a brand lift study can be created on the account."
    )
    capabilities: str | None = Field(default=None, description="JSON array of the account's capabilities.")
    created_time: dt.datetime | None = Field(default=None, description="When the account was created, in UTC.")
    currency: str | None = Field(default=None, description="ISO currency of every amount on the account.")
    default_dsa_beneficiary: str | None = Field(
        default=None, description="Default beneficiary declared under the EU Digital Services Act."
    )
    default_dsa_payor: str | None = Field(
        default=None, description="Default payor declared under the EU Digital Services Act."
    )
    disable_reason: int | None = Field(default=None, description="Reason code when the account is disabled.")
    end_advertiser: str | None = Field(default=None, description="Id of the end advertiser the account serves.")
    end_advertiser_name: str | None = Field(default=None, description="Name of the end advertiser.")
    existing_customers: str | None = Field(default=None, description="JSON of the existing-customer definitions.")
    extended_credit_invoice_group: str | None = Field(
        default=None, description="JSON of the extended credit invoice group the account bills to."
    )
    failed_delivery_checks: str | None = Field(
        default=None, description="JSON array of the delivery checks the account fails."
    )
    fb_entity: int | None = Field(default=None, description="Code of the Meta legal entity the account contracts with.")
    funding_source: str | None = Field(default=None, description="Id of the account's funding source.")
    has_migrated_permissions: bool | None = Field(
        default=None, description="Whether the account has migrated to the unified permissions model."
    )
    has_page_authorized_adaccount: bool | None = Field(
        default=None, description="Whether a Page has authorized the account."
    )
    id: str | None = Field(default=None, description="Graph node id: act_ followed by account_id.")
    io_number: str | None = Field(default=None, description="Insertion order number.")
    is_attribution_spec_system_default: bool | None = Field(
        default=None, description="Whether the account uses the system default attribution windows."
    )
    is_direct_deals_enabled: bool | None = Field(default=None, description="Whether direct deals are enabled.")
    is_in_3ds_authorization_enabled_market: bool | None = Field(
        default=None, description="Whether the account is in a market requiring 3DS card authorization."
    )
    is_notifications_enabled: bool | None = Field(default=None, description="Whether notifications are enabled.")
    is_personal: int | None = Field(default=None, description="Whether the account is for personal use (1) or not.")
    line_numbers: str | None = Field(default=None, description="JSON array of the account's line numbers.")
    marketing_messages_settings: str | None = Field(
        default=None, description="JSON of the account's marketing messages settings."
    )
    media_agency: str | None = Field(default=None, description="Id of the media agency business on the account.")
    min_campaign_group_spend_cap: int | None = Field(
        default=None,
        description="Minimum spend cap a campaign on the account may set, in the currency's minor units (cents for EUR and USD).",
    )
    min_daily_budget: int | None = Field(
        default=None, description="Minimum daily budget an ad set may set, in the currency's minor units."
    )
    name: str | None = Field(default=None, description="Ad account name.")
    offsite_clo_signal_status: int | None = Field(default=None, description="Status of the account's offsite signal.")
    offsite_pixels_tos_accepted: bool | None = Field(
        default=None, description="Whether the offsite pixel terms of service are accepted."
    )
    opportunity_score: float | None = Field(default=None, description="Meta's opportunity score for the account.")
    opportunity_score_weight: int | None = Field(default=None, description="Weight of the opportunity score.")
    owner: str | None = Field(default=None, description="Id of the business or user that owns the account.")
    partner: str | None = Field(default=None, description="Id of the partner business on the account.")
    rf_spec: str | None = Field(default=None, description="JSON of the account's reach and frequency limits.")
    spend_cap: int | None = Field(
        default=None,
        description="Lifetime spend cap of the account; 0 when none, in the currency's minor units (cents for EUR and USD).",
    )
    tax_id_status: int | None = Field(default=None, description="Status code of the account's tax id.")
    tax_id_type: str | None = Field(default=None, description="Type of the account's tax id.")
    timezone_id: str | None = Field(default=None, description="Meta's id of the account's timezone.")
    timezone_name: str | None = Field(
        default=None, description="IANA timezone the account reports in; every report day is in this timezone."
    )
    timezone_offset_hours_utc: float | None = Field(
        default=None, description="Current offset of the account's timezone from UTC, in hours."
    )
    tos_accepted: str | None = Field(default=None, description="JSON map of terms of service to their acceptance.")
