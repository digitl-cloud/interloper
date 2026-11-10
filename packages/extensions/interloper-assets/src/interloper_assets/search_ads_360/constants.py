BASE_URL = "https://searchads360.googleapis.com/v0"
SCOPES = ["https://www.googleapis.com/auth/doubleclicksearch"]

CAMPAIGN_FIELDS = [
    "campaign.name",
    "campaign.id",
    "campaign.status",
    "campaign.advertising_channel_type",
    "campaign.bidding_strategy_type",
    "customer.id",
    "customer.account_type",
    "segments.date",
    "metrics.impressions",
    "metrics.average_cost",
    "metrics.clicks",
    "metrics.ctr",
    "metrics.average_cpc",
    "metrics.cost_micros",
]

CUSTOMER_CLIENT_FIELDS = [
    "customer.descriptive_name",
    "customer.id",
    "customer_client.descriptive_name",
    "customer_client.id",
    "customer_client.status",
    "customer_client.level",
    "customer_client.currency_code",
    "customer_client.manager",
]
