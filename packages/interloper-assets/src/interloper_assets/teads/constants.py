import httpx2

BASE_URL = "https://ads.teads.tv"

REPORT_TIMEZONE = "Etc/GMT+0"
REPORT_POLL_INTERVAL = 30.0
REPORT_TIMEOUT = 30 * 60
DOWNLOAD_TIMEOUT = httpx2.Timeout(300.0, connect=30.0)

STANDARD_DIMENSIONS = [
    "day",
    "advertiser_id",
    "advertiser_name",
    "ad_external_integration_code",
    "ad_id",
    "ad_external_name",
    "creative_id",
    "creative_external_name",
    "creative_external_integration_code",
    "io_line_id",
    "io_line_external_name",
    "price_advertiser_event",
    "io_line_budget",
]

STANDARD_METRICS = [
    "turnover_value",
    "complete-rate",
    "budget_delivered_advertising_value",
    "budget_delivered_value",
    "click",
    "advertiser_billable_volume",
    "click-rate",
    "start",
    "budget_delivered_average_cpc_value",
    "complete",
    "budget_delivered_average_cpm_value",
]
