DIMENSIONS = [
    "FILTER_ADVERTISER_CURRENCY",
    "FILTER_ADVERTISER_NAME",
    "FILTER_ADVERTISER",
    "FILTER_BUDGET_SEGMENT_DESCRIPTION",
    "FILTER_CM360_PLACEMENT_ID",
    "FILTER_CREATIVE_ID",
    "FILTER_CREATIVE_TYPE",
    "FILTER_CREATIVE",
    "FILTER_DATE",
    "FILTER_INSERTION_ORDER_NAME",
    "FILTER_INSERTION_ORDER",
    "FILTER_INVENTORY_COMMITMENT_TYPE",
    "FILTER_INVENTORY_DELIVERY_METHOD",
    "FILTER_LINE_ITEM_END_DATE",
    "FILTER_LINE_ITEM_NAME",
    "FILTER_LINE_ITEM_START_DATE",
    "FILTER_LINE_ITEM_TYPE",
    "FILTER_LINE_ITEM",
    "FILTER_MEDIA_PLAN_NAME",
    "FILTER_MEDIA_PLAN",
    "FILTER_PARTNER_CURRENCY",
    "FILTER_PARTNER_NAME",
    "FILTER_PARTNER",
]

COUNTRY_DIMENSIONS = DIMENSIONS + ["FILTER_COUNTRY"]


COUNTRY_METRICS = [
    "METRIC_ACTIVE_VIEW_AVERAGE_VIEWABLE_TIME",
    "METRIC_ACTIVE_VIEW_MEASURABLE_IMPRESSIONS",
    "METRIC_ACTIVE_VIEW_PCT_VIEWABLE_IMPRESSIONS",
    "METRIC_ACTIVE_VIEW_PERCENT_VISIBLE_ON_COMPLETE",
    "METRIC_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS",
    "METRIC_BILLABLE_IMPRESSIONS",
    "METRIC_CLICKS",
    "METRIC_CPM_FEE1_PARTNER",
    "METRIC_CPM_FEE2_PARTNER",
    "METRIC_CTR",
    "METRIC_DATA_COST_ADVERTISER",
    "METRIC_DATA_COST_PARTNER",
    "METRIC_FEE32_ADVERTISER",
    "METRIC_FEE32_PARTNER",
    "METRIC_IMPRESSIONS",
    "METRIC_LAST_CLICKS",
    "METRIC_LAST_IMPRESSIONS",
    "METRIC_MEDIA_COST_ADVERTISER",
    "METRIC_MEDIA_COST_PARTNER",
    "METRIC_MEDIA_FEE1_PARTNER",
    "METRIC_MEDIA_FEE2_PARTNER",
    "METRIC_REVENUE_ADVERTISER",
    "METRIC_TOTAL_CONVERSIONS",
    "METRIC_TOTAL_MEDIA_COST_ADVERTISER",
    "METRIC_TOTAL_MEDIA_COST_PARTNER",
    "METRIC_TRUEVIEW_CPV_ADVERTISER",
    "METRIC_APP_MEDIATION_PARTNER_FEE_ADVERTISER_CURRENCY",
    "METRIC_APP_MEDIATION_PARTNER_FEE_PARTNER_CURRENCY",
]

METRICS = COUNTRY_METRICS + [
    "METRIC_ADLINGO_FEE_ADVERTISER_CURRENCY",
    "METRIC_ADLINGO_FEE_PARTNER_CURRENCY",
    "METRIC_BILLABLE_COST_ADVERTISER",
    "METRIC_BILLABLE_COST_PARTNER",
    "METRIC_FEE10_ADVERTISER",
    "METRIC_FEE10_PARTNER",
    "METRIC_FEE11_ADVERTISER",
    "METRIC_FEE11_PARTNER",
    "METRIC_FEE12_ADVERTISER",
    "METRIC_FEE12_PARTNER",
    "METRIC_FEE13_ADVERTISER",
    "METRIC_FEE13_PARTNER",
    "METRIC_FEE14_ADVERTISER",
    "METRIC_FEE14_PARTNER",
    "METRIC_FEE15_ADVERTISER",
    "METRIC_FEE15_PARTNER",
    "METRIC_FEE16_ADVERTISER",
    "METRIC_FEE16_PARTNER",
    "METRIC_FEE17_ADVERTISER",
    "METRIC_FEE17_PARTNER",
    "METRIC_FEE18_ADVERTISER",
    "METRIC_FEE18_PARTNER",
    "METRIC_FEE19_ADVERTISER",
    "METRIC_FEE19_PARTNER",
    "METRIC_FEE2_ADVERTISER",
    "METRIC_FEE2_PARTNER",
    "METRIC_FEE20_ADVERTISER",
    "METRIC_FEE20_PARTNER",
    "METRIC_FEE21_ADVERTISER",
    "METRIC_FEE21_PARTNER",
    "METRIC_FEE22_ADVERTISER",
    "METRIC_FEE22_PARTNER",
    "METRIC_FEE31_ADVERTISER",
    "METRIC_FEE31_PARTNER",
    "METRIC_FEE4_ADVERTISER",
    "METRIC_FEE4_PARTNER",
    "METRIC_FEE5_ADVERTISER",
    "METRIC_FEE5_PARTNER",
    "METRIC_FEE6_ADVERTISER",
    "METRIC_FEE6_PARTNER",
    "METRIC_FEE7_ADVERTISER",
    "METRIC_FEE7_PARTNER",
    "METRIC_FEE8_ADVERTISER",
    "METRIC_FEE8_PARTNER",
    "METRIC_FEE9_ADVERTISER",
    "METRIC_FEE9_PARTNER",
    "METRIC_PLATFORM_FEE_ADVERTISER",
    "METRIC_PLATFORM_FEE_PARTNER",
    "METRIC_PLATFORM_FEE_RATE",
    "METRIC_SCIBIDS_FEE_ADVERTISER_CURRENCY",
    "METRIC_SCIBIDS_FEE_PARTNER_CURRENCY",
    "METRIC_SELLER_ID_BLOCKLIST_FEE_ADVERTISER_CURRENCY",
    "METRIC_SELLER_ID_BLOCKLIST_FEE_PARTNER_CURRENCY",
]
# Minimum amount of time between polling requests. Defaults to 1 minute.
MIN_RETRY_INTERVAL = 60
# Maximum amount of time between polling requests. Defaults to 20 minutes.
MAX_RETRY_INTERVAL = 20 * 60
