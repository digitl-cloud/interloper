SA360_BASE_URL = "https://searchads360.googleapis.com/v0"
SA360_SCOPES = ["https://www.googleapis.com/auth/doubleclicksearch"]

# GAQL query templates for SA360 reporting

CAMPAIGNS_QUERY = """
    SELECT
        campaign.name,
        campaign.id,
        campaign.status,
        campaign.advertising_channel_type,
        campaign.bidding_strategy_type,
        customer.id,
        customer.account_type,
        segments.date,
        metrics.impressions,
        metrics.average_cost,
        metrics.clicks,
        metrics.ctr,
        metrics.average_cpc,
        metrics.cost_micros
    FROM campaign
    WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
"""

AD_GROUPS_QUERY = """
    SELECT
        ad_group.name,
        ad_group.id,
        ad_group.status,
        ad_group.type,
        campaign.name,
        campaign.id,
        customer.id,
        segments.date,
        metrics.impressions,
        metrics.clicks,
        metrics.ctr,
        metrics.average_cpc,
        metrics.cost_micros
    FROM ad_group
    WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
"""

ADS_QUERY = """
    SELECT
        ad_group_ad.ad.id,
        ad_group_ad.ad.type,
        ad_group_ad.ad.name,
        ad_group_ad.status,
        ad_group.name,
        ad_group.id,
        campaign.name,
        campaign.id,
        customer.id,
        segments.date,
        metrics.impressions,
        metrics.clicks,
        metrics.ctr,
        metrics.average_cpc,
        metrics.cost_micros
    FROM ad_group_ad
    WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
"""

CONVERSIONS_QUERY = """
    SELECT
        conversion_action.name,
        conversion_action.id,
        conversion_action.category,
        conversion_action.status,
        conversion_action.type,
        campaign.name,
        campaign.id,
        customer.id,
        segments.date,
        segments.conversion_action_name,
        metrics.conversions,
        metrics.conversions_value,
        metrics.all_conversions,
        metrics.all_conversions_value,
        metrics.cross_device_conversions
    FROM conversion_action
    WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
"""

CUSTOMER_CLIENTS_QUERY = """
    SELECT
        customer.descriptive_name,
        customer.id,
        customer_client.descriptive_name,
        customer_client.id,
        customer_client.status,
        customer_client.level,
        customer_client.currency_code,
        customer_client.manager
    FROM customer_client
"""
