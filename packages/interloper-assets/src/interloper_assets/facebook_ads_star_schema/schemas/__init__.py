"""Schemas of the Facebook Ads star schema, one class per view."""

from interloper_assets.facebook_ads_star_schema.schemas.dim_accounts import DimAccounts
from interloper_assets.facebook_ads_star_schema.schemas.dim_ads import DimAds
from interloper_assets.facebook_ads_star_schema.schemas.dim_campaigns import DimCampaigns
from interloper_assets.facebook_ads_star_schema.schemas.fact_ad_performance import FactAdPerformance

__all__ = ["DimAccounts", "DimAds", "DimCampaigns", "FactAdPerformance"]
