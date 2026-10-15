"""Pre-built source definitions: one connector per vendor, re-exported here."""

from interloper_assets.adservice.connector.source import Adservice, AdserviceConnection
from interloper_assets.adup.connector.source import Adup, AdupConnection
from interloper_assets.amazon_ads.connection import AmazonAdsAPILocation, AmazonAdsConnection
from interloper_assets.amazon_ads.connector.source import AmazonAds
from interloper_assets.amazon_ads.star_schema.source import AmazonAdsStarSchema
from interloper_assets.amazon_selling_partner.connector.source import (
    AmazonSellingPartner,
    AmazonSellingPartnerConnection,
)
from interloper_assets.awin.connector.source import Awin, AwinConnection
from interloper_assets.bing_ads.connector.source import BingAds, BingAdsConnection
from interloper_assets.bing_ads.star_schema.source import BingAdsStarSchema
from interloper_assets.brandwatch.connector.source import Brandwatch, BrandwatchConnection
from interloper_assets.campaign.matcher.source import CampaignMatcher
from interloper_assets.campaign.performance_analysis.source import CampaignPerformanceAnalysis
from interloper_assets.campaign_manager_360.connector.source import CampaignManager360, CampaignManager360Connection
from interloper_assets.criteo.connector.source import Criteo, CriteoConnection
from interloper_assets.criteo.star_schema.source import CriteoStarSchema
from interloper_assets.demo.assets import demo_asset
from interloper_assets.demo.basic.source import DemoSource
from interloper_assets.demo.monthly.source import DemoMonthlySource
from interloper_assets.display_video_360.connector.source import DisplayVideo360, DisplayVideo360Connection
from interloper_assets.facebook_ads.connector.source import FacebookAds, FacebookAdsConnection
from interloper_assets.facebook_ads.star_schema.source import FacebookAdsStarSchema
from interloper_assets.facebook_insights.connector.source import FacebookInsights, FacebookInsightsConnection
from interloper_assets.google_ads.connector.source import GoogleAds, GoogleAdsConnection
from interloper_assets.google_analytics_4.export.source import GoogleAnalytics4Export
from interloper_assets.impact.connector.source import Impact, ImpactConnection
from interloper_assets.instagram_insights.connector.source import InstagramInsights, InstagramInsightsConnection
from interloper_assets.linkedin_ads.connector.source import LinkedinAds, LinkedinAdsConnection
from interloper_assets.linkedin_organic.connector.source import LinkedinOrganic, LinkedinOrganicConnection
from interloper_assets.marketing_mix_modeling.model.source import MarketingMixModeling
from interloper_assets.pinterest_ads.connector.source import PinterestAds, PinterestAdsConnection
from interloper_assets.search_ads_360.connector.source import SearchAds360, SearchAds360Connection
from interloper_assets.search_console.connector.source import SearchConsole, SearchConsoleConnection
from interloper_assets.snapchat_ads.connector.source import SnapchatAds, SnapchatAdsConnection
from interloper_assets.snapchat_ads.star_schema.source import SnapchatAdsStarSchema
from interloper_assets.teads.connector.source import Teads, TeadsConnection
from interloper_assets.thetradedesk.connector.source import TheTradeDesk, TheTradeDeskConnection
from interloper_assets.thetradedesk.star_schema.source import TheTradeDeskStarSchema
from interloper_assets.tiktok_ads.connector.source import TiktokAds, TiktokAdsConnection
from interloper_assets.tiktok_ads.star_schema.source import TiktokAdsStarSchema
from interloper_assets.usercentrics.connector.source import Usercentrics, UsercentricsConnection

__all__ = [
    "Adservice",
    "AdserviceConnection",
    "Adup",
    "AdupConnection",
    "AmazonAds",
    "AmazonAdsAPILocation",
    "AmazonAdsConnection",
    "AmazonAdsStarSchema",
    "AmazonSellingPartner",
    "AmazonSellingPartnerConnection",
    "Awin",
    "AwinConnection",
    "BingAds",
    "BingAdsConnection",
    "BingAdsStarSchema",
    "Brandwatch",
    "BrandwatchConnection",
    "CampaignManager360",
    "CampaignManager360Connection",
    "CampaignMatcher",
    "CampaignPerformanceAnalysis",
    "Criteo",
    "CriteoConnection",
    "CriteoStarSchema",
    "DemoMonthlySource",
    "DemoSource",
    "DisplayVideo360",
    "DisplayVideo360Connection",
    "FacebookAds",
    "FacebookAdsConnection",
    "FacebookAdsStarSchema",
    "FacebookInsights",
    "FacebookInsightsConnection",
    "GoogleAds",
    "GoogleAdsConnection",
    "GoogleAnalytics4Export",
    "Impact",
    "ImpactConnection",
    "InstagramInsights",
    "InstagramInsightsConnection",
    "LinkedinAds",
    "LinkedinAdsConnection",
    "LinkedinOrganic",
    "LinkedinOrganicConnection",
    "MarketingMixModeling",
    "PinterestAds",
    "PinterestAdsConnection",
    "SearchAds360",
    "SearchAds360Connection",
    "SearchConsole",
    "SearchConsoleConnection",
    "SnapchatAds",
    "SnapchatAdsConnection",
    "SnapchatAdsStarSchema",
    "Teads",
    "TeadsConnection",
    "TheTradeDesk",
    "TheTradeDeskConnection",
    "TheTradeDeskStarSchema",
    "TiktokAds",
    "TiktokAdsConnection",
    "TiktokAdsStarSchema",
    "Usercentrics",
    "UsercentricsConnection",
    "demo_asset",
]
