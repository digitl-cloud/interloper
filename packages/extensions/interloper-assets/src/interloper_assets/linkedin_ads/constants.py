BASE_URL = "https://api.linkedin.com/rest"
AUTH_BASE_URL = "https://api.linkedin.com"
LINKEDIN_VERSION = "202607"
RESTLI_PROTOCOL_VERSION = "2.0.0"

ANALYTICS_FIELDS = [
    # Core Performance & Cost
    "costInLocalCurrency",
    "impressions",
    "clicks",
    # Conversions & Leads
    "externalWebsiteConversions",
    "conversionValueInLocalCurrency",
    "costPerQualifiedLead",
    # Audience Engagement
    "likes",
    "comments",
    "shares",
    "totalEngagements",
    "follows",
    "averageDwellTime",
    "fullScreenPlays",
    "cardClicks",
    "audiencePenetration",
    # Reach & Awareness
    "approximateMemberReach",
    "opens",
    "companyPageClicks",
]

# One row per sponsored share per campaign per day.
ANALYTICS_PIVOTS = ["SHARE", "CAMPAIGN"]
