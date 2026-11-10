BASE_URL = "https://api.linkedin.com/rest"
AUTH_BASE_URL = "https://api.linkedin.com"
LINKEDIN_VERSION = "202607"
RESTLI_PROTOCOL_VERSION = "2.0.0"

# Max page size of the cursor-paginated ``q=search`` finders.
SEARCH_PAGE_SIZE = 1000

ACCOUNT_FIELDS = [
    "id",
    "name",
    "currency",
    "status",
]

CAMPAIGN_GROUP_FIELDS = [
    "id",
    "name",
    "status",
    "runSchedule",
]

CAMPAIGN_FIELDS = [
    "id",
    "name",
    "type",
    "status",
    "objectiveType",
    "dailyBudget",
    "runSchedule",
    "costType",
    "campaignGroup",
]

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
ANALYTICS_PIVOTS = "List(SHARE,CAMPAIGN)"
