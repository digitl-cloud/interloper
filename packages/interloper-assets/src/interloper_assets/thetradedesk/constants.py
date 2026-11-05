BASE_URL = "https://api.thetradedesk.com/v3"
GRAPHQL_URL = "https://desk.thetradedesk.com/graphql"

# MyReports system template "Global Standard Adgroup" — the ad_groups_stats
# schema is bound to this template's columns.
ADGROUPS_REPORT_TEMPLATE_ID = "168005"

# Reports are requested in UTC, the zone The Trade Desk paces and bills in
# whatever a campaign's own time zone, so every report day is a UTC day.
REPORT_TIMEZONE = "UTC"

GRAPHQL_PAGE_SIZE = 100
ADVERTISER_CONCURRENCY = 4

# The entity fields are those The Trade Desk's REST-to-GraphQL migration
# references map from the legacy REST entities (GET /v3/advertiser/{id},
# GET /v3/campaign/{id}, POST /v3/adgroup/query/advertiser): every scalar the
# REST entity carried, and each single nested object by its key. Collections
# (bid lists, flights, custom labels) and the rows the references mark
# "Not currently available in GraphQL" are not requested.
ADVERTISER_FIELDS = """
    id
    name
    description
    currency { id }
    isArchived
    domainAddress
    industryCategory { id name }
    country { shortName }
    attributionClickLookbackWindowInSeconds
    attributionImpressionLookbackWindowInSeconds
    clickDedupeWindowInSeconds
    conversionDeDupeWindowInSeconds
    ignoreReferralUrlInConversion
    logoUrl
    vettingStatus
    nameDsa
    payerNameDsa
"""
CAMPAIGN_FIELDS = """
    id
    name
    description
    advertiser { id }
    status
    isArchived
    createdAtUtc
    startDate
    endDate
    timeZone
    budget { total totalInImpressions }
    impressionsOnlyBudgetingCPM
    minimumAdGroupSpendInAdvertiserCurrency
    pacingMode
    primaryGoal { type target }
    secondaryGoal { type target }
    tertiaryGoal { type target }
    channelType
    type
"""
AD_GROUP_FIELDS = """
    id
    name
    description
    campaign { id }
    isEnabled
    status
    channel
    funnelLocation
    industryCategoryId
    isProgrammaticGuaranteed
    budgetInAdvertiserCurrency
    budgetInImpressions
    createdAt
    lastUpdated
"""

ADVERTISER_IDS_QUERY = """
query AdvertiserIds($partnerId: ID!, $first: Int!, $after: String) {
  partner(id: $partnerId) {
    advertisers(first: $first, after: $after) {
      nodes { id }
      pageInfo { hasNextPage endCursor }
    }
  }
}
"""
ADVERTISERS_QUERY = f"""
query Advertisers($partnerId: ID!, $first: Int!, $after: String) {{
  partner(id: $partnerId) {{
    advertisers(first: $first, after: $after) {{
      nodes {{ {ADVERTISER_FIELDS} }}
      pageInfo {{ hasNextPage endCursor }}
    }}
  }}
}}
"""
CAMPAIGNS_QUERY = f"""
query Campaigns($advertiserId: ID!, $first: Int!, $after: String) {{
  advertiser(id: $advertiserId) {{
    campaigns(first: $first, after: $after) {{
      nodes {{ {CAMPAIGN_FIELDS} }}
      pageInfo {{ hasNextPage endCursor }}
    }}
  }}
}}
"""
AD_GROUPS_QUERY = f"""
query AdGroups($advertiserId: ID!, $first: Int!, $after: String) {{
  advertiser(id: $advertiserId) {{
    adGroups(first: $first, after: $after, where: {{ isTemplate: {{ eq: false }} }}) {{
      nodes {{ {AD_GROUP_FIELDS} }}
      pageInfo {{ hasNextPage endCursor }}
    }}
  }}
}}
"""
