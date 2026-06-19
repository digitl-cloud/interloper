# Asset naming

How assets in `interloper-assets` are named, and how those names map to the
upstream [`digitlcloud-connectors`](https://github.com/digitl-cloud/digitlcloud-connectors/tree/main/digitlcloud/connectors)
that we are migrating from.

## Convention

Every asset name reflects **what the asset is**, decided by its actual row grain
(not by the upstream report name). Three categories:

| Category | Row grain | Name | Tag |
| --- | --- | --- | --- |
| **Entity** | one row per object (dimension snapshot) | bare plural noun | `Entity` |
| **Aggregated report** | metrics over date / dimension | `<base>_stats` (+ `_by_<dim>`) | `Report` |
| **Event / fact** | one row per event or record | bare plural noun | `Report` (partitioned) |

Schema classes follow the asset name (class = `PascalCase`, file = asset name).

### Exceptions

- **`amazon_ads`** keeps its existing report names (no `_stats` suffix) — pinned
  deliberately, so destination table names stay stable.
- **`demo`** is a test-fixture DAG, not a connector.

## Comparison legend

Used in the tables below to map `digitlcloud-connectors` → interloper:

| Symbol | Meaning |
| --- | --- |
| ✅ | renamed per the convention |
| ⟳ | unchanged |
| ➕ | interloper-only (no connector equivalent) |
| ➖ | connector-only (absent in interloper) |

> The 19 stub sources whose assets only returned `fake_data` have been reduced to
> empty source classes pending real implementation. Their interloper columns below
> show the **convention names to use when reimplementing**; the assets are not
> currently present in code.

## Implemented sources

### adservice

| digitlcloud | interloper | |
| --- | --- | --- |
| campaigns | campaigns_stats | ✅ |
| campaigns_by_browser | campaigns_stats_by_browser | ✅ |
| campaigns_by_city | campaigns_stats_by_city | ✅ |
| campaigns_by_device_type | campaigns_stats_by_device_type | ✅ |
| conversions | conversions | ⟳ event |
| conversions_by_time_of_day | conversions_stats_by_time_of_day | ✅ |

### adup

| digitlcloud | interloper | |
| --- | --- | --- |
| ads | ads_stats | ✅ |
| — | account | ➕ |

### amazon_ads (names pinned — no `_stats`)

| digitlcloud | interloper | |
| --- | --- | --- |
| products_* / display_* / brands_* (20) | identical (20) | ⟳ |
| — | profiles | ➕ |
| — | television_campaigns | ➕ |
| — | television_targeting | ➕ |

### awin

| digitlcloud | interloper | |
| --- | --- | --- |
| advertiser_transactions | transactions | ✅ event (prefix dropped) |
| advertiser_by_publishers | publishers_stats | ✅ (prefix dropped) |

### bing_ads

| digitlcloud | interloper | |
| --- | --- | --- |
| ads | ads_stats | ✅ |

### criteo (connector dir `criteo_marketing`)

| digitlcloud | interloper | |
| --- | --- | --- |
| ads | ads_stats | ✅ |
| campaigns | campaigns_stats | ✅ |

### facebook_ads

| digitlcloud | interloper | |
| --- | --- | --- |
| ads_metadata | ads | ✅ entity |
| campaigns_metadata | campaigns | ✅ entity |
| custom_audiences | custom_audiences | ⟳ entity |
| ads | ads_stats | ✅ |
| ads_by_age_gender | ads_stats_by_age_gender | ✅ |
| ads_by_country | ads_stats_by_country | ✅ |
| campaigns | campaigns_stats | ✅ |
| videos | videos_stats | ✅ |

### impact

| digitlcloud | interloper | |
| --- | --- | --- |
| actions | actions | ⟳ event |
| action_updates | action_updates | ⟳ event |
| action_inquiries | action_inquiries | ⟳ event |
| actions_by_sku | actions_by_sku | ⟳ event |
| clicks | clicks | ⟳ event |
| performance | performance_stats | ✅ |
| performance_by_ad | performance_stats_by_ad | ✅ |
| performance_by_domain | performance_stats_by_domain | ✅ |
| performance_by_io | performance_stats_by_io | ✅ |
| performance_by_shared_id | performance_stats_by_shared_id | ✅ |

### snapchat_ads

| digitlcloud | interloper | |
| --- | --- | --- |
| ad_account_metadata | ad_account | ✅ entity |
| ad_accounts_metadata | ad_accounts | ✅ entity |
| ad_squads_metadata | ad_squads | ✅ entity |
| ads_metadata | ads | ✅ entity |
| campaigns_metadata | campaigns | ✅ entity |
| ads | ads_stats | ✅ |
| ads_by_country | ads_stats_by_country | ✅ |
| campaigns | campaigns_stats | ✅ |
| videos_by_os | videos_stats_by_os | ✅ |

### tiktok_ads

| digitlcloud | interloper | |
| --- | --- | --- |
| ads_metadata | ads | ✅ entity |
| advertisers_metadata | advertisers | ✅ entity |
| campaigns_metadata | campaigns | ✅ entity |
| ads | ads_stats | ✅ |
| ads_by_age_gender | ads_stats_by_age_gender | ✅ |
| ads_by_country | ads_stats_by_country | ✅ |
| ads_by_platform | ads_stats_by_platform | ✅ |
| videos_by_platform | videos_stats_by_platform | ✅ |

## Gutted sources (clean mapping)

These map cleanly to the connector; names shown are the convention targets for
reimplementation.

### campaign_manager_360

| digitlcloud | interloper | |
| --- | --- | --- |
| ads | ads_stats | ✅ |
| campaigns | campaigns_stats | ✅ |
| reach | reach_stats | ✅ |
| custom_audiences | custom_audiences | ⟳ entity |

### double_click_bid_manager

| digitlcloud | interloper | |
| --- | --- | --- |
| lineitems | line_items_stats | ✅ |
| lineitems_by_country | line_items_stats_by_country | ✅ |

### facebook_insights

| digitlcloud | interloper | |
| --- | --- | --- |
| page | page_stats | ✅ |
| post | post_stats | ✅ |

### google_ads

| digitlcloud | interloper | |
| --- | --- | --- |
| ads | ads_stats | ✅ |
| campaigns | campaigns_stats | ✅ |

### linkedin_ads

| digitlcloud | interloper | |
| --- | --- | --- |
| ad_accounts_metadata | ad_accounts | ✅ entity |
| ad_campaign_groups_metadata | ad_campaign_groups | ✅ entity |
| ad_campaigns_metadata | ad_campaigns | ✅ entity |
| ads | ads_stats | ✅ |

### pinterest_ads

| digitlcloud | interloper | |
| --- | --- | --- |
| ad_accounts_metadata | ad_accounts | ✅ entity |
| ad_groups_metadata | ad_groups | ✅ entity |
| ads_metadata | ads | ✅ entity |
| campaigns_metadata | campaigns | ✅ entity |
| ads | ads_stats | ✅ |
| campaigns | campaigns_stats | ✅ |
| ads_conversions | ads_conversions_stats | ✅ |
| videos_by_device | videos_stats_by_device | ✅ |

### search_console

| digitlcloud | interloper | |
| --- | --- | --- |
| page | page_stats | ✅ |
| search_appearance | search_appearance_stats | ✅ |
| site | site_stats | ✅ |
| site_by_country_device | site_stats_by_country_device | ✅ |
| site_by_country_page | site_stats_by_country_page | ✅ |

### teads

| digitlcloud | interloper | |
| --- | --- | --- |
| campaigns | campaigns_stats | ✅ |

### usercentrics (stub schemas — columns not yet defined)

| digitlcloud | interloper | |
| --- | --- | --- |
| granular | granular_stats | ✅ |
| interaction | interaction_stats | ✅ |

## Gutted sources that diverged from the connector

These stubs never faithfully mirrored the connector. Reconcile against the
connector when reimplementing.

### amazon_selling_partner (entirely different asset sets)

| digitlcloud | interloper | |
| --- | --- | --- |
| asin_lookup | — | ➖ |
| geo_sales_by_country | — | ➖ |
| product_fulfillment_metrics | — | ➖ |
| vendor_forecasting_retail | — | ➖ |
| vendor_inventory_retail_manufacturing | — | ➖ |
| vendor_inventory_retail_sourcing | — | ➖ |
| vendor_net_pure_product_margin | — | ➖ |
| vendor_sales_business_manufacturing | — | ➖ |
| vendor_sales_business_sourcing | — | ➖ |
| vendor_sales_retail_manufacturing | — | ➖ |
| vendor_sales_retail_sourcing | — | ➖ |
| vendor_traffic | — | ➖ |
| — | orders | ➕ |
| — | settlements | ➕ |

### brandwatch

| digitlcloud | interloper | |
| --- | --- | --- |
| facebook | — | ➖ |
| instagram | — | ➖ |
| linkedin | — | ➖ |
| twitter | — | ➖ |
| youtube | — | ➖ |
| — | channel_stats (stub) | ➕ |

### display_video_360

| digitlcloud | interloper | |
| --- | --- | --- |
| partners_metadata | partners | ✅ entity |
| audiences_metadata | audiences | ✅ entity |
| custom_audiences | — | ➖ |
| — | audience_composition (dup of audiences) | ➕ |
| — | insertion_orders | ➕ |
| — | line_items | ➕ |

### instagram_insights

| digitlcloud | interloper | |
| --- | --- | --- |
| insights | account_stats | ✅ (renamed) |
| engagement | engagement_stats | ✅ |
| insights_by_country | demographics_stats_by_country | ✅ (renamed) |
| insights_by_city | demographics_stats_by_city | ✅ |
| insights_by_age_gender | demographics_stats_by_age_gender | ✅ |
| profiles | profiles | ⟳ entity |
| media | media | ⟳ entity |
| organic_media | — | ➖ |

### linkedin_organic

| digitlcloud | interloper | |
| --- | --- | --- |
| page | page_stats | ✅ |
| posts | — | ➖ |
| followers | follower_stats | ✅ |
| follower_industry_taxonomy_metadata | — | ➖ |
| follower_job_function_taxonomy_metadata | — | ➖ |
| follower_seniority_taxonomy_metadata | — | ➖ |
| — | share_stats | ➕ |

### search_ads_360

| digitlcloud | interloper | |
| --- | --- | --- |
| campaigns | campaigns_stats | ✅ |
| customer_clients_metadata | — | ➖ |
| — | ad_groups_stats | ➕ |
| — | ads_stats | ➕ |
| — | conversions_stats | ➕ |

### thetradedesk

| digitlcloud | interloper | |
| --- | --- | --- |
| adgroups | ad_groups_stats | ✅ |
| — | campaigns_stats | ➕ |

## Interloper-only sources (no connector equivalent)

| Source | Assets | Notes |
| --- | --- | --- |
| demo | a, b, c, d, e, demo_asset | test fixture DAG |
| campaign_matcher | campaign_matcher, performance_analysis | derived analysis |
| campaign_performance_analysis | campaign_matcher, performance_analysis | derived analysis |
| marketing_mix_modeling | channels, spendings, model | derived analysis |
