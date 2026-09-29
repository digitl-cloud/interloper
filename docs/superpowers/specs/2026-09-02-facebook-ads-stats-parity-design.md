# Facebook Ads: align `AdsStats` with `CampaignsStats`

Date: 2026-09-02
Package: `interloper-assets` (`interloper_assets.facebook_ads`)

## Problem

Both Facebook stats assets request the full `actions`, `action_values`, `cost_per_action_type`,
`cost_per_unique_action_type` and `unique_actions` arrays from the Meta Insights API. The
`FacebookActionsNormalizer` pivots them into one column per action type, and the asset schema then
acts as an allowlist: the `RECONCILE` materialization drops every undeclared column with only a
warning.

`CampaignsStats` declares 193 columns, `AdsStats` only 75. The 124 campaign-only columns are almost
entirely action-type breakdowns (purchase, add_to_cart, omni_*, onsite_web_*, pixel events). At ad
level those metrics are fetched and silently discarded. On Swarovski's main account alone, three
months of data hold 212,810 rows with `actions_offsite_conversion_fb_pixel_add_to_cart` and
89,675 rows with `actions_omni_purchase`, none of which reach the interloper tables.

The difference is an artifact of how the two schemas were sampled in April 2024, not a quota,
size or API constraint: the API request is identical for both grains, the previous stack stored
every action type at ad level anyway (210 live columns), and the extra sparse columns cost about
8 percent per row in BigQuery.

Both schemas also drop three scalars their assets request: `attribution_setting`,
`quality_ranking` and `converted_product_quantity`.

## Change

### `schemas/ads_stats.py`

- Add every field of `CampaignsStats` whose name starts with one of the five pivot families
  `actions_`, `action_values_`, `cost_per_action_type_`, `cost_per_unique_action_type_`,
  `unique_actions_` and that `AdsStats` does not declare yet (122 fields). Types and descriptions
  are copied verbatim from `CampaignsStats`: counts are `int | None`, values and costs are
  `float | None`, every field has `default=None`.
- Add `attribution_setting: str | None`, `quality_ranking: str | None` and
  `converted_product_quantity: float | None`.
- Keep the file in alphabetical field order (both files are alphabetical today). Result: 200 fields.

Excluded on purpose: `instant_experience_clicks_to_open` and `instant_experience_clicks_to_start`
are requested by the campaigns asset only and would be dead columns on ads.

### `schemas/campaigns_stats.py`

- Add the same three scalars: `attribution_setting`, `quality_ranking`, `converted_product_quantity`.
  The campaigns asset requests them and drops them today.

Nothing else changes: no source, constants or normalizer edits. The pivot output already contains
the new columns whenever an account produces those action types.

### Out of scope

- `account_id` is `str` on `AdsStats` and `int` on `CampaignsStats`. Changing a column type on an
  existing table is breaking and needs its own decision.
- Other campaign-only requested-but-undeclared fields (`conversion_rate_ranking`,
  `engagement_rate_ranking`, `full_view_impressions`, `full_view_reach`, `dda_results`,
  `conversion_values`, `instant_experience_outbound_clicks`,
  `qualifying_question_qualify_answer_rate`).
- Per-account custom conversions (`offsite_conversion_custom_<id>`) cannot live in a shared
  catalog schema. Capturing them (for example as a raw repeated `actions` column) is a separate
  design.

## Tests (`tests/test_facebook_ads.py`)

Add to the existing schema tests:

1. **Parity**: every `CampaignsStats` field in the five pivot families is declared on `AdsStats`
   with the same annotation.
2. **Completeness**: every entry of `constants.ADS_INSIGHT_FIELDS` that is not a pivot column
   (`source.PIVOT_COLUMNS`), plus the three platform breakdown columns (`publisher_platform`,
   `platform_position`, `impression_device`), is declared on `AdsStats`. This guards against adding
   a requested field without declaring it.

## Release dependency

The change is additive and safe for tables interloper creates from scratch. For tables that
already exist (for example the Swarovski `facebook_ads.ads_stats__*` tables), the BigQuery
destination cannot add columns today: the load job carries the DataFrame's columns as its schema
with `WRITE_APPEND` and no `schema_update_options`, so the first load containing a new action-type
column is rejected. Releasing this schema change must wait for the destination to allow field
additions (planned as a follow-up, scoped to the `reconcile` materialization strategy).

## Housekeeping

Rename the worktree branch from `claude/bigquery-migration-analysis-97b0cf` to
`feat/facebook-ads-stats-parity` (branch only; the worktree directory is not moved).
