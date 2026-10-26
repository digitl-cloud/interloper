# Star Schema Contract Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Re-cut the two implemented star schemas (Facebook Ads, Amazon Ads) to the contract in `.claude/rules/star-schema.md`, so every platform's views share names, grain, keys and build rules before the five remaining shells are implemented.

**Architecture:** Nothing changes in the framework. Each star schema stays a source of `ibis_view`s over the platform's raw tables, in the platform's dataset. The views are renamed and reshaped: one campaign-grain fact per platform with the core columns, lean facts, four dimensions (accounts, campaigns, ad groups, ads) built from raw assets in tiers that each hold one row per key and day, `as_of_date` as the dimension's snapshot day. The views are recreated on every run, so no stored data changes; only Swarovski's Amazon views are live and nothing reads them yet.

**Tech Stack:** Python 3.10+, Ibis via `interloper_sql` (`ibis_view`, `union`, `latest`), DuckDB in tests, ruff, ty, pytest.

**Spec:** `.claude/rules/star-schema.md` (the contract), with `.claude/rules/assets.md` for the raw layer the views read.

## Global Constraints

- The star schema source stays `<platform>/star_schema/` with key `<platform>_star_schema`, `group="<platform>"`, `dataset="<platform>"`.
- Core columns, in this order, first in every schema class and in every view's select, extras after (rule 2):
  - fact: `date`, `account_id`, `campaign_id`, `impressions` (int), `clicks` (int), `cost` (float), `currency` (str), `conversions` (int), `conversion_value` (float), `purchases` (int), `revenue` (float)
  - `dim_accounts`: `account_id`, `account_name`, `currency`, `timezone`, `as_of_date`
  - `dim_campaigns`: `campaign_id`, `account_id`, `campaign_name`, `status`, `objective`, `campaign_type`, `budget` (float), `budget_type`, `start_date`, `end_date`, `created_at` (datetime), `updated_at` (datetime), `as_of_date`
  - `dim_ad_groups`: `ad_group_id`, `campaign_id`, `account_id`, `ad_group_name`, `status`, `budget`, `budget_type`, `start_date`, `end_date`, `created_at`, `updated_at`, `as_of_date`
  - `dim_ads`: `ad_id`, `ad_group_id`, `campaign_id`, `account_id`, `ad_name`, `status`, `created_at`, `updated_at`, `as_of_date`
- `cost` is the house word; no view carries `spend`. `status` is the configured status; Facebook's effective status is the extra `effective_status`.
- `account_id` is stamped from the upstream's source discriminator on every view, never read from the rows. Every id is cast to `str`.
- Every tier holds one row per key and day before the union; the fact tier contributes keys and `currency` only. Each fact's key is unique and named in the source docstring.
- Facebook `budget` is `NULL` (minor units, no constant divisor); `budget_type` is `daily` when `daily_budget` is set, else `lifetime` when `lifetime_budget` is set, else `NULL`.
- Schema classes: one per file named after the view, class in `PascalCase`, every field `Field(default=None, description=...)`; the description of `conversions` and `purchases` names the vendor metric and its attribution window.
- Component docstrings are product copy (summary plus prose, no `Args:`/`Returns:`), per `python-style.md` rule 7.
- Tests mirror the source: `tests/<platform>/star_schema/test_source.py`, DuckDB via `pytest.importorskip("interloper_duckdb")`.
- Each commit is a valid Conventional Commit; squash as you go. No attribution lines.

## Review Focus

1. A Facebook campaign whose `start_time` or `stop_time` is `NULL` or an empty string: the dimension row carries `NULL`, the view does not fail. Pinned in Task 2 (`test_schedule_dates_come_from_the_local_timestamp_and_blank_is_null`).
2. Facebook instants come as `2026-10-01T00:00:00+0200` (offset without a colon). `created_at`/`updated_at` must parse on DuckDB and BigQuery; if `.cast("timestamp")` refuses the form on DuckDB, insert the colon with `.re_replace(r"([+-]\d{2})(\d{2})$", r"\1:\2")` before the cast. Pinned in Task 2 (`test_entity_instants_parse_with_the_vendors_offset`).
3. A campaign id stored as `BIGINT` in one raw table and `VARCHAR` in another: every tier casts to string before the union. Pinned in Task 2 (`campaigns_stats.account_id` seeded as `BIGINT`) and Task 3 (`BIGINT` campaign and ad group ids).
4. Two report rows tied on key and day, one with a `NULL` attribute: the collapsed tier keeps the value and the view gives the same row on every evaluation. Pinned in Task 3 (`test_tied_report_rows_collapse_deterministically`).
5. A key the fact carries that no bound entity or report knows: the dimension still has a row for it, from the fact tier. Pinned in Task 2 (`test_a_campaign_known_only_from_the_facts_comes_from_them`).

---

### Task 1: Commit the contract

**Files:**
- Create: `.claude/rules/star-schema.md` (already written, uncommitted)
- Modify: `AGENTS.md` (the star schema bullet's pointer, already edited, uncommitted)
- Modify: `docs/guide/dependencies.md:146`

**Interfaces:**
- Produces: the rule every later task argues from.

- [ ] **Step 1: Replace the stale sentence in `docs/guide/dependencies.md`**

Replace the sentence beginning "The star schemas are placeholders for now: they declare their wiring but materialize no" with: "Facebook Ads and Amazon Ads materialize their star schemas as views; the other platforms declare the same wiring and materialize nothing yet."

- [ ] **Step 2: Verify the docs build**

Run: `uv run zensical build --strict` (what the docs workflow runs)
Expected: exit 0.

- [ ] **Step 3: Commit**

```bash
git add .claude/rules/star-schema.md AGENTS.md docs/guide/dependencies.md docs/superpowers/plans/2026-10-26-star-schema-contract.md
git commit -m "docs: the star schema contract every platform publishes"
```

---

### Task 2: Facebook Ads star schema on the contract

**Files:**
- Create: `packages/interloper-assets/src/interloper_assets/facebook_ads/star_schema/schemas/fact_campaign_performance.py`
- Create: `packages/interloper-assets/src/interloper_assets/facebook_ads/star_schema/schemas/dim_ad_groups.py`
- Modify: `.../facebook_ads/star_schema/schemas/{fact_ad_performance,dim_accounts,dim_campaigns,dim_ads,__init__}.py`
- Modify: `.../facebook_ads/star_schema/source.py`
- Test: `packages/interloper-assets/tests/facebook_ads/star_schema/test_source.py`

**Interfaces:**
- Consumes: `interloper_sql.union(upstreams, *columns, **stamps)`, `interloper_sql.latest(rows, key, *order)`; raw schemas `facebook_ads.connector.schemas.{campaigns_stats, ads_stats, campaigns, ads}`.
- Produces: views `fact_campaign_performance`, `fact_ad_performance`, `dim_accounts`, `dim_campaigns`, `dim_ad_groups`, `dim_ads`. The campaign matcher keeps reading `campaign_id` and `campaign_name` from `dim_campaigns`.

**Schemas** (core first, then the listed extras):

- `FactCampaignPerformance`: the eleven core columns, then `reach: int | None`. `conversions` description: "NULL: Facebook reports no single primary conversion, its results depend on the objective; see purchases." `purchases` description: "Facebook purchase actions under the account's attribution setting (7-day click, 1-day view by default)."
- `FactAdPerformance`: `date`, `account_id`, `campaign_id`, `ad_group_id`, `ad_id`, `publisher_platform`, `platform_position`, `impression_device`, `impressions`, `clicks`, `reach`, `cost`, `currency`, `conversions`, `conversion_value`, `purchases`, `revenue`. No names.
- `DimAccounts`: the five core columns. `timezone` description: "NULL until an ad accounts entity is collected."
- `DimCampaigns`: the thirteen core columns, then `effective_status`.
- `DimAdGroups`: the twelve core columns. Everything but ids, name and `as_of_date` is "NULL until an ad sets entity is collected."
- `DimAds`: the nine core columns, then `effective_status`.

**Views** (`source.py`). A module-level `_account(upstream: il.Upstream) -> str` returns the upstream's source discriminator and raises `ConfigError` when the source has none (the shape of today's Amazon `_profile`). `_local_date(column) -> ibis.Value` is `column.nullif("").substr(0, 10).cast("date")`; `_instant(column)` is the timestamp cast with the offset fix of Review Focus 2 if needed.

- `fact_campaign_performance(self, campaigns_stats: list[il.Upstream])`, relation `campaigns_stats` over `facebook_ads.campaigns_stats`, `many=True, optional=True`. `union` the reports on `date_start`, `campaign_id`, `impressions`, `clicks`, `spend`, `account_currency`, `actions_purchase`, `action_values_purchase`, `reach` with the `account_id` stamp; select to the core names (`conversions` and `conversion_value` as typed `NULL`, `purchases` cast `int64`, `revenue` cast `float64`); group by `date`, `account_id`, `campaign_id`, sum the measures, `max(currency)`. Key: `date`, `account_id`, `campaign_id`.
- `fact_ad_performance(self, ads_stats: list[il.Upstream])`: `union` on the lean list with the stamp, rename to the contract names, same casts, no aggregation. Key: `date`, `account_id`, `ad_id`, `publisher_platform`, `platform_position`, `impression_device`.
- `dim_accounts(self, campaigns_stats: list[il.Upstream], fact_campaign_performance: il.Upstream)`: tier 2 from each report grouped by `account_id`, `date_start` with `max(account_name)`, `max(account_currency)`, `timezone` NULL; tier 3 from the fact grouped by `account_id`, `date` with `max(currency)`.
- `dim_campaigns(self, campaigns: list[il.Upstream], fact_campaign_performance: il.Upstream)`, relation `campaigns` over `["facebook_ads.campaigns", "facebook_ads.campaigns_stats"]`: tier 1 from the entity (`id`, `name`, `status`, `objective`, `campaign_type` from `buying_type`, `budget` NULL, `budget_type` per the constraint, `_local_date(start_time)`, `_local_date(stop_time)`, `_instant(created_time)`, `_instant(updated_time)`, `date`, `effective_status`); tier 2 from the report grouped by `campaign_id`, `date_start` with `max(campaign_name)`, `max(objective)`, the rest NULL; tier 3 from the fact grouped by `campaign_id`, `date`, attributes NULL.
- `dim_ad_groups(self, ads_stats: list[il.Upstream], fact_ad_performance: il.Upstream)`, relation `ads_stats`: tier 2 from each report grouped by `adset_id`, `date_start` with `max(adset_name)`, `max(campaign_id)`, the rest NULL; tier 3 from the fact grouped by `ad_group_id`, `date` with `max(campaign_id)`.
- `dim_ads(self, ads: list[il.Upstream], ads_stats: list[il.Upstream], fact_ad_performance: il.Upstream)`, relations `ads` and `ads_stats`: tier 1 from the entity (`id`, `adset_id`, `campaign_id`, `name`, `status`, instants, `date`, `effective_status`); tier 2 from the report grouped by `ad_id`, `date_start` with `max(ad_name)`, `max(adset_id)`, `max(campaign_id)`; tier 3 from the fact grouped by `ad_id`, `date` with `max(ad_group_id)`, `max(campaign_id)`.

- [ ] **Step 1: Rewrite the tests to the contract**

Keep the fixture shape (`warehouse`, `_account`, `_seed`, `_materialize`, `_rows`; `_rows` converts `date` and `as_of_date` to `dt.date`). `_seed` creates `campaigns_stats__{id}` with `account_id BIGINT`, `campaign_id VARCHAR`, `actions_purchase`, `action_values_purchase`, `reach`, two rows for `c{id}` on DAY when `two_report_rows=True`; `campaigns__{id}` with `buying_type 'AUCTION'`, `daily_budget '1000'`, `lifetime_budget NULL`, `start_time '2026-10-01T00:00:00+0200'`, `stop_time ''`, `created_time '2026-09-01T10:00:00+0200'`, `updated_time '2026-09-02T10:00:00+0200'`, `status 'ACTIVE'`, `effective_status 'ACTIVE'`, plus a second campaign with `stop_time NULL` when `two_entities=True`; `ads__{id}` and `ads_stats__{id}` as today with the purchase columns. Tests and assertions:

```python
class TestWiring:
    def test_relations_fan_in_every_account_and_are_optional(self):
        fact = FacebookAdsStarSchema.fact_campaign_performance.relations["campaigns_stats"]
        assert (fact.key, fact.many, fact.optional) == ("facebook_ads.campaigns_stats", True, True)
        assert FacebookAdsStarSchema.dim_campaigns.relations["campaigns"].key == [
            "facebook_ads.campaigns", "facebook_ads.campaigns_stats"
        ]
        assert FacebookAdsStarSchema.dim_ad_groups.relations["ads_stats"].key == "facebook_ads.ads_stats"

    def test_views_carry_the_core_columns_first(self):
        # first 11 of FactCampaignPerformance, first 13 of DimCampaigns, first 12 of DimAdGroups,
        # first 9 of DimAds, first 5 of DimAccounts, exactly as Global Constraints lists them;
        # "spend" not in FactAdPerformance.model_fields


class TestOnDuckDB:
    def test_the_campaign_fact_sums_to_one_row_per_campaign_and_day(self, warehouse): ...
        # two_report_rows: one row for ('111', 'c111', DAY); impressions 150, cost 15.0, currency 'EUR',
        # conversions None, purchases 3, revenue 50.0; account_id is the stamped '111';
        # len(rows) == len({(r['date'], r['account_id'], r['campaign_id']) for r in rows})

    def test_the_ad_fact_keeps_the_placement_grain_without_names(self, warehouse): ...
        # list(columns) == FactAdPerformance fields; cost 12.5; ad_group_id 's1'

    def test_a_campaign_takes_its_entity_over_its_report_over_the_facts(self, warehouse): ...
        # 111 with entity: campaign_name 'Camp from entity', status 'ACTIVE', objective 'OUTCOME_SALES',
        # campaign_type 'AUCTION', budget None, budget_type 'daily', effective_status 'ACTIVE'
        # 222 without: 'Camp from report', status None, budget_type None, start_date None

    def test_schedule_dates_come_from_the_local_timestamp_and_blank_is_null(self, warehouse): ...
        # start_date == dt.date(2026, 10, 1); end_date None for '' and for NULL

    def test_entity_instants_parse_with_the_vendors_offset(self, warehouse): ...
        # created_at == dt.datetime(2026, 9, 1, 8, 0, tzinfo=dt.timezone.utc) or its naive-UTC equivalent
        # as DuckDB returns it; updated_at one day later

    def test_a_newer_fact_does_not_displace_an_older_entity_row(self, warehouse): ...   # as_of_date 2026-09-30
    def test_a_campaign_known_only_from_the_facts_comes_from_them(self, warehouse): ...   # 'c999', attributes None
    def test_ad_groups_come_from_the_report_then_the_facts(self, warehouse): ...
        # [{'ad_group_id': 's1', 'campaign_id': 'c111', 'account_id': '111', 'ad_group_name': 'Set',
        #   'status': None, ..., 'as_of_date': DAY}]

    def test_an_ad_entity_wins_over_the_report(self, warehouse): ...
        # ad_name 'Ad from entity', status 'ACTIVE', ad_group_id 's1', as_of_date 2026-09-30

    def test_accounts_come_from_the_report_then_the_fact(self, warehouse): ...
        # [{'account_id': '111', 'account_name': 'Acct 111', 'currency': 'EUR', 'timezone': None, 'as_of_date': DAY}]

    def test_a_missing_account_table_fails_the_view(self, warehouse): ...   # on fact_campaign_performance
    def test_a_fact_with_no_account_bound_fails_clearly(self, warehouse): ...
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest packages/interloper-assets/tests/facebook_ads/star_schema -q`
Expected: failures on missing `fact_campaign_performance`, `dim_ad_groups`, `as_of_date`, `cost`.

- [ ] **Step 3: Write the six schema modules and the source as specified**

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest packages/interloper-assets/tests/facebook_ads/star_schema -q`
Expected: all pass.

- [ ] **Step 5: Lint and type-check**

Run: `uv run ruff check packages/interloper-assets && uv run ruff format --check packages/interloper-assets && uv run ty check packages/interloper-assets`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add packages/interloper-assets/src/interloper_assets/facebook_ads/star_schema packages/interloper-assets/tests/facebook_ads/star_schema
git commit -m "feat(assets): the Facebook Ads star schema on the contract"
```

---

### Task 3: Amazon Ads star schema on the contract

**Files:**
- Create: `.../amazon_ads/star_schema/schemas/dim_ad_groups.py`
- Modify: `.../amazon_ads/star_schema/schemas/{fact_campaign_performance,dim_campaigns,dim_accounts,__init__}.py`
- Modify: `.../amazon_ads/star_schema/source.py`
- Test: `packages/interloper-assets/tests/amazon_ads/star_schema/test_source.py`

**Interfaces:**
- Consumes: same helpers as Task 2 (each source keeps its own `_account`; no shared module yet); raw schemas `amazon_ads.connector.schemas.{products_campaigns_stats, brands_campaigns_stats, display_campaigns_stats, brands_ad_groups_stats, display_ad_groups_stats, profiles}`.
- Produces: `fact_campaign_performance`, `dim_accounts`, `dim_campaigns`, `dim_ad_groups`. Swarovski's live views are recreated with the new columns on the next run; nothing reads them yet. `AD_PRODUCTS` maps every report key, campaign and ad group, to its product.

**Schemas:**

- `FactCampaignPerformance`: the eleven core columns, then `campaign_type` (the ad product: `sponsored_products`, `sponsored_brands`, `sponsored_display`). `conversions` and `purchases` description: "Purchases attributed within 14 days of a click, as each ad product's campaign report counts them"; `conversion_value` and `revenue`: the matching sales.
- `DimCampaigns`: the thirteen core columns, no extras. `start_date`/`end_date`/`created_at`/`updated_at` descriptions: "NULL until a campaigns entity is collected; Amazon's campaign reports carry no schedule."
- `DimAdGroups`: the twelve core columns, no extras; status, budget, schedule and instants "NULL until an ad groups entity is collected."
- `DimAccounts`: the five core columns, then `account_type`, `marketplace_id`, `country_code`.

**Views:**

- `fact_campaign_performance`: same relation; per-upstream projection to `date`, `campaign_id` (cast string), `impressions`, `clicks`, `cost` (`spend` on products), `currency` (`campaign_budget_currency_code`), `conversions` (`purchases_14d` on products, `purchases` elsewhere), `conversion_value` (`sales_14d` / `sales`), `purchases` and `revenue` the same two columns, stamped `account_id` and `campaign_type`; group by `date`, `account_id`, `campaign_type`, `campaign_id`, sum the measures, `max(currency)`. Keep the `ConfigError("no upstream bound: ...")` guard. Key: `date`, `account_id`, `campaign_type`, `campaign_id`.
- `dim_campaigns(self, campaigns_stats: list[il.Upstream], fact_campaign_performance: il.Upstream)`, relation `campaigns_stats` over the three campaign report keys: tier 2 from each report grouped by `campaign_id`, `date` with `max(campaign_name)`, `max(campaign_status)` as `status`, `max(campaign_budget_amount)` as `budget`, `max(campaign_budget_type)` as `budget_type`, stamped `account_id` and `campaign_type`, the rest NULL; tier 3 from the fact grouped by `campaign_id`, `date` with `max(account_id)`, `max(campaign_type)`.
- `dim_ad_groups(self, ad_groups_stats: list[il.Upstream])`, relation `ad_groups_stats` over `["amazon_ads.products_campaigns_stats", "amazon_ads.brands_ad_groups_stats", "amazon_ads.display_ad_groups_stats"]` (Sponsored Products reports ad groups inside its campaign report): tier 2 from each report grouped by `ad_group_id`, `date` with `max(ad_group_name)`, `max(campaign_id)`, stamped `account_id`, the rest NULL. No fact tier: no Amazon fact carries `ad_group_id`.
- `dim_accounts(self, profiles: list[il.Upstream], fact_campaign_performance: il.Upstream)`: tier 1 from each `profiles` table filtered to `table["profile_id"].cast("string") == ibis.literal(_account(upstream))`, selecting `account_id` (the cast id), `account_info_name`, `currency_code`, `timezone`, `date`, `account_info_type`, `account_info_marketplace_string_id`, `country_code`; tier 2 from the fact grouped by `account_id`, `date` with `max(currency)`, the rest NULL.

- [ ] **Step 1: Rewrite the tests to the contract**

Keep the fixtures; `_seed` adds `campaign_budget_type` to the three campaign reports, stores campaign and ad group ids as `BIGINT`, seeds the Sponsored Products table with two ad-group rows for one campaign whose `campaign_name` differ (`'SP campaign'`, `'SP campaign renamed'`) and whose second row has `campaign_status NULL`, creates `brands_ad_groups_stats__{id}` (one row) and an empty `display_ad_groups_stats__{id}`, and gives `profiles__{id}` a second row for a different profile id. Tests:

```python
class TestWiring:
    def test_raw_upstreams_fan_in_every_profile_and_are_optional(self): ...
        # fact and dim_campaigns share the three campaign keys; dim_ad_groups the three ad group keys; profiles as today
    def test_views_carry_the_core_columns_first(self): ...


class TestOnDuckDB:
    def test_the_fact_unions_every_profile_and_ad_product_at_campaign_grain(self, warehouse): ...
        # keys {('1','sponsored_products'), ('1','sponsored_brands'), ('2', ...)} on (account_id, campaign_type);
        # products: campaign_id '111', impressions 150, clicks 15, cost 15.0, conversions 3, conversion_value 50.0,
        # purchases 3, revenue 50.0, currency 'EUR'; no sponsored_display row; no 'campaign_name' column;
        # rows unique on (date, account_id, campaign_type, campaign_id)

    def test_campaigns_come_from_the_reports_then_the_facts(self, warehouse): ...
        # rows['111']['campaign_type'] == 'sponsored_products'; rows['221']['status'] == 'PAUSED';
        # budget 20.0, budget_type 'DAILY'; start_date None; account_id '1'

    def test_tied_report_rows_collapse_deterministically(self, warehouse): ...
        # campaign_name == 'SP campaign renamed' (max); status == 'ENABLED' (max ignores NULL); two materializations identical

    def test_ad_groups_come_from_every_ad_product_report(self, warehouse): ...
        # the two SP ad groups and the SB ad group, each with campaign_id, account_id '1', status None

    def test_a_profile_takes_its_own_entity_row_over_the_facts(self, warehouse): ...
        # profile 1: account_name 'Acme DE', timezone 'Europe/Berlin', marketplace_id 'A1PA6795UKMFR9',
        # as_of_date 2026-09-30; the foreign profile row does not appear; profile 2: account_name None,
        # currency 'EUR', as_of_date DAY

    def test_a_missing_profile_table_fails_the_view(self, warehouse): ...
    def test_a_fact_with_no_profile_bound_fails_clearly(self, warehouse): ...
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest packages/interloper-assets/tests/amazon_ads/star_schema -q`
Expected: failures on `account_id`, `purchases`, `dim_ad_groups`, `as_of_date`.

- [ ] **Step 3: Write the four schema modules and the source as specified**

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest packages/interloper-assets/tests/amazon_ads/star_schema -q`
Expected: all pass.

- [ ] **Step 5: Lint, type-check, and run the whole assets suite**

Run: `uv run ruff check packages/interloper-assets && uv run ruff format --check packages/interloper-assets && uv run ty check packages/interloper-assets && uv run pytest packages/interloper-assets -q -n auto --maxprocesses 4`
Expected: clean, all pass (the campaign matcher tests still find `campaign_id` and `campaign_name` on `dim_campaigns`).

- [ ] **Step 6: Commit**

```bash
git add packages/interloper-assets/src/interloper_assets/amazon_ads/star_schema packages/interloper-assets/tests/amazon_ads/star_schema
git commit -m "feat(assets): the Amazon Ads star schema on the contract"
```

---

### Task 4: Whole-branch verification

- [ ] **Step 1: Run the full check suite**

Run: `make check-python`
Expected: ruff, ty and the whole suite pass.

- [ ] **Step 2: Rebase on `origin/main`, push with `--force-with-lease`, open the PR titled `feat(assets): star schemas on one contract`**

The PR body: what the contract fixes (the five views per platform, core columns, `cost`, `conversions` versus `purchases`, lean facts with stated keys, dimensions from raw in deterministic tiers, `as_of_date`, scheduled dates, ad groups), that both re-cuts are view changes with no backfill, that Swarovski's Amazon views are recreated with the new columns on the next nightly run and nothing reads them yet, and that the connector entity additions (Amazon campaigns and ad groups, Facebook ad accounts and ad sets, The Trade Desk campaigns) follow in their own PRs.
