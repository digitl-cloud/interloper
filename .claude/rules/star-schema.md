# Star schemas

How a platform's star schema is written in `interloper-assets`. Naming of the raw assets it reads is in [AGENTS.md](../../AGENTS.md#asset-naming) and their contents in [assets.md](assets.md); these rules govern the views built over them. They are binding for all produced code.

A star schema is the platform's **public contract**: Looker, LEA, the cross-platform sources and client queries bind to its view names and columns. The views themselves are cheap to change (recreated on every run, no stored data, no backfill); their consumers are not. So the names below are fixed from the first implementation: a view gains a column when its platform starts filling one, and never renames, retypes or drops one. The internals behind it are free to evolve.

## 1. Every platform publishes the same views

A platform's star schema source (`<platform>/star_schema/`, key `<platform>_star_schema`, `dataset=<platform>`) publishes:

- **`fact_campaign_performance`** — required. One row per campaign and day.
- **`dim_accounts`** — required. One row per account.
- **`dim_campaigns`** — required. One row per campaign.
- **`dim_ad_groups`** — required. One row per ad group, whatever the vendor calls the level between campaign and ad (Facebook's ad set, Snapchat's ad squad, Criteo's ad set). Every platform has one.
- **`dim_ads`** — where the platform has ads. One row per ad.
- **`fact_<grain>_performance`** — optional, at most one, at the platform's native finer grain (`fact_ad_performance` on Facebook with its placement breakdown, `fact_ad_group_performance` on The Trade Desk). It serves platform-internal analysis only.

Cross-platform sources (`campaign/`) bind `*.fact_campaign_performance`, `*.dim_campaigns` and `*.dim_accounts`, never the finer fact, and read columns by the names below. They stamp the platform themselves, from the upstream's source key; a platform's views carry no `platform` column. A column one platform does not publish is absent from its view, so the cross-platform source that needs it states in its own code what stands in for it on that platform (`NULL`, or another measure), per upstream. A consumer that guesses a column name (`_first(row, "campaign_name", "name", ...)`) is a contract violation, not a convenience.

## 2. Each platform publishes what it fills, under one vocabulary

A view publishes the columns its platform fills, under exactly the names and types below and in their order, then any number of **platform extras**. Views of the same name therefore differ by platform: a column the platform cannot fill is **omitted**, never published as a column of `NULL`s. Facebook's campaign fact has no `conversions`, Bing's no `purchases`, Criteo's campaign dimension no `status`.

What the platform fills is decided by the implementation, not by what is bound: a column one of the view's tiers fills (rule 5) is published even while that tier's raw asset is unbound or not yet collected, and is `NULL` on the rows the lower tiers build, as its description says. A column no tier fills is omitted until one does, which adds it to the view. The keys (`date`, `account_id`, `campaign_id` on the campaign fact; each dimension's own id, its parents' ids and `as_of_date`) are always published.

`tests/test_catalog_conventions.py` checks the names, types and order over every star schema.

`fact_campaign_performance`:

| column | type | meaning |
|---|---|---|
| `date` | `dt.date` | the report day as the vendor reports it, in the account's timezone; the time key |
| `account_id` | `str` | the account, see rule 4 |
| `campaign_id` | `str` | the campaign |
| `impressions` | `int` | times the ads were shown |
| `clicks` | `int` | clicks on the ads |
| `cost` | `float` | amount spent, in `currency` |
| `currency` | `str` | ISO code of every amount in the row |
| `conversions` | `int` | the platform's own primary conversion metric, vendor-defined; the description names the vendor metric and its attribution window |
| `conversion_value` | `float` | the value the platform attributes to `conversions`, in `currency` |
| `purchases` | `int` | purchases, where the platform reports purchases as such; same window as the description states |
| `revenue` | `float` | the value of `purchases`, in `currency` |

`conversions` and `purchases` are different columns because they mean different things: a client comparing platforms reads `purchases` and `revenue`, which mean the same everywhere they are published, and `conversions` only within one platform. A platform with no single primary conversion metric (Facebook, whose results depend on the objective) publishes `purchases` and no `conversions`; one that does not say which conversions are purchases (Bing) publishes `conversions` and no `purchases`.

`dim_accounts`: `account_id`, `account_name`, `currency`, `timezone` (IANA name the vendor reports the account's days in), `as_of_date`.

`dim_campaigns`: `campaign_id`, `account_id`, `campaign_name`, `status`, `objective`, `campaign_type`, `budget`, `budget_type`, `start_date`, `end_date`, `created_at`, `updated_at`, `as_of_date`.

- `status` is the **configured** status, in the vendor's vocabulary; a platform that also reports an effective status carries it as the extra `effective_status`.
- `objective`, `campaign_type` and `budget_type` hold the vendor's vocabulary. `campaign_type` is the platform's own sub-product of a campaign (Amazon's ad product, Bing's campaign type, Facebook's buying type); where it also qualifies the fact's grain it is the fact's degenerate dimension under the same name.
- `budget` is in the account's currency, as the platform reports it; a vendor that reports minor units (Facebook) publishes no `budget` until the conversion is implemented with the currency's own offset, never divided by a constant.
- `start_date` and `end_date` are the campaign's **scheduled** dates as the platform holds them, in the account's timezone (Facebook's `start_time`/`stop_time`, Snapchat's `start_time`/`end_time`). A platform that schedules at another level (TikTok, Criteo and Bing schedule the ad group) publishes none on its campaigns. They are never derived from the days the facts show activity; a first or last active day is a different fact and gets its own name if ever needed. A vendor that reports the schedule as a local wall-clock timestamp (`2026-10-01T00:00:00+0200`) gives the date by taking the timestamp's date part, never by converting through UTC, and an empty string is `NULL`. A vendor that reports it as a UTC instant (Snapchat) gives the date with `interloper_sql.local_date(instant, timezone)` and the account's timezone from `dim_accounts`, never by casting the instant to a date: 22:00 UTC on 30 September is 1 October in Berlin. Without the account's timezone the date is `NULL`.
- `created_at` and `updated_at` are the vendor's instants (`dt.datetime`), known from entity rows only.

`dim_ad_groups`: `ad_group_id`, `campaign_id`, `account_id`, `ad_group_name`, `status`, `budget`, `budget_type`, `start_date`, `end_date`, `created_at`, `updated_at`, `as_of_date`, with the same meanings.

`dim_ads`: `ad_id`, `ad_group_id`, `campaign_id`, `account_id`, `ad_name`, `status`, `created_at`, `updated_at`, `as_of_date`.

`cost` is the house word for spend, on every platform, whatever the vendor calls it. Counts are `int`, amounts and rates `float`, days `dt.date`, ids `str`, as in the raw layer.

Platform extras keep the vendor's word where the raw layer has one (`ad_product`, `objective`, `marketplace_id`, `publisher_platform`) and go after the shared columns. An extra that two platforms share by meaning gets one name on both; check the other star schemas before adding one.

## 3. Facts are lean, dimensions carry the attributes

A fact row is keys, degenerate dimensions and measures. Names, statuses, budgets and every other attribute live in the dimensions, so a fact never duplicates a dimension's column and a consumer never has to choose between "the name as reported that day" and "the latest name".

A **degenerate dimension** is a column that qualifies the row's grain and has no entity of its own: Amazon's `campaign_type` (its ad product), Facebook's `publisher_platform`, `platform_position`, `impression_device`. It stays in the fact.

**The fact's key is stated and unique.** `fact_campaign_performance` is unique on `date`, `account_id`, `campaign_id` plus its degenerate dimensions (`campaign_type` on Amazon); the finer fact on its own grain's keys plus its degenerate dimensions. The source docstring names the key, and a test asserts it. Vendor rows finer than the key are summed inside the fact (Sponsored Products reports one row per ad group; the campaign fact sums them).

## 4. Keys

- `account_id` is the source's **discriminator** on every platform, whatever the vendor calls it (Amazon's `profile_id`, TikTok's `advertiser_id`). It is stamped from the upstream's source, not read from the rows, because vendor reports rarely carry it.
- `campaign_id`, `ad_group_id` and `ad_id` are the vendor's ids, cast to `str`.
- A dimension has exactly one row per key. Its key is the join column the facts carry, under the same name.
- A row whose key the vendor left NULL identifies nothing and joins nothing, so no view publishes it: `interloper_sql.latest` drops it from every dimension, and each fact filters its grain's id (`campaign_id`, `ad_id`) right after the union, before anything is summed. Facebook reports placeholder campaign rows without a campaign, Amazon lists archived Sponsored Brands ads without an ad id.

## 5. Dimensions are built from raw assets, in tiers, deterministically

A dimension's attributes come from the raw assets, never from a fact: the entity snapshot first (`campaigns`, `ads`, `profiles`), the report that carries the attribute second (`campaigns_stats`). The fact is the last tier and contributes only what it carries, keys and `currency`, so every key a fact carries resolves even when the dimension's own bindings lag behind the fact's. Columns only an entity row knows are `NULL` on rows built from a report or a fact, and their descriptions say so.

The tiers are ranked with `interloper_sql.latest(rows, key, priority asc, as_of_date desc)`. For that ranking to be total, **each tier holds one row per key and day before the union**: an entity snapshot is one by construction, a report tier aggregates over its finer grain (`max` of each attribute per key and day), the fact tier aggregates its keys the same way, and an entity tier filters to the source's own account where the vendor returns every account the credentials see (Amazon's `/v2/profiles`). A tie left in a tier is a non-deterministic view: the row that wins changes between two SELECTs.

Dimensions are type 1: the latest row wins. `as_of_date` says which day it reflects.

## 6. `as_of_date`, not `date`, on a dimension

`date` is the fact's time key: one row per day, the column every query filters and the partition every job targets. A dimension's day is something else, the day the winning row was observed. Giving it the same name makes a fact-to-dimension join carry two `date` columns (BigQuery rejects `SELECT *` over them, Looker suffixes one silently) and invites filtering a dimension by day as if it were partitioned. `as_of_date` names what it is: the snapshot day the row reflects, not when its attributes became true. A campaign's own schedule is `start_date` and `end_date` (rule 2); attribute history, if a client ever needs it, is a separate view at version grain with `valid_from` and `valid_to`, derived from the daily entity snapshots, never a change to the type 1 dimension.

## 7. Relations

- Every upstream relation is `many=True, optional=True`: the star schema is a view over whatever is bound, and a platform with no bound account produces an empty fact, not a failed one. A fact with nothing bound raises `ConfigError("no upstream bound: ...")` from `union()`; a dimension with no entity tier falls through to the lower tiers.
- Relation names are the raw asset's key (`campaigns_stats`, `profiles`, `ads`). A relation over several keys lists them (`["amazon_ads.products_campaigns_stats", ...]`).
- Bindings are per asset instance and manual: an account added to the organisation is absent from the views until bound. Say so in the source docstring.

## 8. Tests

Each star schema has DuckDB tests in `tests/<platform>/star_schema/test_source.py` that seed two accounts and assert, at least: the fact's grain (finer vendor rows summed to one row per key and day, and no two rows share the stated key), the columns the platform omits, the tier preference (entity beats report beats fact), tie determinism (two tied candidates with different values yield the same row on every evaluation), that a raw row without a key reaches no view, the empty-relation behaviour, and that a missing upstream table fails with the table name.
