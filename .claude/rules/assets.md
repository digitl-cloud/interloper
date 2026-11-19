---
paths:
  - "packages/extensions/interloper-assets/**"
---

# Assets

How a vendor integration is written in `interloper-assets`. Naming — Entity / Report / Event, `<base>_stats`, one schema class per file — is in [AGENTS.md](../../AGENTS.md#asset-naming); these rules govern what the asset *contains*. They are binding for all produced code.

## 1. The asset mirrors the vendor's response

An asset's schema declares the response as the vendor sends it: same fields, same names, same grain. Nothing invented, nothing renamed, nothing quietly left out. Everything downstream — a client's queries, a migration from their old warehouse, the next person comparing our table to the vendor's UI — assumes the table *is* the API response, typed.

That cuts three ways, and each has bitten us:

- **Never drop a field the response carries.** The schema is an allowlist: under `RECONCILE` an undeclared column is dropped with a warning nobody reads, so the data is lost while the pipeline reports success. Facebook `AdsStats` ran for a year at 75 declared fields against a 200-field response, silently discarding every purchase and add-to-cart metric.
- **Never add a field the response does not carry.** See rule 2 for the one exception, which is narrow.
- **Never rename a vendor field.** The normalizer's casing pass is the only permitted transformation, and it is a spelling change, not a rename. Where the vendor's casing defeats the snake-caser, fix it with `column_overrides` on the source's normalizer rather than shipping the mangled name: `eCPAddToCart` is `ecp_add_to_cart`, never `e_cp_add_to_cart`.

An API that returns only the fields a request names (Facebook's Graph API) has no fixed response: the response is the field list. That list is never the API reference's full catalogue, which mixes fields any account serves with fields that need a parameter, a permission or a particular token, and one such field fails the whole request. It is derived from a maintained connector that requests it from every client's accounts: Facebook's entity fields are Airbyte's certified connector's at a pinned commit, with the derivation stated above the lists in `constants.py`, and are never curated field by field.

Derived columns, joins and reshapes belong downstream, not in the asset.

## 2. Stamp a partition column only when the response has no day

A time-partitioned asset needs a `dt.date` column to partition on. Take the vendor's, and stamp one only when there is none to take.

- **The response carries the report day** — partition on that field, under the vendor's own name: `stat_time_day` (TikTok), `date_start` (Facebook), `day` (Criteo), `time_period` (Bing), `date` (Amazon Ads), `start_date` (Amazon Selling Partner). Return the rows untouched.
- **The response carries no day at all** — an entity snapshot (`ads`, `campaigns`, `advertisers`) or a report whose rows are dated only by the request — stamp `date` from `context.partition_date` and declare `date: dt.date | None` in the schema. This is the only column an asset may add.
- **The response carries instants that are not the report day** — stamp `date` too, and keep the vendor's fields as they are. Snapchat's `start_time` is midnight in the *account's* timezone, so `DATE(start_time)` is the day before for a European account; Impact's `event_date` is when the click happened, which for about 8% of rows is not the day of the export it arrived in. An instant (`dt.datetime`) is never the report day.

Never both: an asset that declares the vendor's day *and* stamps one has two columns holding the same value, and the reader cannot tell which is authoritative. If you are adding a `date` to a schema that already has a `dt.date` field, stop — you are about to violate rule 1.

Object lifecycle fields (`created_at`, `updated_at`, `create_time`, `modify_time`) are attributes of the object, not the day it was observed; they never serve as the partition column.

**A snapshot is dated by the day it was taken.** An asset that reads the vendor's *current* state declares `il.SnapshotPartitionConfig(column="date")`, never `il.TimePartitionConfig`, whatever its tag: every `Entity`, and every report of lifetime totals (follower counts, a post's cumulative insights). Its partition is the day the run observed in, on the job's clock, whatever window the run was given, and `context.partition_date` returns that day. Declared as a time partition, it writes today's state under whatever past day a backfill or retry names, and a missed day reads as recoverable when no API can read the past. A report whose rows are dated only by the request is the opposite case: it can be asked for any day, so it stays a time partition.

## 3. Types describe the data, not the vendor's serialization

Vendors serialize loosely: counts as floats, numbers as strings, dates as strings. The schema declares what the value *is*, and the conformer casts on the way in.

- counts (`impressions`, `clicks`, `purchases`, `units_sold`, view and engagement counts) are `int`
- money, rates, ratios and percentages (`cost`, `spend`, `sales`, `cpc`, `ctr`, `roas`, `*_rate`) are `float`
- a day is `dt.date`, an instant is `dt.datetime`, an identifier is `str`, always: an id is a label, vendors are inconsistent with themselves (Amazon sends numbers in reports and strings from its entity endpoints), and an integer id above 2^53 breaks in anything that passes through a float

`tests/test_catalog_conventions.py` checks these by field name over the whole catalog (`*_id`, `*_at`, `*_time`, `*_date`, amounts) and checks that every partition column is a declared `dt.date`; a deliberate exception goes in its `EXCEPTIONS`, with the reason.

**Decide from the data, not from the field name.** Before typing a metric family, check real rows: a column that looks like a count may hold fractions for a good reason. Amazon Ads' 63 count metrics were declared `float` for a year; a scan of 20.5 M rows showed whole numbers in every one of them and fractions in every rate and amount, which is what settled the change. A guess here is a breaking change later: BigQuery cannot retype a column in place.

## 4. One source, one shape

Assets of a source share the source's normalizer and its discriminator. Keep per-asset special cases out of the source: if one report needs a different reshape, give that asset its own normalizer rather than branching inside a shared one. A helper used by a single asset belongs in its body; a helper used by several is a module-level function with a docstring saying which ones and why.

A normalizer class lives in its source folder's `normalizer.py`, with the helpers and constants only it uses. Configuring one is the source's wiring and stays in `source.py`: the instance passed to the decorator or an asset, or a module-level instance several assets share.

## 5. A vendor's API has one engine

Every request a source sends goes through one engine: a class named after the API it speaks (`TiktokAdsApi`, `GraphApi`), holding how that API works, so no asset or connection repeats it. A container keeps its engine in its `api.py`. Containers that call the same API share one engine in their vendor's folder instead and have no `api.py` of their own: Facebook Insights and Instagram Insights both read Meta's Graph API through `meta/api.py`.

- **Its methods are the vendor's mechanisms, never its resources.** One response, a paged list, a report over a window of days, a report job: TikTok's are `get`, `rows` and `report`. A request's parameters are written in the asset that sends it, and so is its path unless the mechanism lives on a single endpoint (TikTok's integrated report), so a new asset never changes the engine; only a new mechanism does.
- **It owns the transport, the normalizer owns the row.** The engine sends, pages, unwraps the vendor's envelope, encodes parameters the way the vendor wants them, runs and polls report jobs, decompresses, throttles and raises the vendor's refusals. It returns records as the vendor sends them; reshaping a record (nested metrics, pivots, casing) stays with the normalizer (rule 4).
- **It holds no account.** Where a request names the account is part of each endpoint (TikTok's `advertiser_id` on `/campaign/get/` but `advertiser_ids` on `/advertiser/info/`, Snapchat's path, Criteo's filters), so the asset passes it with the endpoint's other parameters. Where the vendor scopes every call the same way (Amazon's scope header), each method takes it as its first argument.
- **It never sees the execution context.** An asset passes the day it asks for and stamps its own `date` (rule 2); the engine logs through its module's logger.
- **The connection builds it.** A connection exposes its authenticated transport as `client`, when it has one, and the engine as `api`, built on it. Its fetch-field providers and `check()` go through `api` as the assets do, so a refusal is handled in one place.
