---
name: interloper-source
description: Use when adding or changing an Interloper source, asset, connection or schema in a user project (a connector for an API, a new asset in an existing source, credentials for a service), before writing the code.
---

# Writing an Interloper source

## Overview

A source is a class: configuration fields on the class, assets as `@il.asset` methods reading them
through `self`, resources injected by type annotation. Everything the platform later shows or
validates is derived from that class, so get the declarations right and the rest follows.
Reference: https://docs.interloper.dev/guide/sources/ and https://docs.interloper.dev/guide/assets/

## Recipe

1. **Connection first.** One class per service, credentials as fields, a client as a
   `cached_property`. Environment loading needs a prefix, and the import is pydantic's:

   ```py
   from functools import cached_property

   import httpx
   import interloper as il
   from pydantic_settings import SettingsConfigDict

   @il.connection(name="Shop API")
   class ShopConnection(il.Connection):
       model_config = SettingsConfigDict(env_prefix="shop_")   # SHOP_API_KEY

       api_key: str = il.SecretField(description="API key")

       @cached_property
       def client(self) -> il.RESTClient:
           return il.RESTClient("https://api.shop.example", auth=il.HTTPBearerAuth(self.api_key))
   ```

   Use the sync `RESTClient` with `HTTPBearerAuth`. Add a check so the UI can test credentials;
   return `True` on success, `False` when they provably fail:

   ```py
       async def check(self) -> bool:
           async with httpx.AsyncClient() as client:
               response = await client.get("https://api.shop.example/me", headers={"Authorization": f"Bearer {self.api_key}"})
           return response.is_success
   ```

2. **Schema per asset, including upstream assets.** A schema is a class with typed fields;
   `T | None` marks nullable columns. Destinations such as CSV store strings; the schema types
   the data on write and on read-back into dependent assets. Partitioned assets carry the
   partition column in the schema and in every row.

   ```py
   import datetime as dt

   class Order(il.Schema):
       date: dt.date
       id: int
       total: float

   class OrderStats(il.Schema):
       date: dt.date
       orders: int
       revenue: float
   ```

3. **Source class.** Declare the connection slot on the decorator so it can be passed as a
   keyword and trickles to every asset:

   ```py
   @il.source(name="Shop", tags=["Commerce"], resources={"connection": ShopConnection})
   class Shop(il.Source):
       account: str = il.InputField(description="Shop account id", discriminator=True)

       @il.asset(schema=Order, partitioning=il.TimePartitionConfig(column="date"))
       def orders(self, context: il.ExecutionContext, connection: ShopConnection) -> list[dict]:
           day = context.partition_date
           rows = connection.client.get("/orders", params={"day": day.isoformat()}).json()
           for row in rows:
               row["date"] = day
           return rows

       @il.asset(schema=OrderStats, partitioning=il.TimePartitionConfig(column="date"))
       def order_stats(self, context: il.ExecutionContext, orders: list[dict]) -> list[dict]:
           return [{"date": context.partition_date, "orders": len(orders), "revenue": sum(o["total"] for o in orders)}]
   ```

   A parameter named after a sibling asset is a dependency. A partitioned asset may depend on
   an unpartitioned one; the reverse is rejected when the DAG is built.

4. **Verify with one partition before anything else:**

   ```py
   shop = Shop(account="acme", connection=ShopConnection(api_key="..."), destinations=il.CSVDestination(base_path="./data"))
   print(il.DAG(shop).materialize(il.TimePartition(dt.date(2026, 1, 15))))
   # RunResult(status=completed, ..., completed=2, failed=0, ...)
   ```

   Output lands at `./data/<dataset>/<table>/date=2026-01-15/data.csv`; the dataset defaults
   to the source key. `run()` re-executes and writes nothing; to read what was written, typed:

   ```py
   dest = shop.destinations[0]
   dest.read(il.IOContext(asset=shop.order_stats, partition_or_window=il.TimePartition(dt.date(2026, 1, 15)), schema=OrderStats))
   ```

5. **Register** (packages only, not a bare module): `[project.entry-points."interloper.components"]`
   with `shop = "shop"` in `pyproject.toml`, so the catalog, CLI `key:` references and the UI
   find the class. A single module is referenced by `path: shop.Shop` instead.

6. To run from the CLI or a spec file, use the interloper-run skill.

## Quick reference

| Need | Use |
|------|-----|
| Text, secret, long text, JSON, dropdown | `il.InputField`, `il.SecretField`, `il.TextField`, `il.JsonField`, `il.SelectField(options=[...])` |
| Options fetched from the service | `il.FetchField(provider="connection.accounts")` plus an `@il.fetch_field_provider` method on the connection |
| Name instances by a field | `discriminator=True` on that field (also suffixes table names) |
| Hourly, monthly, yearly partitions | `il.TimePartitionConfig(column=..., granularity=il.TimeGranularity.MONTH)` |
| Whole ranges in one call | `allow_window=True`, then read `context.window` |
| Cross-source dependency | `requires={"param": "other_source.asset"}` |
| Optional dependency | parameter default `None`, or `optional_requires` |
| Reshape vendor payloads | `normalizer=il.Normalizer(flatten_max_level=1, snake_case_digits=True)` |
| OAuth service | subclass `il.RefreshTokenOAuthConnection` with `@il.connection(oauth=il.OAuthConfig("google", scope=...))` |

## Common mistakes

- Guessing constructor keywords (`Shop(io=...)`, `IOContext(dataset=...)`). Unknown keywords raise
  `TypeError`; the instance keywords are `destinations`, `resources`, `dataset`, `select`, the
  class's own fields, and any declared resource slot name.
- Passing `connection=...` without `resources={"connection": ...}` on the decorator. Either
  declare the slot there or pass `resources={"connection": conn}`.
- An upstream asset without a schema: its rows come back from CSV as strings and the dependent
  asset does arithmetic on text.
- The functional `@il.source def ...` form for anything configurable; assets cannot reach the
  configuration there.
- Reading `context.partition_date` on a non-daily asset; use `context.partition.value`.
- Materializing a dependent asset with no destination on the source: the read of the upstream
  fails with `AssetError: No destination found for upstream asset`.
