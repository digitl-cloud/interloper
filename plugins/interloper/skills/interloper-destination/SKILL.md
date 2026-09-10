---
name: interloper-destination
description: Use when writing a custom Interloper destination (files, object storage, a database, an API sink) or debugging how partitions, windows, rewrites and read-back behave in one.
---

# Writing an Interloper destination

## Overview

The base class does the partition bookkeeping; you implement partition-level IO, where the
partition is `None` for the whole of an unpartitioned asset. `il.Destination` for files and objects:
implement `write_partition` and `read_partition`, windows are split and gathered for you.
`DatabaseDestination` for SQL: delete-then-insert by partition range, a window in one batch.
Overriding `write`/`read` wholesale is for a backend whose storage is not per partition,
and is how a destination ends up clobbering every partition on each write when done by accident.
Reference: https://docs.interloper.dev/guide/destinations/

## Recipe

1. **Files and objects**: implement `write_partition` and `read_partition` for one partition (or
   `None` for unpartitioned assets), and `partition_row_counts`:

   ```py
   import json
   from pathlib import Path

   import interloper as il
   from interloper.representation import Representation

   @il.destination(name="JSONL files")
   class JSONLDestination(il.Destination):
       base_path: str = ""

       def _path(self, context: il.IOContext, partition: il.Partition | None) -> Path:
           base = Path(self.base_path) / (context.asset.dataset or "") / context.asset.table
           if partition is None:
               return base / "data.jsonl"
           return base / f"{context.asset.partitioning.column}={partition.id}" / "data.jsonl"

       def write_partition(self, context, partition, data) -> None:
           rows = Representation.of(data).to_records(data)          # list[dict] from any representation
           path = self._path(context, partition)
           path.parent.mkdir(parents=True, exist_ok=True)
           path.write_text("".join(json.dumps(row, default=str) + "\n" for row in rows))

       def read_partition(self, context, partition):
           rows = [json.loads(line) for line in self._path(context, partition).read_text().splitlines() if line]
           return context.schema.reconcile(rows) if context.schema is not None else rows

       def partition_row_counts(self, context) -> dict[str, int]:
           column = context.asset.partitioning.column
           base = Path(self.base_path) / (context.asset.dataset or "") / context.asset.table
           return {p.name.split("=", 1)[1]: sum(1 for _ in (p / "data.jsonl").open()) for p in base.glob(f"{column}=*")}
   ```

   Layout convention: `{base_path}/{dataset}/{table}/{column}={partition_id}/data.<ext>`.
   Reconcile on read against `context.schema`: destinations store strings, dependent assets
   expect the schema's types.

2. **Databases**: `from interloper.destination import DatabaseDestination, PartitionFilter` (not
   exported on `il`). Implement four hooks, each starting with `(table, dataset, ...)`:
   `insert(table, dataset, data, context)` (view the data as rows with
   `Representation.of(data).to_records(data)`, or load it natively; `context.schema` and
   `context.asset.partitioning` are what a table created on first write is shaped from),
   `delete(table, dataset, where)`, `select(table, dataset, where)` and `count(table, dataset,
   column)`. `where` is `None` for the whole table or a `PartitionFilter`: render `where.value` as
   `column = value`, `where.bounds` as `column >= start AND column < end` (half-open, `dt.date`
   bounds; store dates so the comparison works). Every hook must tolerate a table that does not
   exist yet, since `write` skips empty data and a read or delete can come first; override
   `transaction()` to make delete-then-insert atomic. The base always deletes a partition before
   inserting it, so a rewrite never duplicates. `select` returns whatever your client produces
   natively (a DataFrame, an Arrow table, rows); consumers that want rows read `leg.records`.

3. **Verify** with a two-asset daily source:

   ```py
   jsonl, sqlite = JSONLDestination(base_path="./out"), SQLiteDestination(db_path="./out/shop.sqlite")
   shop = Shop(destinations=[jsonl, sqlite])                       # a source from the interloper-source skill
   dag = il.DAG(shop)
   day = il.TimePartition(dt.date(2026, 3, 10))
   dag.materialize(day); dag.materialize(day)                      # second write must not duplicate
   sqlite.read(il.IOContext(asset=shop.orders, partition_or_window=day, schema=Order))
   jsonl.partition_row_counts(il.IOContext(asset=shop.orders, partition_or_window=day))
   shop.orders.materialize(il.TimePartitionWindow(dt.date(2026, 3, 1), dt.date(2026, 3, 3)))   # needs allow_window=True
   ```

   A window write produces one file (or one deleted range) per partition; a window read
   returns one result per partition, newest first. Dependent assets read from the upstream
   asset's first destination (or `default_destination_key`).

4. **Register** like any component: `@il.destination(name=...)` for the catalog, an
   `interloper.components` entry point for packages, `path: mypkg.destinations.JSONLDestination`
   in specs.

## Quick reference

| Need | Use |
|------|-----|
| Fields of `IOContext` | `asset`, `partition_or_window`, `metadata`, `schema` |
| Rows from a DataFrame or list | `Representation.of(data).to_records(data)` |
| Rows the consumer can iterate whatever the destination | `leg.records` on an `il.Upstream` |
| Credentials for the sink | a connection relation: `connection: MyConnection` on the class body, or `relations={"connection": il.Relation(MyConnection)}` on the decorator |
| Cached client on the instance | private attribute `_client: Client | None = None`, plain assignment works |
| Built-ins to imitate | `il.CSVDestination` (partitioned files), `il.FileDestination` (same layout, pickled objects), `il.MemoryDestination` |

## Common mistakes

- Subclassing `il.Destination` and writing `{table}.json`: a partition rewrite wipes the others
  and a window lands in one file.
- Returning raw strings from `read_partition`; the dependent asset does arithmetic on text.
- Dates stored in a format that does not compare with the `dt.date` bounds of the range hooks.
- Forgetting `allow_window=True` on the asset's `TimePartitionConfig` when testing window writes; the error is a
  `PartitionError`, not a destination problem.
- Expecting `DatabaseDestination` on `il.`; import it with `from interloper.destination.database
  import ...` (`import interloper.destination.database` fails, `il.destination` is the decorator).
