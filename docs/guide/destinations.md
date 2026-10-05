# Destinations

A destination decides **where** and **how** asset data is stored and read back. It is separate
from how data is produced, so the same asset can land in a CSV folder, a warehouse or a test
double without changing.

## Configuring destinations

On a source, so every asset without its own inherits it:

```py
source = Shop(destinations=il.CSVDestination(base_path="./data"))
```

On an asset:

```py
asset = source.orders(destinations=il.CSVDestination(base_path="./exports"))
```

A single destination or a list is accepted. With several, every write goes to all of them:

```py
source = Shop(destinations=[
    il.CSVDestination(base_path="./data"),
    WarehouseDestination(connection=warehouse),
])
```

Upstream reads use the **first** resolved destination of the upstream asset.
`default_destination_key` names a preferred one for the platform and UIs to honour.

Decorators can restrict the destination **classes** an asset or source accepts; an instance of
another class raises `DestinationError` at materialization:

```py
@il.asset(relations={"destinations": [il.CSVDestination, WarehouseDestination]})
def orders(self): ...
```

## Built-in destinations

### CSVDestination

CSV files on the local filesystem, one folder per asset, one file per partition:

```
{base_path}/{dataset}/{table}/data.csv
{base_path}/{dataset}/{table}/{column}={partition_id}/data.csv
```

Rows are written as records; the first row's keys become the header. CSV stores strings, so a
read reconciles the rows against the effective schema carried in the context, restoring the
declared types and turning empty strings into `None`. Window writes are split per partition.

### FileDestination

Pickled Python objects, one file per partition:

```
{base_path}/{dataset}/{table}/data.pkl
{base_path}/{dataset}/{table}/{column}={partition_id}/data.pkl
```

Same layout as `CSVDestination`, but it stores whatever the asset returned, tabular or not, so
use it for arbitrary objects, and `CSVDestination` when you want to read the files yourself.
Window writes are split per partition where the data's representation allows it; a non-tabular
object, which nothing can slice, is stored whole under each partition of the window.

### MemoryDestination

An in-process store keyed by `{dataset}/{table}/{column}={partition_id}`, shared by every
instance, meant for tests:

```py
il.MemoryDestination()
il.MemoryDestination.clear()      # between tests
```

Reading a key that was never written raises `DataNotFoundError`.

Other destinations come from companion packages; see [Ecosystem](ecosystem.md).

## IOContext

Every `read()` and `write()` receives an immutable `IOContext`:

| Field | Meaning |
|-------|---------|
| `asset` | The asset being read or written. `asset.table`, `asset.dataset`, `asset.partitioning` name the storage location. |
| `partition_or_window` | The partition or window of this call, or `None` for an unpartitioned asset. |
| `schema` | The effective schema of the data: the declared one, or the one inferred during conform. `None` when none could be resolved. |
| `metadata` | Run metadata (run id, backfill id). |

## Custom destinations

A destination stores data one **partition** at a time, `None` standing for the whole of an
unpartitioned asset. Subclass `il.Destination`, or decorate a plain class with `@il.destination`,
and implement the two partition hooks. Both may be sync or `async def`:

```py
import json
from pathlib import Path
from typing import Any

import interloper as il

@il.destination(name="JSON files")
class JSONDestination(il.Destination):
    base_path: str = ""

    def _path(self, context: il.IOContext, partition: il.Partition | None) -> Path:
        base = Path(self.base_path) / (context.asset.dataset or "") / context.asset.table
        if partition is None:
            return base / "data.json"
        return base / f"{context.asset.partitioning.column}={partition.id}" / "data.json"

    def write_partition(self, context: il.IOContext, partition: il.Partition | None, data: Any) -> None:
        path = self._path(context, partition)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, default=str))

    def read_partition(self, context: il.IOContext, partition: il.Partition | None) -> Any:
        return json.loads(self._path(context, partition).read_text())

    def partition_row_counts(self, context: il.IOContext) -> dict[str, int]:
        base = Path(self.base_path) / (context.asset.dataset or "") / context.asset.table
        column = context.asset.partitioning.column
        return {p.name.split("=", 1)[1]: len(json.loads((p / "data.json").read_text())) for p in base.glob(f"{column}=*")}
```

These three hooks are the whole contract, and they are abstract: a destination missing one cannot be
instantiated. `partition_row_counts` feeds `asset.partition_row_counts()` and the coverage view.

`write()` and `read()` are the base class's: a window write is split into one `write_partition` call
per partition, slicing the data through its [representation](../extending/representations.md)
on the partition column; a window read returns one result per partition, newest first. A
destination written this way is partition-correct by construction, and `CSVDestination`,
`FileDestination`, `MemoryDestination` and `ObjectStoreDestination`, the base of `GCSDestination` and
`S3Destination`, are all built exactly like this. The partitions a context covers are on the context
itself, `context.partitions` and `context.slices(data)`, for a backend that needs them.

The decorator accepts the class's public ClassVars and field defaults, plus `relations=`; see the
[decorators reference](decorators.md). A destination's own connection is a relation,
declared as an annotation or through `relations=`; see
[Resources](resources.md#relations-on-sources-and-destinations).

A backend whose storage is not per partition may override `write()` or `read()`
wholesale. `DatabaseDestination` below does that for writes: rows carry the partition column, so
a window clears every partition it covers and inserts the whole batch once.

### Object store destinations

`ObjectStoreDestination` (imported from `interloper.destination`, together with `StoredObject`)
is a destination whose partitions are objects in a bucket. A backend writes its storage calls and
nothing else, in four hooks that all take an object name relative to the bucket root:

| Hook | Called for |
|------|-----------|
| `put_object(name, payload, content_type, metadata)` | writing; uploads the serialized partition, overwriting any object of that name, with the given custom metadata |
| `get_object(name)` | reading; returns the object's bytes, or `None` when it does not exist, which the base turns into a `DataNotFoundError` |
| `list_objects(prefix)` | `partition_row_counts`; yields a `StoredObject(name, metadata)` per object under the prefix |
| `object_uri(name)` | messages; the backend's URI for the object, such as `gs://bucket/name` |

The base declares two fields, `format` (`parquet`, the default, `jsonl` or `csv`) and `prefix`
(a path prefix inside the bucket), and lays objects out hive style, one per partition:

```
{prefix}/{dataset}/{table}/data.{ext}
{prefix}/{dataset}/{table}/{column}={partition_id}/data.{ext}
```

Behaviour the base class owns:

- **The partition column lives in the path only**: it is dropped from a partition's file contents
  on write, since external readers like BigQuery external tables and DuckDB reject a duplicate
  partition column, and re-injected from the partition on read, so round trips stay lossless.
- **Row counts need no download**: every object carries its row count as `row_count` metadata,
  so `partition_row_counts` is one listing. An object without it, written by another tool, is
  downloaded and counted.
- **Reads come back as rows**, reconciled against `context.schema` when there is one, which
  restores the declared types that JSONL and CSV read back as strings.
- **File shapes follow the schema**: a Parquet file's columns are built from the effective
  schema's field specs, so every partition has the same shape even when a column is all null.

The formats live in `interloper.destination.formats` (`FileFormat`, `ParquetFormat`,
`JSONLFormat`, `CSVFormat`). Parquet needs `pyarrow`, which core does not depend on: it is
imported on first use and a missing install raises `ImportError` naming it. The packages that
ship an object-store backend depend on it themselves.

### Database destinations

`DatabaseDestination` (imported from `interloper.destination`, together with `PartitionFilter`)
is a destination whose partitions are rows selected by a filter in a table. A backend writes its
SQL dialect and nothing else, in four hooks that all start with the table and the dataset the
asset resolves to:

| Hook | Called for |
|------|-----------|
| `insert(table, dataset, data, context)` | writing; the data arrives in its native representation; `Representation.of(data).records` views it as rows and `.to("dataframe")` converts it for a columnar load. The one hook that gets the whole context, since a table created on first write takes its columns from `context.schema` and its partitioning and description from `context.asset` |
| `delete(table, dataset, where)` | replacing; `where` is a `PartitionFilter` or `None` for the whole table |
| `select(table, dataset, where)` | reading; returns whatever table type the backend produces natively |
| `count(table, dataset, column)` | `partition_row_counts`; rows grouped by the column's values |

A `PartitionFilter` is a column and either a `value` (a partition matched by its id) or half-open
`bounds` (a time partition, whose rows may carry any date inside the period). The base resolves
it from the partition, so a backend only renders `column = value` or
`column >= start AND column < end` in its dialect. `transaction()` is an optional context
manager around each delete-then-insert, a no-op by default.

Behaviour the base class owns:

- **Replacing**: a partition's rows are deleted before its data is inserted; a window deletes
  every partition it covers and inserts the batch once.
- **Time partitions are scoped by bounds**, not by equality, because rows of a monthly partition
  carry daily dates.
- **Reads are native**: `read` hands back what `select` returned, a DataFrame from BigQuery, rows
  from a row store. A consumer indifferent to the destination reads `il.Upstream.records`.
- **Data arrives conformed**: the asset's conform step already put the data in the effective
  schema's canonical types, so a backend never re-validates or coerces before writing.
- A warning is emitted when the partition column is missing from written data, since
  downstream reads by partition would then return nothing.

## Table naming

The table name comes from the owning source's `asset_table()`; see
[Sources](sources.md#dataset-and-table-naming). Destinations read `context.asset.table` and
`context.asset.dataset` and never compute names themselves.
