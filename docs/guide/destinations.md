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
@il.asset(destinations=[il.CSVDestination, WarehouseDestination])
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

Same layout as `CSVDestination`, but it stores whatever the asset returned, tabular or not —
so use it for arbitrary objects, and `CSVDestination` when you want to read the files yourself.
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

Other destinations come from companion packages; see [Ecosystem](../reference/ecosystem.md).

## IOContext

Every `read()` and `write()` receives an immutable `IOContext`:

| Field | Meaning |
|-------|---------|
| `asset` | The asset being read or written. `asset.table`, `asset.dataset`, `asset.partitioning` name the storage location. |
| `partition_or_window` | The scope of this call, or `None` for an unpartitioned asset. |
| `schema` | The effective schema of the data: the declared one, or the one inferred during conform. `None` when none could be resolved. |
| `metadata` | Run metadata (run id, backfill id). |

## Custom destinations

Subclass `il.Destination`, or decorate a plain class with `@il.destination`, and implement
`read()` and `write()`. Both may be sync or `async def`:

```py
import json
from pathlib import Path
from typing import Any

import interloper as il

@il.destination(name="JSON files")
class JSONDestination(il.Destination):
    base_path: str = ""

    def write(self, context: il.IOContext, data: Any) -> None:
        path = Path(self.base_path) / context.asset.dataset / f"{context.asset.table}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, default=str))

    def read(self, context: il.IOContext) -> Any:
        path = Path(self.base_path) / context.asset.dataset / f"{context.asset.table}.json"
        return json.loads(path.read_text())
```

This form writes one file per asset and ignores `context.partition_or_window`: a partition
rewrite replaces every partition and a window lands in one file. It suits unpartitioned assets;
partitioned ones belong on `il.PartitionedDestination` below.

Decorator options: `resources`, `key`, `name`, `icon`, `tags`, `read_representation`,
`materialization_strategy`. Resource slots are declared as typed attributes or through
`resources=`; see [Resources](resources.md#resources-on-sources-and-destinations).

Override `partition_row_counts(context)` to report rows per partition; `asset.partition_row_counts()`
and coverage tooling call it.

### Partition-aware destinations

`il.PartitionedDestination` implements the partition dispatch once. Subclasses implement two
scope hooks and are partition-correct by construction:

```py
class JSONDestination(il.PartitionedDestination):
    base_path: str = ""

    def _write_scope(self, context, partition, data) -> None:
        # partition is None for the unpartitioned whole
        ...

    def _read_scope(self, context, partition):
        ...
```

A window write is split into one `_write_scope` call per partition, slicing the data through
its [representation](../extending/representations.md) on the partition column. A window read
returns one result per partition, newest first. `CSVDestination` and `MemoryDestination` are
built this way.

### Database destinations

`DatabaseDestination` (imported from `interloper.destination`, together with
`WriteDisposition`) targets stores addressed by table and schema. Reads and writes reduce to a
small set of row operations:

| Hook | Called for |
|------|-----------|
| `_insert(table, schema, rows)` | writing records |
| `_delete_all(table, schema)` | replacing an unpartitioned asset |
| `_delete_partition(table, schema, column, value)` | replacing a non-time partition |
| `_delete_partition_range(table, schema, column, start, end)` | replacing a time partition, by half-open bounds |
| `_select_all(table, schema)` | reading an unpartitioned asset |
| `_select_partition(table, schema, column, value)` | reading a non-time partition |
| `_select_partition_range(table, schema, column, start, end)` | reading a time partition |
| `_count_by_partition(table, schema, column)` | `partition_row_counts` |

Optional overrides: `_transaction()` (a context manager around each write, a no-op by
default), and `_insert_data(table, schema, data, context)` for backends that load a native
representation directly (a DataFrame into a Parquet load job) using `context.schema`.

Behaviour the base class owns:

- **Write disposition**: `write_disposition = WriteDisposition.REPLACE` (default) deletes the
  matching scope before inserting; `APPEND` never deletes. A class attribute, not a field.
- **Time partitions are scoped by bounds**, not by equality, because rows of a monthly partition
  carry daily dates.
- **Read representation**: rows are materialized into the representation named by
  `read_representation` (`"rows"` by default; `"dataframe"` for pandas-native backends).
- **Write-time strategy**: the `materialization_strategy` field lets a backend demand
  schema-shaped data: `STRICT` validates against the effective schema before writing,
  `RECONCILE` coerces, `AUTO` trusts the conformed data. It is set as a default via the decorator
  and overridable per configured destination.
- A warning is emitted when the partition column is missing from written data, since
  downstream reads by partition would then return nothing.

## Table naming

The table name comes from the owning source's `asset_table()`; see
[Sources](sources.md#dataset-and-table-naming). Destinations read `context.asset.table` and
`context.asset.dataset` and never compute names themselves.
