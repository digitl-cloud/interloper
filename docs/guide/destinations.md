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

Other destinations come from companion packages; see [Ecosystem](../reference/ecosystem.md).

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
```

`write()` and `read()` are the base class's: a window write is split into one `write_partition` call
per partition, slicing the data through its [representation](../extending/representations.md)
on the partition column; a window read returns one result per partition, newest first. A
destination written this way is partition-correct by construction, and `CSVDestination`,
`FileDestination`, `MemoryDestination` and `GCSDestination` are all built exactly like this. The
partitions a context covers are on the context itself, `context.partitions` and `context.slices(data)`,
for a backend that needs them.

The decorator accepts the class's public ClassVars and field defaults, plus `relations=`; see the
[decorators reference](../reference/decorators.md). A destination's own connection is a relation,
declared as an annotation or through `relations=`; see
[Resources](resources.md#relations-on-sources-and-destinations).

Override `partition_row_counts(context)` to report rows per partition; `asset.partition_row_counts()`
and coverage tooling call it.

A backend whose storage is not per partition may override `write()` or `read()`
wholesale. `DatabaseDestination` below does that for writes: rows carry the partition column, so
a window clears every partition it covers and inserts the whole batch once.

### Database destinations

`DatabaseDestination` (imported from `interloper.destination`, together with
`WriteDisposition`) targets stores addressed by table and schema. Its `write_partition` clears one
partition and inserts; its `write` batches a window into one insert. Reads and writes reduce to a
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
  matching partition before inserting; `APPEND` never deletes. A class attribute, not a field.
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
