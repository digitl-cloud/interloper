# Destinations

A **destination** controls **where** and **how** asset data is stored. A destination is
completely separate from how data is produced, so you can swap backends without changing asset
logic.

!!! note "Renamed from IO"

    Earlier versions called this concept "IO" (`il.IO`, `il.FileIO`, …). It is now
    **Destination** (`il.Destination`, `il.FileDestination`, …). The read/write context is
    still called `IOContext`.

## Configuring a destination

### On a source

```py
source = my_source(destinations=il.FileDestination(base_path="./data"))
```

### On an individual asset

```py
asset_instance = my_asset(destinations=il.FileDestination(base_path="./data"))
```

### Declaring destination types up front

The `@asset`/`@source` decorators accept `destinations=[...]` to declare the destination
**types** an asset supports. Instances are then supplied via the `destinations=` argument at
instantiation:

```py
@il.asset(destinations=[il.FileDestination, il.CSVDestination])
def my_asset():
    return [{"x": 1}]
```

### Multiple destinations

Write to several destinations at once by assigning a list. Each destination is identified by
its `id`:

```py
asset_instance = my_asset(
    destinations=[
        il.FileDestination(id="file", base_path="./data"),
        il.CSVDestination(id="csv", base_path="./exports"),
    ],
    default_destination_key="file",
)
```

When an asset has multiple destinations, downstream assets read their upstream input from the
one named by `default_destination_key` (matched against each destination's `id`).

## Built-in destinations

### MemoryDestination

In-memory storage backed by a class-level dict. Useful for tests and quick experiments.

```py
asset_instance = my_asset(destinations=il.MemoryDestination())
asset_instance.materialize()
```

All instances share the same storage. Clear it between tests:

```py
il.MemoryDestination.clear()
```

### FileDestination

Pickle-based storage on the local filesystem.

```py
asset_instance = my_asset(destinations=il.FileDestination(base_path="./data"))
asset_instance.materialize()
# Writes to ./data/{dataset}/{asset_key}/data.pkl
```

With partitioning, files are organized by partition value:

```
./data/{dataset}/{asset_key}/{column}={partition_id}/data.pkl
```

### CSVDestination

CSV files on the local filesystem, with the same path layout as `FileDestination` (`data.csv`
instead of `data.pkl`). Because CSV is untyped, reads reconcile rows against the asset's schema
to restore declared types.

```py
asset_instance = my_asset(destinations=il.CSVDestination(base_path="./data"))
```

## IOContext

Every `read`/`write` call receives an immutable `IOContext`:

- `context.asset` -- the asset being read/written (carries `id`, `dataset`, `partitioning`)
- `context.partition_or_window` -- the current partition or window (`None` if unpartitioned)
- `context.schema` -- the effective schema of the data (declared or inferred), or `None`
- `context.metadata` -- run metadata

## Custom destinations

Create a custom destination by subclassing `il.Destination` (a pydantic model) and implementing
`read`/`write`, or use the `@il.destination` decorator on a plain class:

```py
import json
from pathlib import Path
from typing import Any
import interloper as il

@il.destination(name="JSON")
class JSONDestination(il.Destination):
    base_path: str = ""

    def write(self, context: il.IOContext, data: Any) -> None:
        path = Path(self.base_path) / f"{context.asset.key}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data))

    def read(self, context: il.IOContext) -> Any:
        path = Path(self.base_path) / f"{context.asset.key}.json"
        return json.loads(path.read_text())
```

### Partition-aware destinations

For backends that store one scope per partition, subclass `il.PartitionedDestination`. It
implements partition/window dispatch once; you implement two hooks:

```py
class PartitionedJSONDestination(il.PartitionedDestination):
    base_path: str = ""

    def _write_scope(self, context, partition, data) -> None:
        ...

    def _read_scope(self, context, partition) -> Any:
        ...
```

`MemoryDestination` and `CSVDestination` are built on `PartitionedDestination`.

### Database destinations

For SQL-style stores, subclass `il.DatabaseDestination` (itself a `PartitionedDestination`).
It turns reads/writes into a small set of abstract row operations — `_insert`, `_delete_all`,
`_delete_partition`, `_select_all`, `_select_partition`, `_count_by_partition` — and handles
write dispositions (`REPLACE` deletes the matching scope before insert, `APPEND` does not).
`BigQueryDestination` (below) is implemented this way.

## interloper-google-cloud

The BigQuery destination. Install with:

```sh
pip install interloper-google-cloud
```

```py
from interloper_google_cloud import BigQueryDestination, GoogleCloudConnection

asset_instance = my_asset(
    destinations=BigQueryDestination(
        id="bq",
        connection=GoogleCloudConnection(...),
        project="my-gcp-project",
        location="EU",
        default_dataset="my_dataset",
    ),
)
```

`BigQueryDestination` writes to `{project}.{dataset}.{asset_key}`, applies daily time
partitioning when the partition column is a date/timestamp, and uses the effective schema for
typed loads. It returns pandas DataFrames on read. It requires a `GoogleCloudConnection`
resource carrying the service-account credentials (see [Connections](connections.md)).

!!! note

    The core library ships `FileDestination`, `CSVDestination` and `MemoryDestination`;
    `interloper-google-cloud` adds `BigQueryDestination`. There is no built-in
    Postgres/MySQL/SQLite destination — implement one by subclassing `il.DatabaseDestination`.
