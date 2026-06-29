# Data Validation

Interloper provides several layers of data validation.

## Schema validation

When a `schema` is declared on an asset, the (normalized) data is conformed to the schema during
materialization according to the asset's [materialization strategy](schema.md#materialization-strategy):

```py
import interloper as il

class UserSchema(il.Schema):
    id: int
    name: str

@il.asset(schema=UserSchema, materialization_strategy=il.MaterializationStrategy.STRICT)
def users():
    return [{"id": 1, "name": "Alice"}]
```

If the data doesn't satisfy the schema, a `SchemaError` is raised. If a strategy requires a
schema but none is declared (or the data is non-tabular), an `AssetError` is raised.

## Materialization strategies

The `MaterializationStrategy` controls **how strictly** schemas are enforced. See
[Schema & Normalizer](schema.md) for full details.

| Strategy | Schema required | Behavior |
|----------|-----------------|----------|
| `AUTO` | No | Infers a schema if missing; validates if present (no coercion) |
| `STRICT` | Yes | Fails on any mismatch (extras, missing, wrong types) |
| `RECONCILE` | Yes | Aligns columns and coerces values to the schema's types |

## Schema inference

Under `AUTO` with no explicit schema, a schema is inferred from the data — each key becomes an
optional field whose type is widened across observed rows:

```py
@il.asset  # AUTO strategy, no schema
def users():
    return [{"id": 1, "name": "Alice"}]
    # Inferred: id: int | None, name: str | None
```

## DAG validation

The DAG validates structural constraints at construction time:

- A non-partitioned asset **cannot** depend on a partitioned asset (raises `DAGError`)
- Circular dependencies are detected and raise a `CircularDependencyError`
- Missing dependencies raise a `DependencyNotFoundError`
- Duplicate assets raise a `DAGError`

```py
# This raises a DAGError at construction time:
@il.source
def invalid():
    @il.asset(partitioning=il.TimePartitionConfig(column="date"))
    def partitioned(context: il.ExecutionContext):
        return [{"date": str(context.partition_date)}]

    @il.asset  # non-partitioned depending on a partitioned asset
    def summary(partitioned):
        return [{"count": len(partitioned)}]

    return [partitioned, summary]

il.DAG(invalid(...))  # -> DAGError
```
