# Errors

All framework exceptions derive from `interloper.errors.InterloperError`. Each domain error also
subclasses the built-in it replaces, so `except ValueError` handlers keep working.

```py
from interloper.errors import InterloperError, PartitionError

try:
    dag.materialize()
except PartitionError:
    ...
except InterloperError:
    ...
```

| Exception | Also a | Raised when |
|-----------|--------|-------------|
| `ConfigError` | `ValueError` | A configuration value is missing, mistyped or unresolvable; an unknown runner type; an unregistered OAuth provider without `auth_url`; a component of an unregistered kind. |
| `SpecError` | `ConfigError` | A spec file is missing, unparsable, references undefined `${VAR}`s or is not a valid spec. |
| `CatalogKeyError` | `ConfigError` | A catalog key does not resolve. |
| `DAGError` | `ValueError` | Empty DAG, non-workload item, duplicate id, unpartitioned asset downstream of a partitioned one, non-runnable component in `DAG.from_spec_file`. |
| `CircularDependencyError` | `DAGError` | A cycle in the graph. |
| `DependencyNotFoundError` | `DAGError` | A mandatory dependency points at an id outside the DAG. |
| `AssetNotFoundError` | `DAGError`, `KeyError` | An operation id is not in the DAG. |
| `AssetError` | `ValueError` | Dependencies without a DAG, a failed upstream read, a resource of the wrong type, a strategy requiring a schema without one, a schema declared on non-tabular data, no destinations for row counts. |
| `DependencyContractError` | `AssetError` | A wired upstream does not match the `requires` contract. |
| `SourceError` | `ValueError` | `select` names an unknown asset. |
| `ConnectionCheckError` | | A connection check fails with a curated message. |
| `PartitionError` | `ValueError` | A partitioned asset run without a scope, a window against `allow_window=False`, a granularity mismatch, a scope before `start`, row counts on an unpartitioned asset. |
| `SchemaError` | `ValueError` | Validation or reconciliation fails; inference from empty data. |
| `NormalizerError` | `TypeError` | Data cannot be coerced to rows. |
| `DestinationError` | | A destination instance is not among the asset's allowed classes. |
| `DataNotFoundError` | `DestinationError` | `MemoryDestination` read of a missing key. |
| `EventError` | `ValueError` | Event deserialization fails. |
| `RunnerError` | `RuntimeError` | State accessed before a run, a deadlock or invalid graph state during a walk, a process pool not initialized, a cross-process failure re-raised by message. |
| `AuthenticationError` | `ValueError` | REST OAuth2 auth has no access token or no refresh token. |
| `NotFoundError` | `KeyError` | A database record is missing (platform store). |
| `InUseError` | | A record cannot be deleted because others reference it; carries `referrers` (platform store). |
| `QuotaExceededError` | | An organisation quota refuses an operation; carries `quota`, `limit`, `used` (platform store). |
| `HydrationError` | | A stored spec cannot be rebuilt into its class (platform store). |
| `ComponentDriftError` | | A persisted component's catalog key no longer resolves (platform store). |

## `format_exception`

`interloper.errors.format_exception(exc)` renders any exception as a non-empty single line,
`TypeName: message` or just `TypeName`. Pydantic `ValidationError`s are collapsed to field and
reason, never their input values, so a failing connection never leaks its secrets into an event
or a result.

## Errors that are plain built-ins

Some validation raises built-ins directly: unknown constructor keyword arguments, a resource of
the wrong type passed by slot name, a `FetchField` provider reference that does not resolve, an
`oauth=` decorator option on a non-OAuth connection, and multiple discriminator fields raise
`TypeError`; a required `ResourceRef` that is unset, an invalid key or identifier, a window
ending before it starts, an unsupported granularity, and `bounded_gather(limit=0)` raise
`ValueError`; `Registry[...]` on a missing name raises `KeyError`.
