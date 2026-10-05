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

::: interloper.errors
    options:
      show_root_heading: false

## Errors that are plain built-ins

Some validation raises built-ins directly: unknown constructor keyword arguments, a `FetchField`
provider reference that does not resolve, an `oauth=` decorator option on a non-OAuth connection,
and multiple discriminator fields raise `TypeError`; an invalid key or identifier, a window
ending before it starts, an unsupported granularity, and `bounded_gather(limit=0)` raise
`ValueError`; `Registry[...]` on a missing name raises `KeyError`. Binding a component a relation
does not accept raises `ConfigError` instead, e.g. `Shop(connection=B())` gives `ConfigError:
Shop.connection does not accept connection 'b' (declared: kind ['connection'], key
['shop_connection'])`.
