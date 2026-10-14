"""The asset decorators: an Asset, View or Reference subclass from a decorated function."""

from __future__ import annotations

from typing import Any

from interloper.asset.base import Asset
from interloper.asset.reference import Reference
from interloper.asset.view import View
from interloper.component.decorator import Decorator

asset = Decorator(Asset)
"""Create an Asset subclass from a decorated function.

The decorated function may be sync or ``async``. Sync functions are offloaded to
a worker thread at materialization time; ``async`` functions are awaited
natively on the event loop:

```py
@asset
def users(**kwargs):
    return fetch_users()

@asset
async def events(**kwargs):
    return await fetch_events()

@asset(tags=["Report"], schema=UsersStats, relations={"destinations": [BigQueryDestination]})
def users_stats(config: MyConfig, connection: MyConn) -> Any:
    return fetch_stats()
```

Every parameter of the decorated function declares a relation, inferred from its
annotation; ``relations`` is for the declarations an annotation cannot express,
such as a cross-source or many-valued upstream. It maps a ``data()`` parameter
name to a `Relation`, a component class (the shorthand for a relation on it), or
a list of component classes narrowing the relation `Asset` declares under that
name; explicit declarations win over the inferred ones.

Every other keyword is definition metadata or behaviour: the public ClassVars and
field defaults `Asset` declares (``key``, ``name``, ``icon``, ``tags``,
``schema``, ``partitioning``, ``dataset``, ``normalizer``,
``materialization_strategy``, ...). An unknown name is a ``TypeError`` at
decoration. The result is an Asset subclass with the function as its ``data()``.
"""

view = Decorator(View)
"""Create a View subclass from a decorated function.

Like `asset`, but the function returns the view's definition instead of its
rows: SQL text in the destination's dialect, or anything the destination
compiles. Each upstream parameter receives the destination's reference to that
upstream in ``data``:

```py
@il.view(relations={"orders": il.Relation("asset", "shop.orders", many=True)})
def order_totals(orders: list[il.Upstream]) -> str:
    return " UNION ALL ".join(f"SELECT date, total FROM {upstream.data.name}" for upstream in orders)
```

It takes the settings `asset` takes, plus ``dialect`` to pin raw SQL to one
warehouse dialect, and returns a View subclass with the function as its
``data()``.
"""


def reference(*, table: str, **overrides: Any) -> Decorator[Reference]:
    """Create a Reference subclass from a decorated function: an asset over a table that already exists.

    Like ``il.asset``, but the function has nothing to compute: its name is
    the asset key and its docstring the description, and its body stays
    empty, since a reference is never run. The table and the dataset may
    name a field of the owning source in braces, filled per instance.

    ```py
    @il.source
    class Export(il.Source):
        property_id: str = il.InputField(discriminator=True)

        @il.reference(table="events_*", dataset="analytics_{property_id}", schema=Events)
        def events(self) -> None:
            \"\"\"The property's raw export, read and never written.\"\"\"
    ```

    Args:
        table: The physical table name, used as written, wildcard and all.
        **overrides: Definition metadata and behaviour as for `asset`
            (``schema``, ``tags``, ``dataset``, ``partitioning``, ...).

    Returns:
        A decorator turning the function into a Reference subclass.
    """
    return Decorator(Reference, table_name=table, **overrides)
