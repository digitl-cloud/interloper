"""The ibis_view decorator: an IbisView subclass from a decorated function."""

from __future__ import annotations

from interloper.component import Decorator

from interloper_sql.view.base import IbisView


class IbisViewDecorator(Decorator[IbisView]):
    """Create an IbisView subclass from a decorated function.

    Like ``il.view``, but portable: each upstream parameter receives a typed
    ``ibis.Table`` in ``data``, the function returns an Ibis expression, and the
    destination compiles it for its own dialect:

    ```py
    @ibis_view(schema=Totals, relations={"orders": il.Relation("asset", "shop.orders", many=True)})
    def totals(orders: list[il.Upstream]) -> ibis.Table:
        rows = union(orders)
        return rows.group_by(rows.date).aggregate(total=rows.total.sum())
    ```

    It takes the settings ``il.asset`` takes (a schema is required) and returns an
    IbisView subclass with the function as its ``data()``.
    """


ibis_view = IbisViewDecorator(IbisView)
