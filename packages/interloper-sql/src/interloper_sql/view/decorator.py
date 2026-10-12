"""Decorator for creating IbisView subclasses from functions."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast, overload

from interloper.asset.decorator import build_asset_class
from interloper.component.decorator import decorate
from interloper.component.relation import Relation

from interloper_sql.view.base import IbisView


@overload
def ibis_view(fn: Callable[..., Any], /) -> type[IbisView]: ...
@overload
def ibis_view(
    *,
    relations: dict[str, Any] = ...,
    **overrides: Any,
) -> Callable[[Callable[..., Any]], type[IbisView]]: ...
def ibis_view(
    fn: Callable[..., Any] | None = None,
    /,
    *,
    relations: dict[str, Any] | None = None,
    **overrides: Any,
) -> type[IbisView] | Callable[..., type[IbisView]]:
    """Create an IbisView subclass from a decorated function.

    Like ``il.view``, but portable: each upstream parameter receives a typed
    ``ibis.Table`` in ``data``, the function returns an Ibis expression, and
    the destination compiles it for its own dialect:

    ```py
    @ibis_view(schema=Totals, relations={"orders": il.Relation("asset", "shop.orders", many=True)})
    def totals(orders: list[il.Upstream]) -> ibis.Table:
        rows = union(orders)
        return rows.group_by(rows.date).aggregate(total=rows.total.sum())
    ```

    Args:
        fn: The function to turn into a view, passed positionally when the
            decorator is used bare. ``None`` in the parenthesised form.
        relations: As for ``il.asset``.
        **overrides: As for ``il.asset``; a schema is required.

    Returns:
        An IbisView subclass with the function as its ``data()`` method.
    """
    if fn is not None:
        return decorate(IbisView, fn, build=_build_ibis_view_class, relations=relations, **overrides)

    def wrapper(fn: Callable[..., Any]) -> type[IbisView]:
        return decorate(IbisView, fn, build=_build_ibis_view_class, relations=relations, **overrides)

    return wrapper


# -- Internals -----------------------------------------------------------------


def _build_ibis_view_class(
    fn: Any,
    *,
    classvars: dict[str, Any],
    fields: dict[str, Any],
    relations: dict[str, Relation],
) -> type[IbisView]:
    """Build an IbisView subclass from a function or method.

    Args:
        fn: The function (or method) backing the view's ``data()``.
        classvars: Class-level attributes to set on the generated subclass.
        fields: Pydantic field values to set on the generated subclass.
        relations: Relations the decorator declares, name to relation.

    Returns:
        A dynamically created IbisView subclass.
    """
    return cast(
        "type[IbisView]", build_asset_class(fn, base=IbisView, classvars=classvars, fields=fields, relations=relations)
    )
