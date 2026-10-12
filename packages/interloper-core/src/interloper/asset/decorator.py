"""Decorator for creating Asset subclasses from functions."""

from __future__ import annotations

import functools
import inspect
from collections.abc import Callable
from typing import Any, cast, overload

from interloper.asset.base import Asset
from interloper.asset.reference import Reference
from interloper.asset.view import View
from interloper.component.decorator import decorate
from interloper.component.relation import Relation


@overload
def asset(fn: Callable[..., Any], /) -> type[Asset]: ...
@overload
def asset(
    *,
    relations: dict[str, Any] = ...,
    **overrides: Any,
) -> Callable[[Callable[..., Any]], type[Asset]]: ...
def asset(
    fn: Callable[..., Any] | None = None,
    /,
    *,
    relations: dict[str, Any] | None = None,
    **overrides: Any,
) -> type[Asset] | Callable[..., type[Asset]]:
    """Create an Asset subclass from a decorated function.

    The decorated function may be sync or ``async``. Sync functions are
    offloaded to a worker thread at materialization time; ``async`` functions
    are awaited natively on the event loop:

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

    Every parameter of the decorated function declares a relation, inferred
    from its annotation (see `_collect`);
    ``relations`` is for the declarations an annotation cannot express, such as
    a cross-source or many-valued upstream.

    Args:
        fn: The function to turn into an asset, passed positionally when the
            decorator is used bare. ``None`` in the parenthesised form, which
            returns a decorator instead.
        relations: Relation name, keyed by ``data()`` parameter name, to a
            `Relation`, a component class
            (the shorthand for a relation on it), or a list of component
            classes narrowing the relation `Asset`
            declares under that name. Explicit declarations win over the ones
            inferred from the annotations.
        **overrides: Definition metadata and behaviour: the public ClassVars
            and field defaults `Asset` declares
            (``key``, ``name``, ``icon``, ``tags``, ``schema``,
            ``partitioning``, ``dataset``, ``normalizer``,
            ``materialization_strategy``, ...); see the class. An unknown name
            is a ``TypeError`` at decoration.

    Returns:
        An Asset subclass with the function as its ``data()`` method.
    """
    if fn is not None:
        return decorate(Asset, fn, build=build_asset_class, relations=relations, **overrides)

    def wrapper(fn: Callable[..., Any]) -> type[Asset]:
        return decorate(Asset, fn, build=build_asset_class, relations=relations, **overrides)

    return wrapper


@overload
def view(fn: Callable[..., Any], /) -> type[View]: ...
@overload
def view(
    *,
    relations: dict[str, Any] = ...,
    **overrides: Any,
) -> Callable[[Callable[..., Any]], type[View]]: ...
def view(
    fn: Callable[..., Any] | None = None,
    /,
    *,
    relations: dict[str, Any] | None = None,
    **overrides: Any,
) -> type[View] | Callable[..., type[View]]:
    """Create a View subclass from a decorated function.

    Like `asset`, but the function returns the view's definition instead of
    its rows: SQL text in the destination's dialect, or anything the
    destination compiles. Each upstream parameter receives the destination's
    reference to that upstream in ``data``:

    ```py
    @il.view(relations={"orders": il.Relation("asset", "shop.orders", many=True)})
    def order_totals(orders: list[il.Upstream]) -> str:
        return " UNION ALL ".join(f"SELECT date, total FROM {upstream.data.name}" for upstream in orders)
    ```

    Args:
        fn: The function to turn into a view, passed positionally when the
            decorator is used bare. ``None`` in the parenthesised form.
        relations: As for `asset`.
        **overrides: As for `asset`, plus ``dialect`` to pin raw SQL to one
            warehouse dialect.

    Returns:
        A View subclass with the function as its ``data()`` method.
    """
    if fn is not None:
        return decorate(View, fn, build=_build_view_class, relations=relations, **overrides)

    def wrapper(fn: Callable[..., Any]) -> type[View]:
        return decorate(View, fn, build=_build_view_class, relations=relations, **overrides)

    return wrapper


def reference(*, table: str, **overrides: Any) -> Callable[[Callable[..., Any]], type[Reference]]:
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

    def wrapper(fn: Callable[..., Any]) -> type[Reference]:
        return decorate(Reference, fn, build=_build_reference_class, table_name=table, **overrides)

    return wrapper


def build_asset_class(
    fn: Any,
    *,
    base: type[Asset] = Asset,
    classvars: dict[str, Any],
    fields: dict[str, Any],
    relations: dict[str, Relation],
) -> type[Asset]:
    """Build an Asset subclass from a function or method.

    If the function's first parameter is ``self``, the asset is treated
    as a **method asset**: at materialization time, the source instance
    is passed as ``self``.  Otherwise it's a standalone function asset.

    The generated ``data()`` is a ``**kwargs`` wrapper that translates ``self``
    (the asset) into what the function expects, so it wraps the function in
    the `functools.wraps` sense: relation inference resolves the
    original's annotations through ``__wrapped__`` (see
    `_collect`), and the stamped
    ``__signature__`` is what the parameters read as.

    Args:
        fn: The sync or async function (or method) backing the asset's ``data()``.
        base: The class the generated subclass extends: `Asset`, or a subclass
            of it such as `View`.
        classvars: Class-level attributes to set on the generated subclass
            (``key``, ``tags``, ``schema``, …).
        fields: Pydantic field values to set on the generated subclass, annotated
            from ``base.model_fields``.
        relations: Relations the decorator declares, name to relation. They go
            into the class body, which is what makes them win over the
            relations inferred from the ``data()`` annotations.

    Returns:
        A dynamically created subclass of *base*.
    """
    fn_signature = inspect.signature(fn)
    fn_params = list(fn_signature.parameters.keys())
    is_method = len(fn_params) > 0 and fn_params[0] == "self"

    is_async = inspect.iscoroutinefunction(fn)

    if is_method:
        # Method asset: signature already has `self`, keep as-is for
        # resource inference. The `data()` wrapper passes the source
        # instance as the first positional argument.
        data_sig = fn_signature

        if is_async:

            async def data(self: Asset, **kwargs: Any) -> Any:
                return await fn(self.source, **kwargs)
        else:

            def data(self: Asset, **kwargs: Any) -> Any:
                return fn(self.source, **kwargs)

    else:
        # Standalone function asset: prepend `self` for bound method compat.
        self_param = inspect.Parameter("self", inspect.Parameter.POSITIONAL_OR_KEYWORD)
        data_sig = fn_signature.replace(parameters=[self_param, *fn_signature.parameters.values()])

        if is_async:

            async def data(self: Asset, **kwargs: Any) -> Any:
                return await fn(**kwargs)
        else:

            def data(self: Asset, **kwargs: Any) -> Any:
                return fn(**kwargs)

    functools.update_wrapper(data, fn)
    data.__signature__ = data_sig  # ty: ignore[invalid-assignment]

    namespace: dict[str, Any] = {"data": data, **classvars, **fields}
    if relations:
        namespace["relations"] = relations
    namespace["__module__"] = fn.__module__
    namespace["__qualname__"] = fn.__qualname__

    annotations: dict[str, Any] = {}
    for field_name in fields:
        if field_name in base.model_fields:
            annotations[field_name] = base.model_fields[field_name].annotation
    if annotations:
        namespace["__annotations__"] = annotations

    cls = type(fn.__name__, (base,), namespace)

    if fn.__doc__:
        cls.__doc__ = fn.__doc__

    return cls


# -- Internals -----------------------------------------------------------------


def _build_view_class(
    fn: Any,
    *,
    classvars: dict[str, Any],
    fields: dict[str, Any],
    relations: dict[str, Relation],
) -> type[View]:
    """Build a View subclass from a function or method.

    Args:
        fn: The function (or method) backing the view's ``data()``.
        classvars: Class-level attributes to set on the generated subclass.
        fields: Pydantic field values to set on the generated subclass.
        relations: Relations the decorator declares, name to relation.

    Returns:
        A dynamically created View subclass.
    """
    return cast("type[View]", build_asset_class(fn, base=View, classvars=classvars, fields=fields, relations=relations))


def _build_reference_class(
    fn: Any,
    *,
    classvars: dict[str, Any],
    fields: dict[str, Any],
    relations: dict[str, Relation],
) -> type[Reference]:
    """Build a Reference subclass from a function or method.

    Args:
        fn: The function (or method) the reference is declared on; never run.
        classvars: Class-level attributes to set on the generated subclass (``table_name`` among them).
        fields: Pydantic field values to set on the generated subclass.
        relations: Relations the decorator declares, name to relation.

    Returns:
        A dynamically created Reference subclass.
    """
    return cast(
        "type[Reference]",
        build_asset_class(fn, base=Reference, classvars=classvars, fields=fields, relations=relations),
    )
