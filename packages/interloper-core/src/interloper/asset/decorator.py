"""Decorator for creating Asset subclasses from functions."""

from __future__ import annotations

import functools
import inspect
from collections.abc import Callable
from typing import Any, overload

from interloper.asset.base import Asset
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
    are awaited natively on the event loop::

        @asset
        def users(**kwargs):
            return fetch_users()

        @asset
        async def events(**kwargs):
            return await fetch_events()

        @asset(tags=["Report"], schema=UsersStats, relations={"destinations": [BigQueryDestination]})
        def users_stats(config: MyConfig, connection: MyConn) -> Any:
            return fetch_stats()

    Every parameter of the decorated function declares a relation, inferred
    from its annotation (see :meth:`~interloper.asset.base.Asset._collect`);
    ``relations`` is for the declarations an annotation cannot express, such as
    a cross-source or many-valued upstream.

    Args:
        fn: The function to turn into an asset, passed positionally when the
            decorator is used bare. ``None`` in the parenthesised form, which
            returns a decorator instead.
        relations: Relation name, keyed by ``data()`` parameter name, to a
            :class:`~interloper.component.relation.Relation`, a component class
            (the shorthand for a relation on it), or a list of component
            classes narrowing the relation :class:`~interloper.asset.base.Asset`
            declares under that name. Explicit declarations win over the ones
            inferred from the annotations.
        **overrides: Definition metadata and behaviour: the public ClassVars
            and field defaults :class:`~interloper.asset.base.Asset` declares
            (``key``, ``name``, ``icon``, ``tags``, ``schema``,
            ``partitioning``, ``dataset``, ``normalizer``,
            ``materialization_strategy``, ...); see the class. An unknown name
            is a ``TypeError`` at decoration.

    Returns:
        An Asset subclass with the function as its ``data()`` method.
    """
    if fn is not None:
        return decorate(Asset, fn, build=_build_asset_class, relations=relations, **overrides)

    def wrapper(fn: Callable[..., Any]) -> type[Asset]:
        return decorate(Asset, fn, build=_build_asset_class, relations=relations, **overrides)

    return wrapper


# -- Internals -----------------------------------------------------------------


def _build_asset_class(
    fn: Any,
    *,
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
    the :func:`functools.wraps` sense: relation inference resolves the
    original's annotations through ``__wrapped__`` (see
    :meth:`~interloper.asset.base.Asset._collect`), and the stamped
    ``__signature__`` is what the parameters read as.

    Args:
        fn: The sync or async function (or method) backing the asset's ``data()``.
        classvars: Class-level attributes to set on the generated subclass
            (``key``, ``tags``, ``schema``, …).
        fields: Pydantic field values to set on the generated subclass, annotated
            from ``Asset.model_fields``.
        relations: Relations the decorator declares, name to relation. They go
            into the class body, which is what makes them win over the
            relations inferred from the ``data()`` annotations.

    Returns:
        A dynamically created Asset subclass.
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
        if field_name in Asset.model_fields:
            annotations[field_name] = Asset.model_fields[field_name].annotation
    if annotations:
        namespace["__annotations__"] = annotations

    cls = type(fn.__name__, (Asset,), namespace)

    if fn.__doc__:
        cls.__doc__ = fn.__doc__

    return cls
