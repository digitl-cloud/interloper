"""Decorator for creating Asset subclasses from functions."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any, overload

from interloper.asset.base import Asset
from interloper.destination import Destination
from interloper.normalizer import MaterializationStrategy, Normalizer
from interloper.partitioning import PartitionConfig
from interloper.resource import Resource
from interloper.schema import Schema


@overload
def asset(fn: Callable[..., Any], /) -> type[Asset]: ...
@overload
def asset(
    *,
    resources: dict[str, type[Resource]] = ...,
    destinations: list[type[Destination]] = ...,
    schema: type[Schema] | None = ...,
    partitioning: PartitionConfig | None = ...,
    requires: dict[str, str] = ...,
    optional_requires: dict[str, str] = ...,
    tags: list[str] = ...,
    key: str = ...,
    name: str = ...,
    icon: str = ...,
    materialization_strategy: MaterializationStrategy = ...,
    normalizer: Normalizer | None = ...,
) -> Callable[[Callable[..., Any]], type[Asset]]: ...
def asset(
    fn: Callable[..., Any] | None = None,
    /,
    *,
    resources: dict[str, type[Resource]] | None = None,
    destinations: list[type[Destination]] | None = None,
    schema: type[Schema] | None = None,
    partitioning: PartitionConfig | None = None,
    requires: dict[str, str] | None = None,
    optional_requires: dict[str, str] | None = None,
    tags: list[str] | None = None,
    key: str | None = None,
    name: str | None = None,
    icon: str | None = None,
    materialization_strategy: MaterializationStrategy | None = None,
    normalizer: Normalizer | None = None,
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

        @asset(resources={"config": MyConfig, "connection": MyConn})
        def other(config: MyConfig, connection: MyConn) -> Any:
            return fetch_other()

    Args:
        fn: The function to turn into an asset, passed positionally when the
            decorator is used bare. ``None`` in the parenthesised form, which
            returns a decorator instead.
        resources: Resource types keyed by ``data()`` parameter name. Explicit
            declarations win over the types inferred from annotations.
        destinations: Destination types the asset is allowed to write to.
        schema: The asset's output schema. ``None`` leaves it undeclared, so
            AUTO infers one at materialization.
        partitioning: Partition config for the asset. ``None`` means unpartitioned.
        requires: Mandatory upstream dependencies, keyed by ``data()`` parameter
            name, valued by asset key (bare or qualified).
        optional_requires: Same shape as ``requires``, but unresolved
            dependencies pass ``None`` instead of failing.
        tags: Catalog tags for the asset (e.g. ``["Report"]``).
        key: Asset key. Defaults to the decorated function's name.
        name: Human-readable display name. Defaults to a label built from the key.
        icon: Icon identifier shown in the UI.
        materialization_strategy: How the data is checked against the schema.
        normalizer: Normalizer applied to the data before conform.

    Returns:
        An Asset subclass with the function as its ``data()`` method.
    """
    classvars: dict[str, Any] = {}
    fields: dict[str, Any] = {}

    if destinations is not None:
        classvars["destination_types"] = destinations
    if schema is not None:
        classvars["schema"] = schema
    if partitioning is not None:
        classvars["partitioning"] = partitioning
    if tags is not None:
        classvars["tags"] = tags
    if key is not None:
        classvars["key"] = key
    if name is not None:
        classvars["name"] = name
    if icon is not None:
        classvars["icon"] = icon
    if resources is not None:
        classvars["resource_types"] = resources
    if requires is not None:
        classvars["requires"] = requires
    if optional_requires is not None:
        classvars["optional_requires"] = optional_requires

    if materialization_strategy is not None:
        fields["materialization_strategy"] = materialization_strategy
    if normalizer is not None:
        fields["normalizer"] = normalizer

    if fn is not None:
        return _build_asset_class(fn, classvars=classvars, fields=fields)

    def wrapper(fn: Callable[..., Any]) -> type[Asset]:
        return _build_asset_class(fn, classvars=classvars, fields=fields)

    return wrapper


def _build_asset_class(
    fn: Any,
    *,
    classvars: dict[str, Any],
    fields: dict[str, Any],
) -> type[Asset]:
    """Build an Asset subclass from a function or method.

    If the function's first parameter is ``self``, the asset is treated
    as a **method asset** — at materialization time, the source instance
    is passed as ``self``.  Otherwise it's a standalone function asset.

    Explicit resource declarations from decorator kwargs are passed through.
    Annotation-based inference is handled by ``Asset.__init_subclass__``.

    Args:
        fn: The sync or async function (or method) backing the asset's ``data()``.
        classvars: Class-level attributes to set on the generated subclass
            (``key``, ``tags``, ``resource_types``, …).
        fields: Pydantic field values to set on the generated subclass, annotated
            from ``Asset.model_fields``.

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
                return await fn(self._source, **kwargs)
        else:

            def data(self: Asset, **kwargs: Any) -> Any:
                return fn(self._source, **kwargs)

        data.__signature__ = data_sig  # ty: ignore[invalid-assignment]
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

        data.__signature__ = data_sig  # ty: ignore[invalid-assignment]

    namespace: dict[str, Any] = {"data": data, **classvars, **fields}
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
