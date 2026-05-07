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
def asset(  # type: ignore[reportInconsistentOverload]
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
def asset(  # type: ignore[reportInconsistentOverload]
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

    Can be used bare or with arguments::

        @asset
        def users(**kwargs):
            return fetch_users()

        @asset(resources={"config": MyConfig, "connection": MyConn})
        def other(config: MyConfig, connection: MyConn) -> Any:
            return fetch_other()

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

    Returns:
        A dynamically created Asset subclass.
    """
    fn_sig = inspect.signature(fn)
    fn_params = list(fn_sig.parameters.keys())
    is_method = len(fn_params) > 0 and fn_params[0] == "self"

    if is_method:
        # Method asset: signature already has `self`, keep as-is for
        # resource inference. The `data()` wrapper passes the source
        # instance as the first positional arg.
        data_sig = fn_sig

        def data(self: Asset, **kwargs: Any) -> Any:
            source = self._source
            return fn(source, **kwargs)

        data.__signature__ = data_sig  # type: ignore[attr-defined]
    else:
        # Standalone function asset: prepend `self` for bound method compat.
        self_param = inspect.Parameter("self", inspect.Parameter.POSITIONAL_OR_KEYWORD)
        data_sig = fn_sig.replace(parameters=[self_param, *fn_sig.parameters.values()])

        def data(self: Asset, **kwargs: Any) -> Any:  # type: ignore[no-redef]
            return fn(**kwargs)

        data.__signature__ = data_sig  # type: ignore[attr-defined]

    namespace: dict[str, Any] = {"data": data, **classvars, **fields}
    namespace["__module__"] = fn.__module__
    namespace["__qualname__"] = fn.__qualname__

    annotations: dict[str, Any] = {}
    for attr in fields:
        if attr in Asset.model_fields:
            annotations[attr] = Asset.model_fields[attr].annotation
    if annotations:
        namespace["__annotations__"] = annotations

    cls = type(fn.__name__, (Asset,), namespace)

    if fn.__doc__:
        cls.__doc__ = fn.__doc__

    # Mark method assets so the source can detect them
    if is_method:
        cls._is_method_asset = True  # type: ignore[attr-defined]

    return cls
