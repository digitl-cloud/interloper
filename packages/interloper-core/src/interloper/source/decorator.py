"""Decorator for creating Source subclasses from classes or functions."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any, overload

from interloper.asset import Asset
from interloper.component.build import build_component_class
from interloper.destination import Destination
from interloper.normalizer import MaterializationStrategy, Normalizer
from interloper.resource import Resource
from interloper.source.base import Source

# Mapping from decorator kwarg name → (namespace key, "classvar" | "field").
_SOURCE_PARAMS: dict[str, tuple[str, str]] = {
    "resources": ("resource_types", "classvar"),
    "destinations": ("destination_types", "classvar"),
    "tags": ("tags", "classvar"),
    "key": ("key", "classvar"),
    "name": ("name", "classvar"),
    "icon": ("icon", "classvar"),
    "dataset": ("dataset", "field"),
    "default_destination_key": ("default_destination_key", "field"),
    "materializable": ("materializable", "field"),
    "normalizer": ("normalizer", "field"),
    "materialization_strategy": ("materialization_strategy", "field"),
}


@overload
def source(target: type | Callable[..., Any], /) -> type[Source]: ...
@overload
def source(  # type: ignore[reportInconsistentOverload]
    *,
    resources: dict[str, type[Resource]] = ...,
    destinations: list[type[Destination]] = ...,
    tags: list[str] = ...,
    key: str = ...,
    name: str = ...,
    icon: str = ...,
    dataset: str = ...,
    default_destination_key: str = ...,
    materializable: bool = ...,
    normalizer: Normalizer | None = ...,
    materialization_strategy: MaterializationStrategy = ...,
) -> Callable[[type | Callable[..., Any]], type[Source]]: ...
def source(  # type: ignore[reportInconsistentOverload]
    target: type | Callable[..., Any] | None = None,
    /,
    *,
    resources: dict[str, type[Resource]] | None = None,
    destinations: list[type[Destination]] | None = None,
    tags: list[str] | None = None,
    key: str | None = None,
    name: str | None = None,
    icon: str | None = None,
    dataset: str | None = None,
    default_destination_key: str | None = None,
    materializable: bool | None = None,
    normalizer: Normalizer | None = None,
    materialization_strategy: MaterializationStrategy | None = None,
) -> type[Source] | Callable[..., type[Source]]:
    """Create a Source subclass from a decorated class or function.

    Returns:
        A Source subclass with discovered assets.
    """
    kwargs = {
        "resources": resources,
        "destinations": destinations,
        "tags": tags,
        "key": key,
        "name": name,
        "icon": icon,
        "dataset": dataset,
        "default_destination_key": default_destination_key,
        "materializable": materializable,
        "normalizer": normalizer,
        "materialization_strategy": materialization_strategy,
    }
    classvars, fields = _split_params(kwargs, _SOURCE_PARAMS)

    if target is not None:
        return _build_source(target, classvars=classvars, fields=fields)

    def wrapper(target: type | Callable[..., Any]) -> type[Source]:
        return _build_source(target, classvars=classvars, fields=fields)

    return wrapper


def _split_params(
    kwargs: dict[str, Any],
    param_map: dict[str, tuple[str, str]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Split decorator kwargs into classvars and fields dicts.

    Skips ``None`` values (unset parameters).

    Args:
        kwargs: Raw decorator keyword arguments.
        param_map: Maps kwarg name → (namespace key, "classvar" | "field").

    Returns:
        A (classvars, fields) tuple.
    """
    classvars: dict[str, Any] = {}
    fields: dict[str, Any] = {}
    for kwarg_name, value in kwargs.items():
        if value is None:
            continue
        ns_key, kind = param_map[kwarg_name]
        if kind == "classvar":
            classvars[ns_key] = value
        else:
            fields[ns_key] = value
    return classvars, fields


def _build_source(
    target: type | Callable[..., Any],
    *,
    classvars: dict[str, Any],
    fields: dict[str, Any],
) -> type[Source]:
    """Route to class-based or function-based builder.

    Returns:
        A dynamically created Source subclass.
    """
    if inspect.isclass(target):
        return _build_source_from_class(target, classvars=classvars, fields=fields)
    return _build_source_from_fn(target, classvars=classvars, fields=fields)


def _build_source_from_fn(
    fn: Callable[..., Any],
    *,
    classvars: dict[str, Any],
    fields: dict[str, Any],
) -> type[Source]:
    """Build a Source subclass from a decorated function.

    Calls the function and expects it to return a list of ``type[Asset]``.

    Returns:
        A dynamically created Source subclass.
    """
    result = fn()
    assets: list[type[Asset]] = []
    if isinstance(result, list):
        assets = result
    elif isinstance(result, type) and issubclass(result, Asset):
        assets = [result]

    namespace: dict[str, Any] = {"asset_types": assets}
    namespace.update(classvars)
    namespace.update(fields)
    namespace["__module__"] = fn.__module__
    namespace["__qualname__"] = fn.__qualname__  # type: ignore[attr-defined]

    annotations: dict[str, Any] = {}
    for attr in fields:
        if attr in Source.model_fields:
            annotations[attr] = Source.model_fields[attr].annotation

    # Extract config fields from function signature annotations.
    # Parameters with Field helpers (InputField, FetchField, etc.) become
    # Pydantic fields on the Source class, rendered as the config form.
    sig = inspect.signature(fn)
    fn_annotations = fn.__annotations__ if hasattr(fn, "__annotations__") else {}
    for param_name, param in sig.parameters.items():
        if param_name in ("self", "context", "kwargs"):
            continue
        if param_name in fn_annotations:
            annotations[param_name] = fn_annotations[param_name]
            if param.default is not inspect.Parameter.empty:
                namespace[param_name] = param.default

    if annotations:
        namespace["__annotations__"] = annotations

    source_cls = type(fn.__name__, (Source,), namespace)  # type: ignore[attr-defined]

    if fn.__doc__:
        source_cls.__doc__ = fn.__doc__

    for asset_cls in assets:
        asset_cls._source_type = source_cls

    return source_cls


def _build_source_from_class(
    cls: type,
    *,
    classvars: dict[str, Any],
    fields: dict[str, Any],
) -> type[Source]:
    """Build a Source subclass from a decorated class.

    Asset subclasses defined in the class body (via ``@asset`` on methods)
    are auto-collected by ``Source.__init_subclass__._collect_asset_types()``.

    Returns:
        A dynamically created Source subclass.
    """
    merged_classvars = dict(classvars)

    annotations: dict[str, Any] = {}
    for attr in fields:
        if attr in Source.model_fields:
            annotations[attr] = Source.model_fields[attr].annotation

    # We need to inject field annotations; build_component_class merges
    # cls.__dict__["__annotations__"], but we also need the field-type
    # annotations.  Easiest to add them to classvars as __annotations__.
    if annotations:
        merged_classvars.setdefault("__annotations__", {}).update(annotations)

    source_cls = build_component_class(cls, base=Source, classvars=merged_classvars, fields=fields)

    # Set _source_type backref on each asset.
    for asset_cls in source_cls.asset_types:
        asset_cls._source_type = source_cls

    return source_cls
