"""Decorator for creating Source subclasses from classes or functions."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any, overload

from interloper.asset import Asset
from interloper.component.decorator import declare, decorate
from interloper.component.relation import Relation
from interloper.source.base import Source


@overload
def source(target: type | Callable[..., Any], /) -> type[Source]: ...
@overload
def source(
    *,
    relations: dict[str, Any] = ...,
    **overrides: Any,
) -> Callable[[type | Callable[..., Any]], type[Source]]: ...
def source(
    target: type | Callable[..., Any] | None = None,
    /,
    *,
    relations: dict[str, Any] | None = None,
    **overrides: Any,
) -> type[Source] | Callable[..., type[Source]]:
    """Create a Source subclass from a decorated class or function.

    A decorated class declares its configuration fields, its assets (methods
    carrying ``@asset``) and its helpers; a decorated function returns the
    asset classes and turns its own annotated parameters into configuration
    fields::

        @source(tags=["Advertising"], dataset="raw_shop", relations={"connection": ShopConnection})
        class Shop(Source): ...

    Args:
        target: The decorated class or function when used bare; ``None`` when
            used with arguments, in which case a decorator is returned.
        relations: Relation name to a
            :class:`~interloper.component.relation.Relation`, a component class
            (the shorthand for a relation on it), or a list of component
            classes narrowing the relation
            :class:`~interloper.source.base.Source` declares under that name.
            Explicit declarations win over the ones read from the class
            annotations.
        **overrides: Definition metadata and behaviour: the public ClassVars
            and field defaults :class:`~interloper.source.base.Source` declares
            (``key``, ``name``, ``icon``, ``tags``, ``dataset``,
            ``default_destination_key``, ``normalizer``,
            ``materialization_strategy``, ...); see the class. An unknown name
            is a ``TypeError`` at decoration.

    Returns:
        A Source subclass with discovered assets.
    """
    if target is not None:
        return decorate(Source, target, build=_build_source, relations=relations, **overrides)

    def wrapper(target: type | Callable[..., Any]) -> type[Source]:
        return decorate(Source, target, build=_build_source, relations=relations, **overrides)

    return wrapper


# -- Internals -----------------------------------------------------------------


def _build_source(
    target: type | Callable[..., Any],
    *,
    classvars: dict[str, Any],
    fields: dict[str, Any],
    relations: dict[str, Relation],
) -> type[Source]:
    """Route to the class-based or function-based builder, then declare the relations.

    Args:
        target: The decorated class or function.
        classvars: Class-level attributes to stamp on the built class.
        fields: Field default overrides for the built class.
        relations: Relations the decorator declares, name to relation.

    Returns:
        A dynamically created Source subclass.
    """
    if inspect.isclass(target):
        source_cls = _build_source_from_class(target, classvars=classvars, fields=fields)
    else:
        source_cls = _build_source_from_fn(target, classvars=classvars, fields=fields)
    return declare(source_cls, relations)


def _build_source_from_fn(
    fn: Callable[..., Any],
    *,
    classvars: dict[str, Any],
    fields: dict[str, Any],
) -> type[Source]:
    """Build a Source subclass from a decorated function.

    Calls the function and expects it to return a list of ``type[Asset]``.

    Args:
        fn: The decorated function. Its annotated parameters become config
            fields on the built class, and its return value supplies the assets.
        classvars: Class-level attributes to stamp on the built class.
        fields: Field default overrides for the built class.

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
    namespace["__qualname__"] = fn.__qualname__  # ty: ignore[unresolved-attribute]

    annotations: dict[str, Any] = {}
    for field_name in fields:
        if field_name in Source.model_fields:
            annotations[field_name] = Source.model_fields[field_name].annotation

    # Extract config fields from function signature annotations.
    # Parameters with Field helpers (InputField, FetchField, etc.) become
    # Pydantic fields on the Source class, rendered as the config form.
    signature = inspect.signature(fn)
    fn_annotations = fn.__annotations__ if hasattr(fn, "__annotations__") else {}
    for parameter_name, parameter in signature.parameters.items():
        if parameter_name in ("self", "context", "kwargs"):
            continue
        if parameter_name in fn_annotations:
            annotations[parameter_name] = fn_annotations[parameter_name]
            if parameter.default is not inspect.Parameter.empty:
                namespace[parameter_name] = parameter.default

    if annotations:
        namespace["__annotations__"] = annotations

    source_cls = type(fn.__name__, (Source,), namespace)  # ty: ignore[unresolved-attribute]

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
    Field annotations are handled by ``Source.build_class``.

    Args:
        cls: The decorated class.
        classvars: Class-level attributes to stamp on the built class.
        fields: Field default overrides for the built class.

    Returns:
        A dynamically created Source subclass.
    """
    source_cls = Source.build_class(cls, classvars=classvars, fields=fields)

    for asset_cls in source_cls.asset_types:
        asset_cls._source_type = source_cls

    return source_cls
