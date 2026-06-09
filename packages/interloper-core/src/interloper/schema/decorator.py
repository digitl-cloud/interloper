"""Decorator for creating Schema subclasses from plain classes."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, overload

from interloper.component.build import build_component_class
from interloper.schema.base import Schema


@overload
def schema(cls: type, /) -> type[Schema]: ...
@overload
def schema(
    *,
    key: str = ...,
    name: str = ...,
) -> Callable[[type], type[Schema]]: ...
def schema(
    cls: type | None = None,
    /,
    *,
    key: str | None = None,
    name: str | None = None,
) -> type[Schema] | Callable[[type], type[Schema]]:
    """Create a Schema subclass from a decorated class.

    Can be used bare or with arguments::

        @schema
        class UserSchema:
            id: int
            name: str
            email: str

        @schema(key="custom", name="Custom Schema")
        class OtherSchema:
            value: float

    The decorated class's annotations and attributes become the Schema
    subclass body.

    Returns:
        A Schema subclass.
    """
    classvars: dict[str, Any] = {}
    if key is not None:
        classvars["key"] = key
    if name is not None:
        classvars["name"] = name

    if cls is not None:
        return build_component_class(cls, base=Schema, classvars=classvars)

    def wrapper(cls: type) -> type[Schema]:
        return build_component_class(cls, base=Schema, classvars=classvars)

    return wrapper
