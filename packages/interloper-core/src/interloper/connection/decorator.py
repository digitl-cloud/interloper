"""Decorator for creating Connection subclasses from plain classes."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar, overload

from interloper.component.build import build_component_class
from interloper.connection.base import Connection

# Bounded TypeVar so that classes already extending Connection preserve their
# specific type through the decorator.  Plain classes fall through to the
# fallback overload and get type[Connection].
ConnectionT = TypeVar("ConnectionT", bound=Connection)


@overload
def connection(cls: type[ConnectionT], /) -> type[ConnectionT]: ...
@overload
def connection(cls: type, /) -> type[Connection]: ...
@overload
def connection(
    *,
    key: str = ...,
    name: str = ...,
    icon: str = ...,
    tags: list[str] = ...,
) -> Callable[[type[ConnectionT]], type[ConnectionT]]: ...  # type: ignore[reportInconsistentOverload]
def connection(  # type: ignore[reportInconsistentOverload]
    cls: type | None = None,
    /,
    *,
    key: str | None = None,
    name: str | None = None,
    icon: str | None = None,
    tags: list[str] | None = None,
) -> type[Connection] | Callable[[type], type[Connection]]:
    """Create a Connection subclass from a decorated class.

    Can be used bare or with arguments::

        @connection
        class MyConnection:
            host: str = "localhost"
            port: int = 5432
            username: str
            password: str

        @connection(key="custom", name="Custom Connection")
        class OtherConnection:
            url: str

    The decorated class's annotations and attributes become the Connection
    subclass body.  Since Connection extends ``BaseSettings``, fields can
    still be loaded from environment variables.

    Returns:
        A Connection subclass.
    """
    classvars: dict[str, Any] = {}
    if key is not None:
        classvars["key"] = key
    if name is not None:
        classvars["name"] = name
    if icon is not None:
        classvars["icon"] = icon
    if tags is not None:
        classvars["tags"] = tags

    if cls is not None:
        return build_component_class(cls, base=Connection, classvars=classvars)

    def wrapper(cls: type) -> type[Connection]:
        return build_component_class(cls, base=Connection, classvars=classvars)

    return wrapper
