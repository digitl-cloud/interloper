"""Decorator for creating Destination subclasses from plain classes."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar, overload

from interloper.destination.base import Destination
from interloper.normalizer import MaterializationStrategy
from interloper.resource import Resource

# Bounded TypeVar so that classes already extending Destination preserve their
# specific type through the decorator (e.g. BigQueryDestination stays
# BigQueryDestination, not Destination).  Plain classes fall through to the
# fallback overload and get type[Destination].
DestinationT = TypeVar("DestinationT", bound=Destination)


@overload
def destination(cls: type[DestinationT], /) -> type[DestinationT]: ...
@overload
def destination(cls: type, /) -> type[Destination]: ...
@overload
def destination(
    *,
    resources: dict[str, type[Resource]] = ...,
    key: str = ...,
    tags: list[str] = ...,
    name: str = ...,
    icon: str = ...,
    read_representation: str = ...,
    materialization_strategy: MaterializationStrategy = ...,
) -> Callable[[type[DestinationT]], type[DestinationT]]: ...
def destination(
    cls: type | None = None,
    /,
    *,
    resources: dict[str, type[Resource]] | None = None,
    key: str | None = None,
    tags: list[str] | None = None,
    name: str | None = None,
    icon: str | None = None,
    read_representation: str | None = None,
    materialization_strategy: MaterializationStrategy | None = None,
) -> type[Destination] | Callable[[type], type[Destination]]:
    """Create a Destination subclass from a decorated class.

    Resource dependencies can be declared via annotations::

        @destination
        class MyDest:
            connection: PostgresConnection

            def read(self, context): ...
            def write(self, context, data): ...

    Or via explicit kwargs::

        @destination(resources={"connection": PostgresConnection})
        class MyDest:
            def read(self, context): ...
            def write(self, context, data): ...

    Annotations typed as ``Resource`` subclasses are automatically extracted
    from the class body and converted to ``ResourceRef`` descriptors.  They
    do **not** become Pydantic model fields.

    Returns:
        A Destination subclass.
    """
    classvars: dict[str, Any] = {}
    if resources is not None:
        classvars["resource_types"] = resources
    if read_representation is not None:
        classvars["read_representation"] = read_representation

    fields: dict[str, Any] = {}
    if materialization_strategy is not None:
        fields["materialization_strategy"] = materialization_strategy
    if tags is not None:
        classvars["tags"] = tags
    if key is not None:
        classvars["key"] = key
    if name is not None:
        classvars["name"] = name
    if icon is not None:
        classvars["icon"] = icon

    if cls is not None:
        return Destination.build_class(cls, classvars=classvars, fields=fields)

    def wrapper(cls: type) -> type[Destination]:
        return Destination.build_class(cls, classvars=classvars, fields=fields)

    return wrapper
