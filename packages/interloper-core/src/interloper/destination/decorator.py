"""Decorator for creating Destination subclasses from plain classes."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar, overload

from interloper.component import Relation
from interloper.destination.base import Destination
from interloper.normalizer import MaterializationStrategy

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
    relations: dict[str, Relation] = ...,
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
    relations: dict[str, Relation] | None = None,
    key: str | None = None,
    tags: list[str] | None = None,
    name: str | None = None,
    icon: str | None = None,
    read_representation: str | None = None,
    materialization_strategy: MaterializationStrategy | None = None,
) -> type[Destination] | Callable[[type], type[Destination]]:
    """Create a Destination subclass from a decorated class.

    Relations can be declared via annotations::

        @destination
        class MyDest:
            connection: PostgresConnection

            def read(self, context): ...
            def write(self, context, data): ...

    Or via explicit kwargs::

        @destination(relations={"connection": il.Relation(PostgresConnection)})
        class MyDest:
            def read(self, context): ...
            def write(self, context, data): ...

    An annotation naming a component class declares a relation rather than a
    Pydantic model field.

    Args:
        cls: The class being decorated when used bare (``@destination``);
            ``None`` when used with keyword arguments, in which case a
            decorator is returned instead.
        relations: Relation name → :class:`~interloper.component.relation.Relation`,
            an alternative to declaring relations as class annotations.
        key: Registry key for the destination; defaults to the base class's.
        tags: Tags surfaced in the destination's definition.
        name: Human-readable name; defaults to a label built from the class name.
        icon: Icon identifier surfaced in the destination's definition.
        read_representation: Name of the representation reads materialize into
            (e.g. ``"rows"``, ``"dataframe"``).
        materialization_strategy: Default write-time schema strategy, overridable
            per configured destination.

    Returns:
        A Destination subclass.
    """
    classvars: dict[str, Any] = {}
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
        return _build_destination(cls, classvars=classvars, fields=fields, relations=relations)

    def wrapper(cls: type) -> type[Destination]:
        return _build_destination(cls, classvars=classvars, fields=fields, relations=relations)

    return wrapper


# -- Internals -----------------------------------------------------------------


def _build_destination(
    cls: type,
    *,
    classvars: dict[str, Any],
    fields: dict[str, Any],
    relations: dict[str, Relation] | None,
) -> type[Destination]:
    """Build the Destination subclass and declare the decorator's relations on it.

    Args:
        cls: The decorated class.
        classvars: Class-level attributes to stamp on the built class.
        fields: Field default overrides for the built class.
        relations: Relations the decorator declares, name to relation, or
            ``None`` when the decorator declares none.

    Returns:
        A Destination subclass.
    """
    destination_cls = Destination.build_class(cls, classvars=classvars, fields=fields)
    if relations:
        # The class already collected its relations when it was created, before
        # the decorator had a chance to add any, so it collects again.
        destination_cls.relations = {**destination_cls.relations, **relations}
        destination_cls.collect()
    return destination_cls
