"""Decorator for creating Destination subclasses from plain classes."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar, overload

from interloper.component.decorator import declare, decorate
from interloper.component.relation import Relation
from interloper.destination.base import Destination

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
    relations: dict[str, Any] = ...,
    **overrides: Any,
) -> Callable[[type[DestinationT]], type[DestinationT]]: ...
def destination(
    cls: type | None = None,
    /,
    *,
    relations: dict[str, Any] | None = None,
    **overrides: Any,
) -> type[Destination] | Callable[[type], type[Destination]]:
    """Create a Destination subclass from a decorated class.

    Relations can be declared via annotations::

        @destination
        class MyDest:
            connection: PostgresConnection

            def read(self, context): ...
            def write(self, context, data): ...

    Or through the ``relations`` channel::

        @destination(name="My destination", relations={"connection": PostgresConnection})
        class MyDest:
            def read(self, context): ...
            def write(self, context, data): ...

    An annotation naming a component class declares a relation rather than a
    Pydantic model field.

    Args:
        cls: The class being decorated when used bare (``@destination``);
            ``None`` when used with keyword arguments, in which case a
            decorator is returned instead.
        relations: Relation name to a
            :class:`~interloper.component.relation.Relation`, a component class
            (the shorthand for a relation on it), or a list of component
            classes narrowing the relation the decorated class declares under
            that name. Explicit declarations win over the class annotations.
        **overrides: Definition metadata and behaviour: the public ClassVars
            and field defaults the decorated class declares, or
            :class:`~interloper.destination.base.Destination` itself for a
            plain class (``key``, ``name``, ``icon``, ``tags``, and
            ``read_representation`` / ``materialization_strategy`` on a
            :class:`~interloper.destination.database.DatabaseDestination`);
            see the class. An unknown name is a ``TypeError`` at decoration.

    Returns:
        A Destination subclass.
    """
    if cls is not None:
        return decorate(Destination, cls, build=_build_destination, relations=relations, **overrides)

    def wrapper(cls: type) -> type[Destination]:
        return decorate(Destination, cls, build=_build_destination, relations=relations, **overrides)

    return wrapper


# -- Internals -----------------------------------------------------------------


def _build_destination(
    cls: type,
    *,
    classvars: dict[str, Any],
    fields: dict[str, Any],
    relations: dict[str, Relation],
) -> type[Destination]:
    """Build the Destination subclass and declare the decorator's relations on it.

    Args:
        cls: The decorated class.
        classvars: Class-level attributes to stamp on the built class.
        fields: Field default overrides for the built class.
        relations: Relations the decorator declares, name to relation.

    Returns:
        A Destination subclass.
    """
    return declare(Destination.build_class(cls, classvars=classvars, fields=fields), relations)
