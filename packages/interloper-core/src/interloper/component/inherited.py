"""Inherited settings: fields an owned component reads through its owner while it leaves them unset.

A field marked ``x-inherit`` (``inherit=True`` on a field factory) stores only
the component's own value, ``None`` while unset. Reading it asks the owner,
through `Component.inherited`, so nothing is copied down and nothing goes
stale; validation, serialization, equality and copies see the stored value
alone, since Pydantic keeps it in the instance ``__dict__``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from interloper.component.base import Component

_MARKER = "x-inherit"


class InheritedSetting:
    """The read-through descriptor installed over one inherited field.

    A data descriptor, so it wins over the instance ``__dict__`` on reads while
    Pydantic keeps writing there: the stored value is the override, and the
    read is the effective value.
    """

    def __init__(self, name: str) -> None:
        """Bind the descriptor to the field it reads.

        Args:
            name: The field name, under which the value is stored.
        """
        self.name = name

    def __get__(self, instance: Component | None, owner: type | None = None) -> Any:
        """Read the effective value: the owner decides, or the stored value stands.

        Args:
            instance: The component read, or ``None`` for a read on the class.
            owner: The class the descriptor is installed on; unused, part of
                the descriptor protocol. Defaults to ``None``.

        Returns:
            On an owned component, what its owner hands it (see
            `Component.inherited`), given the stored value with a blank string
            read as unset; on a standalone one, the stored value.

        Raises:
            AttributeError: On the class, as for any Pydantic field: Pydantic
                reads a subclass's inherited defaults through ``getattr`` on
                its bases, and a class-level value here would replace them.
        """
        if instance is None:
            raise AttributeError(self.name)
        own = instance.__dict__.get(self.name)
        if own == "":
            own = None
        parent = instance.parent
        if parent is None:
            return own
        return parent.inherited(instance, self.name, own)

    def __set__(self, instance: Component, value: Any) -> None:
        """Store an own value, as Pydantic does for a field assignment.

        Args:
            instance: The component written to.
            value: The new own value; ``None`` means unset.
        """
        instance.__dict__[self.name] = value

    @classmethod
    def install(cls, component: type[Component]) -> None:
        """Install the descriptor over every inherited field of a component class.

        A field is inherited when any class in the MRO marks it ``x-inherit``.
        A decorator setting (``@il.asset(dataset=...)``) copies the field's
        ``FieldInfo`` and keeps the marker; a subclass redeclaring the field
        by hand carries a fresh one without it, and must keep reading through
        its owner, hence the walk over the MRO.

        Args:
            component: The class Pydantic has just built.
        """
        marked = {
            name
            for base in component.__mro__
            for name, field in getattr(base, "__pydantic_fields__", {}).items()
            if isinstance(field.json_schema_extra, dict) and field.json_schema_extra.get(_MARKER)
        }
        for name in marked & component.model_fields.keys():
            setattr(component, name, cls(name))
