"""ClassBody: what a decorated class or function contributes to the class a decorator builds."""

from __future__ import annotations

import inspect
from dataclasses import dataclass, field
from types import FunctionType
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from interloper.serializable.base import Serializable


@dataclass(frozen=True)
class ClassBody:
    """What a decorated class or function contributes to the class a decorator builds.

    A decorator reads its target into a body and builds one new class from it
    (see `Serializable.build_class`). A class already extending the decorator's
    anchor contributes nothing but itself, as the base of the new class; any
    other target contributes members and annotations, and the anchor is the base.
    """

    name: str
    module: str
    qualname: str
    doc: str | None
    members: dict[str, Any] = field(default_factory=dict)
    annotations: dict[str, Any] = field(default_factory=dict)
    base: type[Serializable] | None = None

    @classmethod
    def of(cls, target: Any, anchor: type[Serializable]) -> ClassBody:
        """Read a decorator's target: a class as a class, a function the way the anchor says.

        Args:
            target: The decorated class or function.
            anchor: The kind the decorator builds.

        Returns:
            The body the decorator builds its class from.
        """
        if isinstance(target, type):
            return cls.from_class(target, anchor)
        return anchor.body_from_function(target)

    @classmethod
    def from_class(cls, target: type, anchor: type[Serializable]) -> ClassBody:
        """Read a decorated class.

        Args:
            target: The decorated class.
            anchor: The kind the decorator builds.

        Returns:
            An empty body based on *target* when it already extends *anchor*,
            else its members (dunders excluded) and its own annotations.
        """
        name, module, qualname, doc = target.__name__, target.__module__, target.__qualname__, target.__doc__
        if issubclass(target, anchor):
            return cls(name=name, module=module, qualname=qualname, doc=doc, base=target)
        members = {member: value for member, value in target.__dict__.items() if not member.startswith("__")}
        annotations = dict(inspect.get_annotations(target))
        return cls(name=name, module=module, qualname=qualname, doc=doc, members=members, annotations=annotations)

    @classmethod
    def from_function(
        cls, fn: FunctionType, *, members: dict[str, Any], annotations: dict[str, Any] | None = None
    ) -> ClassBody:
        """Build the body a function contributes, named after the function.

        Args:
            fn: The decorated function.
            members: The members the anchor makes of it (a ``data()``, asset classes, defaults).
            annotations: Annotations of those members, ``None`` for none.

        Returns:
            The body.
        """
        return cls(
            name=fn.__name__,
            module=fn.__module__,
            qualname=fn.__qualname__,
            doc=fn.__doc__,
            members=members,
            annotations=dict(annotations or {}),
        )
