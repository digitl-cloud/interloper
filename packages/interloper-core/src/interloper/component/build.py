"""Shared builder for creating Component subclasses from decorated classes."""

from __future__ import annotations

from typing import Any, ClassVar, TypeVar, cast

from interloper.component.base import Component

C = TypeVar("C", bound=Component)


def build_component_class(
    cls: type,
    *,
    base: type[C],
    classvars: dict[str, Any] | None = None,
    fields: dict[str, Any] | None = None,
) -> type[C]:
    """Build a Component subclass from a decorated class.

    When the decorated class already extends the target base (or a subclass
    of it), the decorator only needs to stamp ClassVars onto the existing
    class — no rebuild required.  This preserves Pydantic field descriptors,
    JSON Schema metadata, validators, and everything else the metaclass set
    up during original class creation.

    When the decorated class does **not** extend the base, a new class is
    created via ``type()`` that inherits from *base* and carries over the
    decorated class's members and annotations.

    Args:
        cls: The decorated class to transform.
        base: The Component subclass to inherit from.
        classvars: Class-level attributes (key, name, tags, …).
        fields: Instance-level field defaults (dataset, normalizer, …).

    Returns:
        A Component subclass.
    """
    already_subclass = any(
        isinstance(b, type) and issubclass(b, base) for b in cls.__bases__
    )

    if already_subclass:
        # The class is already properly constructed by Pydantic's metaclass.
        # Just stamp the decorator-provided classvars and fields.
        if classvars:
            for name, value in classvars.items():
                setattr(cls, name, value)
        if fields:
            for name, value in fields.items():
                setattr(cls, name, value)
        return cast(type[C], cls)

    # Plain class — build a new class that inherits from base.
    namespace: dict[str, Any] = {}

    for name, value in cls.__dict__.items():
        if name.startswith("__"):
            continue
        namespace[name] = value

    if classvars:
        namespace.update(classvars)
    if fields:
        namespace.update(fields)

    namespace["__module__"] = cls.__module__
    namespace["__qualname__"] = cls.__qualname__

    # Carry over annotations so Pydantic sees declared fields.
    if "__annotations__" in cls.__dict__:
        namespace.setdefault("__annotations__", {}).update(cls.__dict__["__annotations__"])

    # Annotate classvars as ClassVar so Pydantic doesn't treat them as
    # model fields.  Without this, a bare `tags = ["Cloud"]` in the
    # namespace triggers a PydanticUserError for non-annotated attributes.
    if classvars:
        annotations = namespace.setdefault("__annotations__", {})
        for cv_name in classvars:
            if cv_name not in annotations:
                annotations[cv_name] = ClassVar

    result_cls = type(cls.__name__, (base,), namespace)

    if cls.__doc__:
        result_cls.__doc__ = cls.__doc__

    return cast(type[C], result_cls)
