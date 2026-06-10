"""Shared builder for creating Component subclasses from decorated classes."""

from __future__ import annotations

from typing import Any, ClassVar, TypeVar, cast

from pydantic import create_model

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

    The invariant: **decorators build classes, they never mutate finalized
    ones.**  Field defaults always pass through the Pydantic metaclass so
    they become real field definitions — a plain ``setattr`` on a built
    pydantic class would leave ``model_fields`` (and therefore every
    instance) on the old default.

    Two construction paths:

    - The decorated class already extends *base*: field defaults (when
      present) produce a new subclass via :func:`pydantic.create_model`
      with the parent's annotations; ClassVars are stamped on the result
      (plain class attributes — no pydantic machinery involved).
    - The decorated class does **not** extend *base*: a new class is
      created via ``type()`` that inherits from *base* and carries over the
      decorated class's members and annotations, with decorator fields
      annotated from the base's field definitions.

    Args:
        cls: The decorated class to transform.
        base: The Component subclass to inherit from.
        classvars: Class-level attributes (key, name, tags, …).
        fields: Field default overrides (dataset, normalizer, …); every key
            must be an existing field on *base*.

    Returns:
        A Component subclass.

    Raises:
        TypeError: If a decorator field is not a field of *base*.
    """
    for field_name in fields or {}:
        if field_name not in base.model_fields:
            raise TypeError(f"Decorator field '{field_name}' is not a field of {base.__name__}.")

    already_subclass = any(isinstance(b, type) and issubclass(b, base) for b in cls.__bases__)

    if already_subclass:
        result_cls = cast(type[C], cls)
        if fields:
            field_definitions: dict[str, Any] = {
                name: (result_cls.model_fields[name].annotation, value) for name, value in fields.items()
            }
            result_cls = create_model(
                cls.__name__,
                __base__=result_cls,
                __module__=cls.__module__,
                **field_definitions,
            )
            result_cls.__qualname__ = cls.__qualname__
            result_cls.__doc__ = cls.__doc__
        if classvars:
            for name, value in classvars.items():
                setattr(result_cls, name, value)
        return result_cls

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

    # Annotate decorator fields from the base's field definitions so the
    # metaclass registers them as field default overrides.
    if fields:
        annotations = namespace.setdefault("__annotations__", {})
        for field_name in fields:
            if field_name not in annotations:
                annotations[field_name] = base.model_fields[field_name].annotation

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

    return cast("type[C]", result_cls)
