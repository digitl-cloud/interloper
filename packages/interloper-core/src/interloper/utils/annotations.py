"""Read class annotations and validate values against them."""

from __future__ import annotations

import sys
from functools import cache
from typing import Any, ClassVar, get_args, get_origin

from pydantic import ConfigDict, PydanticSchemaGenerationError, TypeAdapter


def is_classvar(hint: Any) -> bool:
    """Whether an annotation, evaluated or still a string, is a ``ClassVar``.

    Both forms occur: modules importing ``annotations`` from ``__future__``
    hold their annotations as strings, while a class built at runtime can
    carry a bare ``ClassVar`` object.

    Args:
        hint: The annotation as written.

    Returns:
        True when the annotation is ``ClassVar``, subscripted or bare, however
        the ``typing`` module it comes from is spelled.
    """
    if hint is ClassVar or get_origin(hint) is ClassVar:
        return True
    if isinstance(hint, str):
        return hint.partition("[")[0].rpartition(".")[2].strip() == "ClassVar"
    return False


def classvar_type(hint: Any, owner: type) -> Any:
    """The type a ``ClassVar`` annotation declares, resolved in its owner's module.

    Args:
        hint: The ``ClassVar`` annotation, evaluated or still a string.
        owner: The class whose body holds the annotation; its module and
            namespace resolve the names a string annotation uses.

    Returns:
        The declared type, or ``None`` for a bare ``ClassVar`` or an annotation
        that cannot be resolved (a name imported only for type checking, or
        not yet defined while its module is still loading).
    """
    if isinstance(hint, str):
        module = sys.modules.get(owner.__module__)
        try:
            hint = eval(hint, dict(vars(module)) if module else {}, dict(vars(owner)))
        except (NameError, AttributeError, SyntaxError, TypeError):
            return None
    arguments = get_args(hint)
    return arguments[0] if arguments else None


@cache
def type_adapter(declared: Any) -> TypeAdapter[Any]:
    """A cached validator for a declared type.

    Types Pydantic cannot describe on its own (plain classes) are validated
    by instance check.

    Args:
        declared: The type to validate against; must be hashable.

    Returns:
        The adapter, built once per type.
    """
    try:
        return TypeAdapter(declared)
    except PydanticSchemaGenerationError:
        return TypeAdapter(declared, config=ConfigDict(arbitrary_types_allowed=True))
