"""The decorator engine: the three channels every component decorator carries.

A decorator is the function form of a class body, and a class body says three
kinds of thing, so :func:`decorate` carries exactly three channels and nothing
else:

1. **Definition metadata and behaviour**, as plain keyword arguments: the
   anchor's public ``ClassVar`` annotations (``key``, ``name``, ``icon``,
   ``tags``, ``schema``, ``partitioning``, ``oauth``,
   ...) and its Pydantic field defaults (``dataset``,
   ``default_destination_key``, ``normalizer``, ``materialization_strategy``,
   ...). :func:`_route` sorts them by introspecting the anchor, so nothing is
   hand-maintained per kind and an unknown name is refused at decoration.
2. **Relations**, as ``relations=``, the only relation channel:
   :func:`_relations` reads a :class:`~interloper.component.relation.Relation`,
   a component class, or a list of component classes narrowing a relation the
   anchor already declares.
3. **The kind's own build step**, as ``build=``: how a function becomes
   ``data()``, how a function returning asset classes becomes a source, how a
   decorated class is subclassed or stamped in place.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, ClassVar, TypeVar, get_origin

from interloper.component.base import Component
from interloper.component.relation import Relation

ComponentT = TypeVar("ComponentT", bound=Component)

# Names carrying the framework's own machinery: never routable, whatever their
# annotation says. `relations` has its own channel, the rest are collected, not
# declared.
_RESERVED = frozenset({"kind", "relations", "internal_fields", "asset_types", "model_config"})

# The identity field is per-instance, so it is a constructor argument and never
# a class-level default.
_IDENTITY_FIELD = "id"


def decorate(
    anchor: type[Component],
    target: Any,
    *,
    build: Callable[..., type[ComponentT]],
    relations: dict[str, Any] | None = None,
    **overrides: Any,
) -> type[ComponentT]:
    """Route a decorator's three channels and hand them to the kind's build step.

    The routing introspects the decorated class when it already extends the
    anchor, and the anchor itself otherwise: a decorated subclass may carry
    ClassVars and fields the anchor never declares (``materialization_strategy``
    on a ``DatabaseDestination``, ``oauth`` on an ``OAuthConnection``), and those
    are legitimate overrides.

    Args:
        anchor: The kind's anchor class, the authority on what the decorator
            accepts and on the relations a narrowing list may narrow.
        target: The decorated function or class, passed through to ``build``.
        build: The kind's build step, called as
            ``build(target, classvars=..., fields=..., relations=...)``.
        relations: The ``relations=`` channel, name to
            :class:`~interloper.component.relation.Relation`, component class,
            or list of component classes. ``None`` declares none.
        **overrides: Definition metadata and behaviour: the anchor's public
            ClassVars and field defaults (see the anchor class for what it
            declares).

    Returns:
        Whatever the build step built, a subclass of the anchor.
    """
    declaring = target if isinstance(target, type) and issubclass(target, anchor) else anchor
    classvars, fields = _route(declaring, overrides)
    declared = _relations(declaring, relations or {})
    return build(target, classvars=classvars, fields=fields, relations=declared)


def declare(cls: type[ComponentT], relations: dict[str, Relation]) -> type[ComponentT]:
    """Add a decorator's relations to an already-built class, for a kind's build step.

    The build step of every kind whose decorator takes a class ends here, and
    nothing outside the decorator layer calls it: the class collected its
    relations when it was created, before the decorator had a chance to add
    any, so it collects again.

    Args:
        cls: The class the build step produced.
        relations: The relations the decorator declares, name to relation.

    Returns:
        The same class, its relations merged and their descriptors reinstalled.
    """
    if relations:
        cls.relations = {**cls.relations, **relations}
        cls._collect()
    return cls


# -- Internals -----------------------------------------------------------------


def _route(anchor: type[Component], overrides: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Sort a decorator's plain keyword arguments into ClassVars and field defaults.

    A name is a ClassVar when some class in the anchor's MRO annotates it
    ``ClassVar`` and it is neither private nor reserved; a field when the
    anchor declares it as a Pydantic field other than the identity one. A name
    that is both says two contradictory things (the ClassVar reads on the
    class, the field on every instance), so it is refused rather than routed to
    one of them.

    Args:
        anchor: The class the routing introspects, the anchor of the kind or
            the decorated subclass of it.
        overrides: The decorator's plain keyword arguments, name to value.

    Returns:
        The ClassVars to stamp on the built class and the field defaults to
        override on it, in that order.

    Raises:
        TypeError: If a name is both a ClassVar and a field, or neither.
    """
    classvar_names = _classvar_names(anchor)
    field_names = {name for name in anchor.model_fields if name != _IDENTITY_FIELD}

    classvars: dict[str, Any] = {}
    fields: dict[str, Any] = {}
    for name, value in overrides.items():
        if name in classvar_names and name in field_names:
            raise TypeError(
                f"{anchor.__name__} declares '{name}' both as a ClassVar and a field, "
                f"so the decorator cannot tell which one to set"
            )
        if name in classvar_names:
            classvars[name] = value
        elif name in field_names:
            fields[name] = value
        elif name in anchor.relations:
            raise TypeError(
                f"{anchor.__name__} declares '{name}' as a relation; "
                f'pass it through relations={{"{name}": ...}}'
            )
        else:
            accepted = sorted(classvar_names | field_names)
            raise TypeError(f"{anchor.__name__} does not accept '{name}'; accepted: {accepted}")
    return classvars, fields


def _classvar_names(anchor: type[Component]) -> set[str]:
    """The public, non-reserved ClassVar names anywhere in the anchor's MRO.

    Args:
        anchor: The class whose MRO is walked.

    Returns:
        Every name the MRO annotates ``ClassVar`` that a decorator may write.
    """
    names: set[str] = set()
    for base in anchor.__mro__:
        for name, hint in base.__dict__.get("__annotations__", {}).items():
            if name.startswith("_") or name in _RESERVED:
                continue
            if _is_classvar(hint):
                names.add(name)
    return names


def _is_classvar(hint: Any) -> bool:
    """Whether an annotation, evaluated or still a string, is a ``ClassVar``.

    Both forms occur: the framework's own modules import
    ``annotations`` from ``__future__``, so their annotations are strings,
    while a class built by :meth:`~interloper.serializable.base.Serializable.build_class`
    carries a bare ``ClassVar`` object for each stamped name.

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


def _relations(anchor: type[Component], relations: dict[str, Any]) -> dict[str, Relation]:
    """Read the ``relations=`` channel into the relations it declares.

    Three value forms: a :class:`~interloper.component.relation.Relation`,
    kept as written; a component class, the same shorthand the annotation form
    is (``Relation(cls)``); a non-empty list or tuple of component classes,
    meaning "the relation the anchor already declares under this name, its key
    list narrowed to these classes' keys".

    Args:
        anchor: The class whose declared relations a narrowing list narrows.
        relations: The channel as written, name to value.

    Returns:
        Name to relation, ready for the build step to declare.

    Raises:
        TypeError: If a value is none of the three forms, if a narrowing list
            names a relation the anchor does not declare, or if one of its
            classes is of a kind that relation does not accept.
    """
    declared: dict[str, Relation] = {}
    for name, value in relations.items():
        if isinstance(value, Relation):
            declared[name] = value
            continue
        if _is_component(value):
            declared[name] = Relation(value)
            continue

        classes = list(value) if isinstance(value, (list, tuple)) else []
        if not classes or not all(_is_component(cls) for cls in classes):
            raise TypeError(
                f"Relation '{name}' accepts a Relation, a Component class, or a non-empty list of "
                f"Component classes, got {value!r}"
            )
        relation = anchor.relations.get(name)
        if relation is None:
            raise TypeError(
                f"{anchor.__name__} declares no relation named '{name}' to narrow; "
                f"declared: {sorted(anchor.relations)}"
            )
        for cls in classes:
            if cls.kind not in relation.kinds:
                raise TypeError(
                    f"Relation '{name}' accepts kinds {relation.kinds}, not '{cls.kind}' ({cls.__name__})"
                )
        declared[name] = relation.model_copy(update={"key": [cls.key for cls in classes]})
    return declared


def _is_component(value: Any) -> bool:
    """Whether a value is a component class.

    Args:
        value: The value to test.

    Returns:
        True when the value is a subclass of :class:`~interloper.component.base.Component`.
    """
    return isinstance(value, type) and issubclass(value, Component)
