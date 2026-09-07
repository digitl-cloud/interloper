"""The relation primitive: what a component declares about the components it links to."""

from __future__ import annotations

from types import UnionType
from typing import TYPE_CHECKING, Any, Literal, NamedTuple, Union, get_args, get_origin

from pydantic import BaseModel, ConfigDict, Field

from interloper.serializable import IgnoredDescriptor

if TYPE_CHECKING:
    from interloper.component.base import Component

ANY_SOURCE = "*"


class ComponentIdentity(NamedTuple):
    """What a component is, for relation matching: its owning source key and its own key.

    ``source_key`` is ``None`` for a component with no owning source (a
    connection, a config, a standalone asset); assets owned by a source
    carry that source's key.
    """

    source_key: str | None
    key: str

    @classmethod
    def of(cls, component: Component) -> ComponentIdentity:
        """Build the identity of an already-constructed component.

        Args:
            component: The component to identify.

        Returns:
            The component's identity, its parent's key as ``source_key`` when
            it has a parent, ``None`` otherwise.
        """
        parent = component.parent
        return cls(parent.key if parent is not None else None, component.key)

    @classmethod
    def resolve(cls, declared_key: str, *, own_source_key: str | None) -> ComponentIdentity:
        """The identity a declared relation key expects.

        A bare key is scoped to the declaring component's own source, a
        qualified key (``source.key``) names the source explicitly.

        Args:
            declared_key: The relation key as written, bare (``"orders"``)
                or qualified (``"shop.orders"``, ``"*.campaigns"``).
            own_source_key: Key of the source declaring the relation, used to
                scope a bare key. ``None`` when the declaring component has no
                owning source.

        Returns:
            The expected identity; ``source_key`` is ``own_source_key`` for a
            bare key, or the part before the dot for a qualified key.
        """
        source_key, dot, key = declared_key.rpartition(".")
        if not dot:
            return cls(own_source_key, declared_key)
        return cls(source_key, key)

    def satisfies(self, declared_key: str, *, own_source_key: str | None) -> bool:
        """Whether this identity is an acceptable match for a declared key.

        A bare key expects a component of the declaring source, a qualified
        key a component of the named source, and ``*.key`` a component of
        that key owned by some source, whichever it is; an identity with no
        source never satisfies a wildcard.

        Args:
            declared_key: The relation key as written on the declaring component.
            own_source_key: Key of the source declaring the relation, used to
                scope a bare key. ``None`` when the declaring component has no
                owning source.

        Returns:
            True when the key matches and the source constraint holds.
        """
        expected = ComponentIdentity.resolve(declared_key, own_source_key=own_source_key)
        if expected.key != self.key:
            return False
        if expected.source_key == ANY_SOURCE:
            return self.source_key is not None
        return expected.source_key == self.source_key

    def __str__(self) -> str:
        """Format as a key.

        Returns:
            The qualified-key form (``source.key``); bare when there is no source.
        """
        return f"{self.source_key}.{self.key}" if self.source_key else self.key


class Relation(BaseModel):
    """One declared link from an owner component to the components that may fill it.

    ``kind`` and ``key`` name the acceptable targets: ``kind`` names one or
    several component kinds, ``key`` (when non-empty) narrows to specific
    keys within those kinds, matched through :meth:`ComponentIdentity.satisfies`.
    ``many`` marks a relation that binds several components at once;
    ``optional`` marks one that may stay unbound. ``default`` and
    ``self_filling`` together describe a relation that can be resolved
    without an explicit binding.

    ``default`` (intended type ``Callable[[], Component] | None``) and
    ``target`` (intended type ``type[Component] | None``, the class this
    relation was declared from when class-declared) are annotated ``Any``
    to satisfy the type checker, not Pydantic: ``Component`` is only
    imported under ``TYPE_CHECKING``, so the annotation cannot name it
    directly. Both fields are excluded from dumps.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    kind: str | list[str]
    key: str | list[str] = ""
    many: bool = False
    optional: bool = False
    default: Any = Field(default=None, exclude=True)
    on_delete: Literal["block", "detach"] = "block"
    name: str = ""
    target: Any = Field(default=None, exclude=True)

    def __init__(self, kind_or_class: str | list[str] | type = "", key: str | list[str] = "", /, **data: Any) -> None:
        """Build a relation from either a component class or explicit kind/key values.

        Args:
            kind_or_class: A component class (its ``kind`` and ``key`` class
                attributes seed the relation and ``target`` is set to it), or
                the relation's ``kind`` value as a string or list of strings.
            key: The relation's ``key`` value, ignored when ``kind_or_class``
                is a class.
            **data: Remaining field values (``many``, ``optional``,
                ``default``, ``on_delete``, ``name``), forwarded to Pydantic.
        """
        if isinstance(kind_or_class, type):
            data.update(
                kind=kind_or_class.kind,  # ty: ignore[unresolved-attribute]
                key=kind_or_class.key,  # ty: ignore[unresolved-attribute]
                target=kind_or_class,
            )
        else:
            data.setdefault("kind", kind_or_class)
            data.setdefault("key", key)
        super().__init__(**data)

    @classmethod
    def from_annotation(cls, hint: Any, namespace: dict[str, Any]) -> Relation | None:
        """Build the relation an annotation declares, when it declares one.

        Recognised shapes are a component class (``connection: PostgresConnection``),
        a forward reference to one, and the optional forms ``X | None`` and
        ``Optional[X]``. Anything else is not a relation and stays an ordinary
        field: a scalar, a container, and ``Component`` itself, which names no
        kind and so cannot say what would fill the relation.

        Args:
            hint: The annotation as written, already evaluated or still the
                forward-reference string ``from __future__ import annotations``
                leaves behind.
            namespace: The namespace a forward reference resolves against,
                normally the declaring class's module globals.

        Returns:
            A relation targeting the annotated class, ``optional`` when the
            annotation admits ``None``, or ``None`` when the annotation
            declares no relation.
        """
        from interloper.component.base import Component

        target, optional = cls._unwrap_optional(hint, namespace)
        if get_origin(target) is None and isinstance(target, type) and issubclass(target, Component) and target.kind:
            return cls(target, optional=optional)
        return None

    @staticmethod
    def _unwrap_optional(hint: Any, namespace: dict[str, Any]) -> tuple[Any, bool]:
        """Strip an annotation's ``None`` arm and resolve a forward reference.

        Args:
            hint: The annotation as written, evaluated or a string.
            namespace: The namespace a forward reference resolves against.

        Returns:
            The single named type the annotation carries (``None`` when it
            carries several, or a string that does not resolve) and whether
            the annotation admits ``None``.
        """
        if isinstance(hint, str):
            text = hint.strip()
            optional = text.startswith("Optional[") and text.endswith("]")
            if optional:
                text = text[len("Optional[") : -1].strip()
            written = [part.strip() for part in text.split("|")]
            named = [part for part in written if part != "None"]
            if len(named) != 1:
                return None, False
            return namespace.get(named[0]), optional or len(named) < len(written)
        if get_origin(hint) in (Union, UnionType):
            arguments = get_args(hint)
            named_types = [argument for argument in arguments if argument is not type(None)]
            if len(named_types) != 1:
                return None, False
            return named_types[0], len(named_types) < len(arguments)
        return hint, False

    def kinds(self) -> list[str]:
        """Normalise ``kind`` to a list.

        Returns:
            ``kind`` as a list, whether declared as a single string or already a list.
        """
        return [self.kind] if isinstance(self.kind, str) else list(self.kind)

    def keys(self) -> list[str]:
        """Normalise ``key`` to a list.

        Returns:
            ``key`` as a list, empty when unset (any key of the relation's
            kinds is accepted), whether declared as a single string or a list.
        """
        if not self.key:
            return []
        return [self.key] if isinstance(self.key, str) else list(self.key)

    def accepts(self, kind: str, identity: ComponentIdentity, *, owner: ComponentIdentity) -> bool:
        """Whether a candidate component may fill this relation.

        Args:
            kind: The candidate component's kind.
            identity: The candidate component's identity.
            owner: The identity of the component declaring this relation, used
                to scope bare keys.

        Returns:
            True when ``kind`` is one of ``kinds()`` and, when ``keys()`` is
            non-empty, ``identity`` satisfies at least one of them.
        """
        if kind not in self.kinds():
            return False
        keys = self.keys()
        if not keys:
            return True
        return any(identity.satisfies(declared, own_source_key=owner.source_key) for declared in keys)

    @property
    def self_filling(self) -> bool:
        """Whether this relation can be resolved without an explicit binding.

        True when a ``default`` factory is set, or when the relation is
        single-valued, has a ``target`` class, and every field of that class
        (besides ``id``) is optional.

        Returns:
            True when the relation can fill itself.
        """
        if self.default is not None:
            return True
        if self.many or self.target is None:
            return False
        fields = getattr(self.target, "model_fields", None)
        if fields is None:
            return False
        return all(not field.is_required() for name, field in fields.items() if name != "id")

    def fallback(self) -> Any | None:
        """Produce the value this relation resolves to when left unbound.

        Returns:
            A fresh ``default()`` when set, a fresh ``target()`` when the
            relation is self-filling through its target's optional fields,
            or ``None`` when the relation cannot fill itself.
        """
        if self.default is not None:
            return self.default()
        if self.self_filling and self.target is not None:
            return self.target()
        return None


class Bound(IgnoredDescriptor):
    """Descriptor installed for each relation: the ``Relation`` on the class, bound value(s) on an instance."""

    def __init__(self, relation: Relation) -> None:
        """Attach the descriptor to its relation.

        Args:
            relation: The relation this descriptor exposes and binds against.
        """
        self.relation = relation

    def __get__(self, instance: Any, owner: type | None = None) -> Any:
        """Resolve the relation itself on class access, bound value(s) on an instance.

        Args:
            instance: The component the attribute is accessed on, or ``None``
                for class-level access.
            owner: The class the descriptor is defined on; unused.

        Returns:
            The ``Relation`` when accessed on the class; otherwise the
            instance's bound value(s) for this relation's name.
        """
        if instance is None:
            return self.relation
        return instance.bound(self.relation.name)

    def __set__(self, instance: Any, value: Any) -> None:
        """Rebind this relation's slot on an instance.

        Args:
            instance: The component to rebind the relation on.
            value: ``None`` or an empty sequence clears the binding; a single
                component or a list/tuple of components replaces it.
        """
        targets = value if isinstance(value, (list, tuple)) else ([] if value is None else [value])
        instance._bound.pop(self.relation.name, None)
        if targets:
            instance.bind(self.relation.name, *targets)
