"""The relation primitive: what a component declares about the components it links to."""

from __future__ import annotations

from types import UnionType
from typing import TYPE_CHECKING, Any, Literal, NamedTuple, Union, get_args, get_origin, overload

from pydantic import BaseModel, ConfigDict, Field
from pydantic_settings import BaseSettings

if TYPE_CHECKING:
    from interloper.component.base import Component

ANY_SOURCE = "*"


def unwrap_optional(hint: Any, namespace: dict[str, Any]) -> tuple[Any, bool]:
    """Strip an annotation's ``None`` arm and resolve a forward reference.

    The single reading of ``X | None`` in the framework: both
    :meth:`Relation.from_annotation` and the asset layer's ``data()``
    parameter inference go through it.

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

    A relation is also its own descriptor: :meth:`Component.collect` installs
    the stamped copy under the relation's name, so the class attribute reads
    as the declaration and the instance attribute as what is bound to it, and
    assignment rebinds it.

    The declaration form the framework writes pairs the relation with the
    annotation of what fills it
    (``destinations: list[Destination] = Relation("destination", many=True)``).
    The ``TYPE_CHECKING`` ``__new__`` is what makes that form check: the type
    checker reads a Pydantic field of the annotated type, so the constructor
    kwarg and the instance attribute are both typed, while the runtime sees a
    ``Relation`` (Pydantic ignores it, and :meth:`Component.collect` drops the
    annotation before fields are collected).

    ``default`` (intended type ``Callable[[], Component] | None``) and
    ``target`` (intended type ``type[Component] | None``, the class this
    relation was declared from when class-declared) are annotated ``Any``
    to satisfy the type checker, not Pydantic: ``Component`` is only
    imported under ``TYPE_CHECKING``, so the annotation cannot name it
    directly. Both fields are excluded from dumps.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    kind: str | list[str]
    key: str | list[str] = ""
    many: bool = False
    optional: bool = False
    default: Any = Field(default=None, exclude=True)
    on_delete: Literal["block", "detach"] = "block"
    name: str = ""
    target: Any = Field(default=None, exclude=True)

    if TYPE_CHECKING:

        def __new__(cls, *args: Any, **kwargs: Any) -> Any:
            """Type-checking-only stub that widens construction to ``Any``.

            Never defined at runtime, where Pydantic's own ``__new__`` builds
            the relation. It exists so a typed declaration
            (``x: list[Destination] = Relation("destination", many=True)``)
            reads as a field of the annotated type instead of a type error.

            Args:
                *args: Positional arguments, forwarded to :meth:`__init__`.
                **kwargs: Keyword arguments, forwarded to :meth:`__init__`.

            Returns:
                The new relation, typed ``Any`` so it satisfies any annotation.
            """
            ...

    def __init__(self, kind_or_class: str | list[str] | type = "", key: str | list[str] = "", /, **data: Any) -> None:
        """Build a relation from either a component class or explicit kind/key values.

        Args:
            kind_or_class: A component class (its ``kind`` and ``key`` class
                attributes seed the relation and ``target`` is set to it), or
                the relation's ``kind`` value as a string or list of strings.
            key: The relation's ``key`` value, ignored when ``kind_or_class``
                is a class.
            **data: Remaining field values (``many``, ``optional``,
                ``default``, ``on_delete``, ``name``), forwarded to Pydantic,
                which rejects any other name rather than dropping it.
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

        target, optional = unwrap_optional(hint, namespace)
        if get_origin(target) is None and isinstance(target, type) and issubclass(target, Component) and target.kind:
            return cls(target, optional=optional)
        return None

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

        A bare key is scoped to the owner's source only when the *candidate*
        is an asset: an asset is the one kind whose key is source-local.
        Every other candidate kind is keyed globally by its catalog key, so a
        bare key there names that class wherever it comes from. Scoping by
        the relation's declared kinds instead of the candidate's own would
        wrongly scope a source or destination candidate to the owner's source
        the moment the relation also accepts assets (a job's ``targets``,
        say).

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
        own_source_key = owner.source_key if kind == "asset" else None
        return any(identity.satisfies(declared, own_source_key=own_source_key) for declared in keys)

    @property
    def source_local(self) -> bool:
        """Whether every key this relation declares names a component of the owner's own source.

        A bare key (``"orders"``) is scoped to the declaring component's own
        source, so that source is the authority on what fills it; a qualified
        or wildcard key (``"shop.orders"``, ``"*.orders"``) reaches outside it,
        and only the graph of a run can say what fills it.

        Returns:
            True when the relation declares at least one key and none of them
            is qualified; False for a relation declaring no key at all, which
            accepts any key of its kinds wherever it comes from.
        """
        keys = self.keys()
        return bool(keys) and all("." not in key for key in keys)

    @property
    def self_filling(self) -> bool:
        """Whether this relation can be resolved without an explicit binding.

        Three cases, all of them single-valued but for the first: a ``default``
        factory is set; the ``target`` is a settings class, whose required
        fields come from the environment, so a resource the user never bound
        is still constructible where it is read; or every field of the
        ``target`` (besides ``id``) is optional, so the class constructs bare.

        Returns:
            True when the relation can fill itself.
        """
        if self.default is not None:
            return True
        if self.many or self.target is None:
            return False
        if isinstance(self.target, type) and issubclass(self.target, BaseSettings):
            return True
        fields = getattr(self.target, "model_fields", None)
        if fields is None:
            return False
        return all(not field.is_required() for name, field in fields.items() if name != "id")

    def fallback(self) -> Any | None:
        """Produce the value this relation resolves to when left unbound.

        A settings target is constructed here and not at build time, which is
        what keeps a connection whose credentials live in the environment
        usable: the validation error of a credential that is neither bound nor
        in the environment propagates from the read that needed it.

        Returns:
            A fresh ``default()`` when set, a fresh ``target()`` when the
            relation is self-filling, or ``None`` when the relation cannot
            fill itself.
        """
        if self.default is not None:
            return self.default()
        if self.self_filling and self.target is not None:
            return self.target()
        return None

    # -- Descriptor ------------------------------------------------------------

    @overload
    def __get__(self, instance: None, owner: type | None = None) -> Relation: ...
    @overload
    def __get__(self, instance: Component, owner: type | None = None) -> Any: ...
    def __get__(self, instance: Component | None, owner: type | None = None) -> Any:
        """Resolve to the relation itself on class access, to bound value(s) on an instance.

        A relation is its own descriptor: :meth:`Component.collect` installs
        the stamped copy under the relation's name, so ``Widget.connection`` is
        the declaration and ``widget.connection`` what is bound to it.

        Args:
            instance: The component the attribute is accessed on, or ``None``
                for class-level access.
            owner: The class the descriptor is installed on; unused, the
                relation already carries its name.

        Returns:
            This relation on class access; otherwise the instance's bound
            value(s) for this relation's name (see
            :meth:`Component.bound`).
        """
        if instance is None:
            return self
        return instance.bound(self.name)

    def __set__(self, instance: Component, value: Any) -> None:
        """Replace what is bound to this relation on *instance*, atomically.

        Assignment is one of the two ways a binding is written, and it goes
        through the same write path as :meth:`Component.bind`: the same rules
        apply (a rejected assignment leaves the previous binding exactly as it
        was, duplicates collapse, a non-optional relation cannot be emptied)
        and whatever the owner cascades into its children is re-cascaded.

        Args:
            instance: The component the assignment was made on.
            value: ``None`` or an empty sequence clears the binding, a single
                component binds it, a list or tuple binds every element.
        """
        targets = tuple(value) if isinstance(value, (list, tuple)) else (() if value is None else (value,))
        instance._replace_binding(self.name, targets)
