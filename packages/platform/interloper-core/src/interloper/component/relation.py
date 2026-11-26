"""The relation primitive: what a component declares about the components it links to."""

from __future__ import annotations

from collections.abc import Collection, Hashable, Iterable, Mapping
from types import UnionType
from typing import TYPE_CHECKING, Any, Final, Literal, NamedTuple, TypeVar, Union, get_args, get_origin, overload

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr
from pydantic_settings import BaseSettings

if TYPE_CHECKING:
    from interloper.component.base import Component

ANY_SOURCE = "*"
UPSTREAM: Final = "upstream"
K = TypeVar("K", bound=Hashable)


def unwrap_optional(hint: Any, namespace: dict[str, Any]) -> tuple[Any, bool]:
    """Strip an annotation's ``None`` arm and resolve a forward reference.

    The single reading of ``X | None`` in the framework: both
    `Relation.from_annotation` and the asset layer's ``data()``
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
    keys within those kinds, matched through `ComponentIdentity.satisfies`.
    ``many`` marks a relation that binds several components at once;
    ``optional`` marks one that may stay unbound or be left empty, and says
    nothing else: ``on_delete`` alone decides what deleting a bound target
    does, ``block`` refusing the deletion while this relation holds the
    target and ``detach`` letting it through and dropping the binding.
    ``default`` and ``has_fallback`` together describe a relation that can be
    resolved without an explicit binding.

    A relation is also its own descriptor: `Component._collect` installs
    the stamped copy under the relation's name, so the class attribute reads
    as the declaration and the instance attribute as what the relation
    resolves to, and assignment rebinds it.

    A relation is declared by an annotation naming the component class that
    fills it (``connection: PostgresConnection``), or by a ``Relation`` value
    paired with the annotation of what fills it: an empty ``Relation()``
    takes its kind, key and target from that annotation and adds the flags it
    is given, one naming its target or kind needs none
    (``destinations: list[Destination] = Relation("destination", many=True)``).
    The ``TYPE_CHECKING`` ``__new__`` is what makes a value check: the type
    checker reads a Pydantic field of the annotated type with a default, so
    the constructor kwarg is optional and both it and the instance attribute
    are typed, while the runtime sees a
    ``Relation`` (Pydantic ignores it, and `Component._collect` drops the
    annotation before fields are collected).

    Two attributes are not declared but derived. ``name`` is stamped from the
    attribute the relation is declared under, and ``target`` is the class the
    ``Relation(cls)`` shorthand was written with, the class a fallback is
    built from; neither is a constructor argument. ``default`` (intended type
    ``Callable[[], Component] | None``) and ``target`` (intended type
    ``type[Component] | None``) are typed ``Any`` for the type checker, since
    ``Component`` is only imported under ``TYPE_CHECKING``; both stay out of
    dumps.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    kind: str | list[str]
    key: str | list[str] = ""
    many: bool = False
    optional: bool = False
    default: Any = Field(default=None, exclude=True)
    on_delete: Literal["block", "detach"] = "block"
    name: str = ""

    _target: Any = PrivateAttr(default=None)

    if TYPE_CHECKING:

        def __new__(cls, *args: Any, **kwargs: Any) -> Any:
            """Type-checking-only stub that widens construction to ``Any``.

            Never defined at runtime, where Pydantic's own ``__new__`` builds
            the relation. It exists so a typed declaration
            (``x: list[Destination] = Relation("destination", many=True)``)
            reads as a field of the annotated type instead of a type error.

            Args:
                *args: Positional arguments, forwarded to `__init__`.
                **kwargs: Keyword arguments, forwarded to `__init__`.

            Returns:
                The new relation, typed ``Any`` so it satisfies any annotation.
            """
            ...

    def __init__(
        self, kind_or_class: str | list[str] | type[Component] = "", key: str | list[str] = "", /, **data: Any
    ) -> None:
        """Build a relation from either a component class or explicit kind/key values.

        Args:
            kind_or_class: A component class (its ``kind`` and ``key`` class
                attributes seed the relation and it becomes the ``target``),
                or the relation's ``kind`` value as a string or list of strings.
            key: The relation's ``key`` value, ignored when ``kind_or_class``
                is a class.
            **data: Remaining field values (``many``, ``optional``,
                ``default``, ``on_delete``), forwarded to Pydantic, which
                rejects any other name rather than dropping it.

        Raises:
            TypeError: If ``name`` is passed; it is stamped from the attribute
                the relation is declared under.
        """
        if "name" in data:
            raise TypeError("Relation.name is stamped from the attribute the relation is declared under")
        if isinstance(kind_or_class, type):
            data.update(kind=kind_or_class.kind, key=kind_or_class.key)
        else:
            data.setdefault("kind", kind_or_class)
            data.setdefault("key", key)
        super().__init__(**data)
        self._target = kind_or_class if isinstance(kind_or_class, type) else None

    @property
    def target(self) -> Any:
        """The component class this relation was declared from, if it was declared from one.

        Returns:
            The class given to ``Relation(cls)``, else ``None``.
        """
        return self._target

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

    def with_target_of(self, annotated: Relation) -> Relation:
        """Complete an empty ``Relation()`` with the target its annotation declares.

        Args:
            annotated: The relation the annotation declares (see `from_annotation`).

        Returns:
            The annotation's relation, with every flag this one was given
            (``optional``, ``on_delete``, ``default``, ...) in place of its own.
        """
        given = self.model_fields_set - {"kind", "key"}
        return annotated.model_copy(update={name: getattr(self, name) for name in given})

    @property
    def kinds(self) -> list[str]:
        """Normalise ``kind`` to a list.

        Returns:
            ``kind`` as a list, whether declared as a single string or already a list.
        """
        return [self.kind] if isinstance(self.kind, str) else list(self.kind)

    @property
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
        if kind not in self.kinds:
            return False
        keys = self.keys
        if not keys:
            return True
        own_source_key = owner.source_key if kind == "asset" else None
        return any(identity.satisfies(declared, own_source_key=own_source_key) for declared in keys)

    @property
    def local(self) -> bool:
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
        keys = self.keys
        return bool(keys) and all("." not in key for key in keys)

    def matches(
        self, owner: ComponentIdentity, candidates: Mapping[K, ComponentIdentity], *, siblings: Collection[K]
    ) -> list[K]:
        """The candidate assets this relation accepts for an owner asset, the one matching rule.

        What fills an unbound upstream relation is decided here and by its
        identity half, `fills`: the DAG passes its other nodes here, the
        platform asks `fills` per pair of identities and keeps siblings in SQL.
        A candidate matches when it `fills` the relation; a `local` relation
        also keeps only the owner's siblings, since
        a bare key names an asset of the owner's own source instance. A
        relation declaring no key matches nothing: it says nothing about
        which assets fill it. How many of the matches a relation may hold is
        the caller's to judge.

        Args:
            owner: The identity of the asset declaring this relation.
            candidates: The pool, each candidate's handle (an id) mapped to its
                identity. The owner itself is left out by the caller.
            siblings: The handles of the candidates owned by the owner's own
                source instance.

        Returns:
            The handles of the matching candidates, in the pool's order.
        """
        if not self.keys:
            return []
        local = self.local
        return [
            handle
            for handle, identity in candidates.items()
            if self.fills(owner, identity) and (not local or handle in siblings)
        ]

    def fills(self, owner: ComponentIdentity, identity: ComponentIdentity) -> bool:
        """Whether an asset of this identity fills this relation for an owner asset, siblings aside.

        The half of `matches` that identities alone decide, so a caller that
        knows its pool only later (the platform, which expands identities to
        an organisation's rows in SQL) asks it once per pair of identities and
        applies the other half itself: a `local` relation keeps only the
        owner's siblings.

        Args:
            owner: The identity of the asset declaring this relation.
            identity: The candidate asset's identity.

        Returns:
            True when the relation declares a key and `accepts` takes the
            identity as an asset.
        """
        return bool(self.keys) and self.accepts("asset", identity, owner=owner)

    def other_sources(self, own_source_key: str | None) -> frozenset[str]:
        """The sources outside the owner's own that this relation's keys name.

        Args:
            own_source_key: Key of the source declaring the relation, which a
                bare key is scoped to. ``None`` when it is not known yet (an
                asset class before its source collects it) or there is none.

        Returns:
            The source keys its qualified keys name other than
            ``own_source_key``, `ANY_SOURCE` for a wildcard key; empty for a
            relation whose keys are all local, or that declares none.
        """
        sources = {ComponentIdentity.resolve(key, own_source_key=own_source_key).source_key for key in self.keys}
        return frozenset(source for source in sources if source is not None and source != own_source_key)

    @classmethod
    def upstream_source_relation(cls, relations: Iterable[Relation], own_source_key: str | None) -> Relation | None:
        """The `UPSTREAM` relation a component carries for relations reading other sources' assets.

        It binds the upstream sources those relations read: an asset holding
        rows of it reads only those sources' assets, else its source's
        upstream sources, else every asset its relations match. The framework
        gives it, so no author declares it, and it accepts the sources the
        relations name, or any source when one of them is a wildcard.

        Args:
            relations: The relations of an asset, or of every asset of a source.
            own_source_key: Key of the source owning them, which bare keys are
                scoped to; ``None`` when not known yet or there is none.

        Returns:
            The relation, named `UPSTREAM`, of kind ``source``, many,
            optional and blocking the deletion of a bound source; ``None``
            when no relation accepting assets names another source.
        """
        sources = frozenset().union(
            *(relation.other_sources(own_source_key) for relation in relations if "asset" in relation.kinds)
        )
        if not sources:
            return None
        keys = "" if ANY_SOURCE in sources else sorted(sources)
        return cls("source", keys, many=True, optional=True, on_delete="block").model_copy(update={"name": UPSTREAM})

    @property
    def has_fallback(self) -> bool:
        """Whether this relation can be resolved without an explicit binding.

        Three cases, all of them single-valued but for the first: a ``default``
        factory is set; the ``target`` is a settings class, whose required
        fields come from the environment, so a resource the user never bound
        is still constructible where it is read; or every field of the
        ``target`` (besides ``id``) is optional, so the class constructs bare.

        Returns:
            True when `fallback` would produce a value.
        """
        if self.default is not None:
            return True
        if self.many or self.target is None:
            return False
        if issubclass(self.target, BaseSettings):
            return True
        return all(not field.is_required() for name, field in self.target.model_fields.items() if name != "id")

    def fallback(self) -> Any | None:
        """Produce the value this relation resolves to when left unbound.

        A settings target is constructed here and not at build time, which is
        what keeps a connection whose credentials live in the environment
        usable: the validation error of a credential that is neither bound nor
        in the environment propagates from the read that needed it.

        Returns:
            A fresh ``default()`` when set, a fresh ``target()`` when the
            target constructs on its own, or ``None`` when the relation has
            no fallback.
        """
        if self.default is not None:
            return self.default()
        if self.has_fallback and self.target is not None:
            return self.target()
        return None

    def targets_from(self, value: Any) -> tuple[Component, ...]:
        """Read what a constructor kwarg or an assignment hands this relation.

        Args:
            value: ``None`` or an empty sequence for nothing, a list or tuple
                of components, or a single component for a single-valued
                relation.

        Returns:
            The components, in the order given.

        Raises:
            TypeError: If a single component is handed to a ``many`` relation,
                which takes a list.
        """
        if value is None:
            return ()
        if isinstance(value, (list, tuple)):
            return tuple(value)
        if self.many:
            raise TypeError(f"Relation '{self.name}' is many-valued: pass a list, [{type(value).__name__}(...)]")
        return (value,)

    # -- Descriptor ------------------------------------------------------------

    @overload
    def __get__(self, instance: None, owner: type | None = None) -> Relation: ...
    @overload
    def __get__(self, instance: Component, owner: type | None = None) -> Any: ...
    def __get__(self, instance: Component | None, owner: type | None = None) -> Any:
        """Resolve to the relation itself on class access, to bound value(s) on an instance.

        A relation is its own descriptor: `Component._collect` installs
        the stamped copy under the relation's name, so ``Widget.connection`` is
        the declaration and ``widget.connection`` what it resolves to.

        Args:
            instance: The component the attribute is accessed on, or ``None``
                for class-level access.
            owner: The class the descriptor is installed on; unused, the
                relation already carries its name.

        Returns:
            This relation on class access; otherwise what the relation
            resolves to on the instance (see `Component.resolve`): a list for a
            ``many`` relation, and for a single-valued one what is bound or
            read through the owner, else the fallback, else ``None``.
        """
        if instance is None:
            return self
        return instance.resolve(self.name)

    def __set__(self, instance: Component, value: Any) -> None:
        """Replace what is bound to this relation on *instance*, atomically.

        Assignment is one of the two ways a binding is written, and it goes
        through the same write path as `Component.bind`: the same rules
        apply (a rejected assignment leaves the previous binding exactly as it
        was, duplicates collapse, a non-optional relation cannot be emptied)
        and whatever the owner cascades into its children is re-cascaded.

        Args:
            instance: The component the assignment was made on.
            value: ``None`` or an empty sequence clears the binding, a list or
                tuple binds every element, and a single component binds a
                single-valued relation (see `targets_from`).
        """
        instance._replace_binding(self.name, self.targets_from(value))
