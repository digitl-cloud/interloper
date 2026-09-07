"""Base component: the fundamental building block of the interloper framework.

Two layers: :class:`Serializable` is anything that is "a class plus its
configuration", serializable through :class:`Spec` and resolvable from its
import path; :class:`Component` extends it into a catalog citizen with kind,
identity and relations. ``KINDS`` maps each kind to its anchor class, the
single per-kind authority every kind-level question reads from.
"""

from __future__ import annotations

import sys
import uuid
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, ClassVar

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr
from typing_extensions import Self

from interloper.component.relation import Bound, ComponentIdentity, Relation
from interloper.errors import ConfigError
from interloper.registry import Registry
from interloper.serializable.base import IgnoredDescriptor, Serializable, Spec
from interloper.utils.imports import get_object_path
from interloper.utils.text import to_label, to_snake_case

if TYPE_CHECKING:
    from interloper.catalog.base import Catalog


# -- Registry ------------------------------------------------------------------
_KINDS_ENTRY_POINT = "interloper.kinds"


def _adopt_kind(name: str, loaded: Any) -> tuple[str, type[Component]]:
    """Resolve a loaded kinds entry to its ``(kind, anchor)`` pair.

    Args:
        name: The entry-point name, used only to name the offending entry in the error.
        loaded: The loaded entry-point object, expected to be a ``Component`` class.

    Returns:
        The pair the registry stores.

    Raises:
        TypeError: If the entry does not point at a ``Component`` class.
    """
    if not (isinstance(loaded, type) and issubclass(loaded, Component)):
        raise TypeError(f"Entry '{name}' in the '{_KINDS_ENTRY_POINT}' group is not a Component class: {loaded!r}")
    anchor = loaded.anchor()
    return anchor.kind, anchor


KINDS: Registry[type[Component]] = Registry(_KINDS_ENTRY_POINT, adopt=_adopt_kind)


# -- Definitions ---------------------------------------------------------------
class ComponentDefinition(BaseModel):
    """Read-only view of a Component class's metadata.

    Returned by ``Component.definition()``. Not a separate architectural
    entity, just a structured projection of the class for API consumers.
    Every kind is self-describing: ``config_schema`` is the JSON Schema of
    its user-configurable fields, ``relations`` the links it declares toward
    other components.
    """

    kind: str
    key: str = ""
    path: str
    name: str
    icon: str = ""
    description: str = ""
    tags: list[str] = Field(default_factory=list)
    config_schema: dict[str, Any] = Field(default_factory=dict)
    state_schema: dict[str, Any] = Field(default_factory=dict)
    relations: dict[str, Relation] = Field(default_factory=dict)


# -- Component -----------------------------------------------------------------
class Component(Serializable):
    """Fundamental building block: identifiable, composable, serializable.

    Every catalog citizen extends ``Component``. On top of
    :class:`Serializable` it provides:

    - **Identity**: ``kind`` (class-level category), ``id`` (instance-level,
      overridable) and ``identity``, the pair relation matching reads.
    - **Relations**: one ``relations`` map, name to
      :class:`~interloper.component.relation.Relation`, collected from the
      class body and bound per instance.
    - **Definition**: ``definition()`` exposes class metadata for API consumers.

    A relation written in a class body is a ``Relation`` value, so ``Relation``
    joins :class:`~interloper.serializable.base.IgnoredDescriptor` in
    ``ignored_types``: Pydantic never mistakes a declared relation for a field.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, ignored_types=(IgnoredDescriptor, Relation))

    kind: ClassVar[str] = ""
    icon: ClassVar[str] = ""
    relations: ClassVar[dict[str, Relation]] = {}
    sensitive: ClassVar[bool] = False
    state_model: ClassVar[type[BaseModel] | None] = None
    _defer_validation: ClassVar[bool] = False

    id: str = Field(default="")

    _bound: dict[str, list[Component]] = PrivateAttr(default_factory=dict)
    _parent: Component | None = PrivateAttr(default=None)

    # -- Construction ----------------------------------------------------------
    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Auto-derive ``kind`` and collect the class's relations.

        ``kind`` is set only for direct children of ``Component``
        (``Source``, ``Asset``, ``Config``, ...).  Further subclasses
        inherit their parent's ``kind`` unless they explicitly declare one.
        (``key`` derivation comes from :class:`Serializable`.)

        Args:
            **kwargs: Class-creation keyword arguments, passed through to ``super()``.
        """
        super().__init_subclass__(**kwargs)
        if "kind" not in cls.__dict__ and any(base is Component for base in cls.__bases__):
            cls.kind = to_snake_case(cls.__name__)
        cls.collect()

    @classmethod
    def collect(cls) -> None:
        """Merge the class's declared relations and install their descriptors.

        Three declaration styles feed one map, in increasing precedence: an
        annotation naming a component class (``connection: PostgresConnection``),
        a :class:`Relation` class attribute (``config = Relation(BigQueryConfig)``),
        and a ``relations`` dict written on the class body, which is what the
        decorators emit. The result merges over every base's map, so a subclass
        entry replaces the inherited entry of the same name and nothing an
        ancestor declared is ever lost.

        Each entry is copied and stamped with its name, then exposed through a
        :class:`~interloper.component.relation.Bound` descriptor: the relation
        at class access, the bound component(s) at instance access. The copy is
        what keeps a subclass's redeclaration off its parent's map.

        Annotated relations are dropped from the class's own annotations before
        Pydantic collects its fields, so a relation is never also a field.

        The :class:`Bound` descriptor this installs is invisible to the type
        checker, which still sees the class-body value it replaced (a
        ``relations`` dict, an annotation, or a bare ``Relation``). A class
        that declares its anchors through a ``relations`` dict therefore
        repeats each one under ``if TYPE_CHECKING:`` as the attribute its
        callers actually see at runtime (``destinations: list[Destination]``),
        with no comment needed at each site now that the reason lives here.
        """
        inherited: dict[str, Relation] = {}
        for base in reversed(cls.__mro__[1:]):
            inherited.update(getattr(base, "relations", None) or {})

        annotations: dict[str, Any] = cls.__dict__.get("__annotations__", {})
        module = sys.modules.get(cls.__module__)
        namespace = vars(module) if module else {}

        own: dict[str, Relation] = {}
        for name, hint in annotations.items():
            annotated = Relation.from_annotation(hint, namespace)
            if annotated is not None:
                own[name] = annotated
        own.update({name: value for name, value in cls.__dict__.items() if isinstance(value, Relation)})
        declared = cls.__dict__.get("relations")
        if isinstance(declared, dict):
            own.update(declared)

        cls.relations = {**inherited, **own}
        for name, relation in cls.relations.items():
            stamped = relation.model_copy(update={"name": name})
            cls.relations[name] = stamped
            setattr(cls, name, Bound(stamped))
            annotations.pop(name, None)

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        """Validate that at most one config field is marked as the discriminator.

        Args:
            **kwargs: Class-creation keyword arguments, passed through to ``super()``.

        Raises:
            TypeError: If several fields carry ``discriminator=True``.
        """
        super().__pydantic_init_subclass__(**kwargs)
        marked = cls._discriminator_fields()
        if len(marked) > 1:
            raise TypeError(f"Component '{cls.__name__}' marks multiple discriminator fields: {sorted(marked)}")

    def __init__(self, /, **data: Any) -> None:
        """Bind the relation kwargs, validate everything else as fields.

        A kwarg named after a declared relation binds that relation instead of
        reaching Pydantic, which knows nothing about relations: a list or tuple
        binds every element, ``None`` binds nothing. Nothing is bound
        implicitly, so the instance is checked once every explicit target is in
        place, unless the class defers that check.

        Unknown kwargs are a loud error rather than pydantic's silent
        ``extra="ignore"`` drop: a misnamed field would otherwise vanish, and a
        stale persisted config key surfaces as drift at load time, consistent
        with the fail-closed drift checks.

        Args:
            **data: Field values and relation targets, keyed by field or
                relation name.

        Raises:
            TypeError: If a kwarg names neither a field nor a declared relation.
        """
        cls = type(self)
        targets = {name: data.pop(name) for name in list(data) if name in cls.relations}
        unknown = [name for name in data if name not in cls.model_fields]
        if unknown:
            raise TypeError(f"{cls.__name__} got unexpected keyword argument(s): {', '.join(sorted(unknown))}")
        super().__init__(**data)
        for name, value in targets.items():
            if value is None:
                continue
            self.bind(name, *(value if isinstance(value, (list, tuple)) else [value]))
        if not cls._defer_validation:
            self.validate_relations()

    def model_post_init(self, context: Any) -> None:
        """Default ``id`` to a generated UUID if not provided.

        Args:
            context: The pydantic validation context, unused here.
        """
        if not self.id:
            self.id = str(uuid.uuid4())

    def __setattr__(self, name: str, value: Any) -> None:
        """Rebind a relation on plain attribute assignment, otherwise defer to Pydantic.

        Pydantic's own ``__setattr__`` only special-cases ``property``, so a
        :class:`~interloper.component.relation.Bound` descriptor never runs on
        assignment; this is what makes ``widget.destinations = [...]`` work.

        The replacement is validated before the existing binding is touched,
        so a rejected reassignment (wrong kind, too many targets for a
        single-valued relation) leaves the previous binding exactly as it was.

        Args:
            name: The attribute being set.
            value: For a relation name, ``None`` or an empty sequence clears
                the binding, a single component or a list/tuple of components
                replaces it; any other name is forwarded to Pydantic as a
                field assignment.
        """
        if name in type(self).relations:
            relation = self._relation(name)
            targets = tuple(value) if isinstance(value, (list, tuple)) else (() if value is None else (value,))
            self._check_targets(name, relation, targets)
            self._bound[name] = list(targets)
            return
        super().__setattr__(name, value)

    # -- Relations -------------------------------------------------------------
    def _check_targets(self, name: str, relation: Relation, targets: tuple[Component, ...]) -> None:
        """Check that *targets* are legal for one of this component's relations.

        Every target must be one the relation :meth:`~Relation.accepts`, and a
        single-valued relation may not receive more than one target at once.
        Performs no mutation, so both :meth:`bind` and relation reassignment
        through ``__setattr__`` can call it before touching ``_bound``, and a
        rejected replacement leaves the existing binding untouched.

        Args:
            name: The relation name as declared on the class.
            relation: The declared relation *targets* are checked against.
            targets: The candidate components to check.

        Raises:
            ConfigError: If a target's kind or key is not one the relation
                accepts, or if a single-valued relation is given more than one target.
        """
        owner = self.identity
        for target in targets:
            if not relation.accepts(target.kind, target.identity, owner=owner):
                raise ConfigError(
                    f"{type(self).__name__}.{name} does not accept {target.kind} '{target.qualified_key}' "
                    f"(declared: kind {relation.kinds()}, key {relation.keys() or 'any'})"
                )
        if not relation.many and len(targets) > 1:
            raise ConfigError(f"{type(self).__name__}.{name} is single-valued; unbind before binding another target")

    def bind(self, name: str, *targets: Component) -> None:
        """Bind components to one of this component's declared relations.

        A ``many`` relation accumulates, skipping targets it already holds; a
        single-valued one accepts exactly one target and refuses to swap it
        silently, so repointing is an explicit :meth:`unbind` first.

        Args:
            name: The relation name as declared on the class.
            *targets: The components to bind. Binding nothing is a no-op, so a
                caller may splat an empty list.

        Raises:
            ConfigError: If a target's kind or key is not one the relation
                accepts, or if a single-valued relation would hold two targets.
        """
        relation = self._relation(name)
        if not targets:
            return
        self._check_targets(name, relation, targets)
        current = self._bound.get(name, [])
        if relation.many:
            accumulated = list(current)
            for target in targets:
                if all(target is not held for held in accumulated):
                    accumulated.append(target)
            self._bound[name] = accumulated
        elif current and current[0] is not targets[0]:
            raise ConfigError(f"{type(self).__name__}.{name} is single-valued; unbind before binding another target")
        else:
            self._bound[name] = list(targets)

    def unbind(self, name: str, *targets: Component) -> None:
        """Detach components from one of this component's declared relations.

        Args:
            name: The relation name as declared on the class.
            *targets: The components to detach; ones the relation does not hold
                are ignored.

        Raises:
            ConfigError: If detaching would leave a non-optional relation with
                nothing bound.
        """
        relation = self._relation(name)
        current = self._bound.get(name, [])
        remaining = [held for held in current if all(held is not target for target in targets)]
        if current and not remaining and not relation.optional:
            raise ConfigError(f"{type(self).__name__}.{name} is non-optional and cannot be emptied")
        self._bound[name] = remaining

    def bound(self, name: str) -> Component | list[Component] | None:
        """What is explicitly bound to one of this component's declared relations.

        Args:
            name: The relation name as declared on the class.

        Returns:
            The bound components as a list for a ``many`` relation (empty when
            nothing is bound), otherwise the single bound component or ``None``.
        """
        relation = self._relation(name)
        values = self._bound.get(name, [])
        if relation.many:
            return list(values)
        return values[0] if values else None

    def bound_ids(self) -> dict[str, list[str]]:
        """The ids this component's bindings point at, for persistence.

        Returns:
            Relation name to bound component ids, in binding order; relations
            with nothing bound are absent.
        """
        return {name: [target.id for target in targets] for name, targets in self._bound.items() if targets}

    def trickle(self, child: Component) -> None:
        """Fill a child's unbound relations from this component's own bindings.

        For every relation the child declares under a name this component has
        itself bound, and that the child leaves unbound, this binds whichever
        of the parent's targets the child's relation :meth:`~Relation.accepts`
        (every accepted target for a ``many`` relation, the first accepted one
        otherwise). A binding the child already holds is never touched.

        Args:
            child: The component to trickle this component's bindings into.
        """
        for name, relation in type(child).relations.items():
            if child._bound.get(name) or name not in self._bound:
                continue
            accepted = [t for t in self._bound[name] if relation.accepts(t.kind, t.identity, owner=child.identity)]
            if accepted:
                child.bind(name, *(accepted if relation.many else accepted[:1]))

    def resolve(self, name: str) -> Any:
        """What one of this component's declared relations actually resolves to.

        Unlike :meth:`bound`, this falls back to what the relation can fill
        itself with when nothing is bound, so a self-filling relation (a config
        whose every field is defaulted, say) works without a binding.

        Args:
            name: The relation name as declared on the class.

        Returns:
            The bound value when there is one, otherwise the relation's
            fallback for a single-valued relation and an empty list for a
            ``many`` one.
        """
        relation = self._relation(name)
        bound = self.bound(name)
        if bound or relation.many:
            return bound
        return relation.fallback()

    def validate_relations(self, nodes: Mapping[str, Component] | None = None) -> None:
        """Check that every relation this component holds is sound.

        Three checks per relation: it is bound, unless it is optional or
        self-filling; a single-valued relation holds at most one target; every
        bound target is one the relation :meth:`~Relation.accepts`. When
        *nodes* is given, a bound non-optional ``asset``-kind target must also
        be one of *nodes*. This is a DAG-wide check the constructor cannot
        make, since the DAG doesn't exist yet at construction time.

        Args:
            nodes: Every node materializing in the same run, keyed by id. When
                ``None``, the DAG-membership check is skipped.

        Raises:
            ConfigError: If any relation is unbound and non-optional, holds
                several targets while single-valued, holds a target it does
                not accept, or (when *nodes* is given) points a non-optional
                relation at an asset absent from *nodes*.
        """
        problems: list[str] = []
        for name, relation in type(self).relations.items():
            targets = self._bound.get(name, [])
            if not targets:
                if not relation.optional and not relation.self_filling:
                    problems.append(f"'{name}' is unbound and non-optional")
                continue
            if not relation.many and len(targets) > 1:
                problems.append(f"'{name}' is single-valued but holds {len(targets)} targets")
            for target in targets:
                if not relation.accepts(target.kind, target.identity, owner=self.identity):
                    problems.append(f"'{name}' holds {target.kind} '{target.qualified_key}', which it does not accept")
                elif (
                    nodes is not None
                    and not relation.optional
                    and target.kind == "asset"
                    and "asset" in relation.kinds()
                    and target.id not in nodes
                ):
                    problems.append(f"'{name}' points at asset '{target.qualified_key}' which is not in the DAG")
        if problems:
            raise ConfigError(f"{type(self).__name__} '{self.qualified_key}': " + "; ".join(problems))

    def _relation(self, name: str) -> Relation:
        """Look up one of this component's declared relations by name.

        Args:
            name: The relation name as declared on the class.

        Returns:
            The declared relation.

        Raises:
            KeyError: If the class declares no relation of that name.
        """
        declared = type(self).relations
        if name not in declared:
            raise KeyError(f"{type(self).__name__} declares no relation '{name}'; declared: {sorted(declared)}")
        return declared[name]

    # -- Instance discrimination -----------------------------------------------

    @classmethod
    def _discriminator_fields(cls) -> list[str]:
        """Names of the config fields marked ``discriminator=True``.

        Returns:
            The marked field names (normally zero or one).
        """
        return [
            name
            for name, field in cls.model_fields.items()
            if isinstance(field.json_schema_extra, dict) and field.json_schema_extra.get("x-discriminator")
        ]

    @classmethod
    def discriminator_field(cls) -> str | None:
        """The config field marked ``discriminator=True``, if any.

        Returns:
            The field name, or ``None`` when the class declares no discriminator.
        """
        marked = cls._discriminator_fields()
        return marked[0] if marked else None

    @property
    def discriminator(self) -> str | None:
        """This instance's discriminator value, if declared and set.

        The value of the config field marked ``discriminator=True``: what
        distinguishes instances of the same component class (an ad account
        id, a site URL, …). Drives the derived :meth:`instance_name` and, for
        sources, the per-instance asset table names.
        """
        field_name = self.discriminator_field()
        if field_name is None:
            return None
        value = getattr(self, field_name)
        return str(value) if value else None

    def instance_name(self) -> str:
        """Display name for this instance: its discriminator value.

        The class label is the fallback when no discriminator is declared or
        set, since the type is already visible alongside the name everywhere
        the name is shown, so it isn't repeated in it.

        A derived *default*, not an identity: the persistence layer uses it to
        seed a blank component name, and users may override it freely. It never
        feeds physical naming.

        Returns:
            E.g. ``"act_123"``, or ``"Facebook Ads"`` without a discriminator.
        """
        return self.discriminator or type(self).name or to_label(type(self).__name__)

    # -- Identity --------------------------------------------------------------
    @property
    def parent(self) -> Component | None:
        """The component that owns this one, ``None`` when it stands alone.

        A source owns its assets; a connection, a config or a standalone asset
        has no owner. Ownership scopes the bare keys a relation declares, so it
        is what :attr:`identity` reads.
        """
        return self._parent

    @parent.setter
    def parent(self, value: Component | None) -> None:
        """Set the component that owns this one.

        Args:
            value: The owning component, or ``None`` to detach it.
        """
        self._parent = value

    @property
    def identity(self) -> ComponentIdentity:
        """What this component is for relation matching: its owner's key and its own."""
        return ComponentIdentity.of(self)

    @property
    def qualified_key(self) -> str:
        """This component's key, qualified by its owner's key when it has an owner."""
        return str(self.identity)

    def __str__(self) -> str:
        """Human-readable representation: ``Name (key: k, id: i)``.

        Returns:
            Formatted string with class name, key, and id.
        """
        return f"{type(self).__name__} (key: {self.key}, id: {self.id})"

    @classmethod
    def anchor(cls) -> type[Component]:
        """The class anchoring this component's kind.

        The anchor is the base-most class in the MRO that declares the
        kind (``Connection`` for any connection subclass), so any component
        class resolves to the single per-kind authority.

        Returns:
            The anchoring class.
        """
        anchor = cls
        for base in cls.__mro__:
            if (
                base is not Component
                and isinstance(base, type)
                and issubclass(base, Component)
                and getattr(base, "kind", "") == cls.kind
            ):
                anchor = base
        return anchor

    # -- Serialization & resolution --------------------------------------------
    def to_spec(self) -> Spec:
        """Serialize this instance to a reconstructible spec, carrying its id.

        Returns:
            A Spec capturing this instance's state and identity.
        """
        return super().to_spec().model_copy(update={"id": self.id})

    @classmethod
    def resolve_key(cls, key: str, catalog: Catalog | None = None) -> type[Self]:
        """Resolve a catalog key to a component class of this (sub)class.

        The key is looked up in *catalog* (or, when none is given, the
        settings-configured catalog, built lazily), and the class it names
        is imported.

        Called on a subclass, the resolved class must be of that subclass
        (``Source.resolve_key("facebook_ads")``); anything else raises
        ``TypeError``.

        Args:
            key: The catalog key naming the component class.
            catalog: The catalog to look the key up in. Defaults to ``None``, which builds the
                settings-configured catalog.

        Returns:
            The resolved class.

        Raises:
            CatalogKeyError: If the key is not in the catalog.
        """
        if catalog is None:
            from interloper.catalog.base import Catalog

            catalog = Catalog.from_settings()
        definition = catalog.get(key)
        if definition is None:
            from interloper.errors import CatalogKeyError

            raise CatalogKeyError(f"Unknown catalog key '{key}'")
        return cls._resolve_import(definition.path, ref=key)

    # -- Definition ------------------------------------------------------------
    @classmethod
    def definition(cls) -> ComponentDefinition:
        """Produce a structured definition of this component class.

        Returns:
            A ComponentDefinition with metadata derived from the class.
        """
        return ComponentDefinition(
            kind=cls.kind,
            key=cls.key,
            path=get_object_path(cls),
            name=cls.name or to_label(cls.__name__),
            icon=cls.icon,
            description=cls.__doc__ or "",
            tags=list(getattr(cls, "tags", [])),
            config_schema=cls.config_schema(),
            state_schema=cls.state_model.model_json_schema() if cls.state_model else {},
            relations=dict(cls.relations),
        )
