"""Base component: the fundamental building block of the interloper framework.

Two layers: `Serializable` is anything that is "a class plus its
configuration", serializable through `Spec` and resolvable from its
import path; `Component` extends it into a catalog citizen with kind,
identity and relations. ``KINDS`` maps each kind to its anchor class, the
single per-kind authority every kind-level question reads from.
"""

from __future__ import annotations

import sys
import uuid
from collections.abc import Mapping
from copy import deepcopy
from typing import TYPE_CHECKING, Any, ClassVar, ForwardRef

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr
from typing_extensions import Self

from interloper.component.inherited import InheritedSetting
from interloper.component.maturity import Maturity
from interloper.component.relation import ComponentIdentity, Relation, unwrap_optional
from interloper.errors import ConfigError
from interloper.registry import Registry
from interloper.serializable.base import IgnoredDescriptor, Serializable, SerializationContext, Spec
from interloper.utils.text import to_label, to_snake_case

if TYPE_CHECKING:
    from interloper.catalog.base import Catalog
    from interloper.partitioning.base import PartitionWindow


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
    other components, ``maturity`` how far it is from being relied on.
    """

    kind: str
    key: str = ""
    path: str
    name: str
    icon: str = ""
    description: str = ""
    tags: list[str] = Field(default_factory=list)
    maturity: Maturity = Maturity.STABLE
    config_schema: dict[str, Any] = Field(default_factory=dict)
    state_schema: dict[str, Any] = Field(default_factory=dict)
    relations: dict[str, Relation] = Field(default_factory=dict)


# -- Component -----------------------------------------------------------------
class Component(Serializable):
    """Fundamental building block: identifiable, composable, serializable.

    Every catalog citizen extends ``Component``. On top of
    `Serializable` it provides:

    - **Identity**: ``kind`` (class-level category), ``id`` (instance-level,
      overridable) and ``identity``, the pair relation matching reads.
    - **Relations**: one ``relations`` map, name to
      `Relation`, collected from the
      class body and bound per instance.
    - **Definition**: ``definition()`` exposes class metadata for API consumers.

    A relation written in a class body is a ``Relation`` value, so ``Relation``
    joins `IgnoredDescriptor` in
    ``ignored_types``: Pydantic never mistakes a declared relation for a field.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, ignored_types=(IgnoredDescriptor, Relation))

    kind: ClassVar[str] = ""
    icon: ClassVar[str] = ""
    maturity: ClassVar[Maturity] = Maturity.STABLE
    relations: ClassVar[dict[str, Relation]] = {}
    sensitive: ClassVar[bool] = False
    state_model: ClassVar[type[BaseModel] | None] = None

    id: str = Field(default="")

    _bound: dict[str, list[Component]] = PrivateAttr(default_factory=dict)
    _parent: Component | None = PrivateAttr(default=None)

    # -- Construction ----------------------------------------------------------

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Auto-derive ``kind``, collect the class's relations and validate its settings.

        ``kind`` is set only for direct children of ``Component``
        (``Source``, ``Asset``, ``Config``, ...).  Further subclasses
        inherit their parent's ``kind`` unless they explicitly declare one.
        (``key`` derivation and the check of the settings the class body
        declares come from `Serializable`.)

        Args:
            **kwargs: Class-creation keyword arguments, passed through to ``super()``.
        """
        super().__init_subclass__(**kwargs)
        if "kind" not in cls.__dict__ and any(base is Component for base in cls.__bases__):
            cls.kind = to_snake_case(cls.__name__)
        cls._collect()

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        """Check the discriminator fields and the annotations Pydantic kept as fields.

        Runs once Pydantic has built the fields, which is the only moment the
        two collectors can be compared: `_collect` reads the annotations
        against the declaring module, Pydantic reads them against the full
        defining scope. An annotation naming a component class that
        `_collect` did not turn into a relation therefore became a plain
        field, silently: no binding, no ``{ref}``, and two classes differing
        only in declaration order behaving differently. Both that and an
        annotation Pydantic could not resolve at all are refused here. Every
        field marked ``x-inherit`` then gets its read-through descriptor (see
        `InheritedSetting`).

        Args:
            **kwargs: Class-creation keyword arguments, passed through to ``super()``.

        Raises:
            TypeError: If several fields carry ``discriminator=True``, if a
                field's annotation names a component class that was not
                collected as a relation, or if a field's annotation could not
                be resolved.
        """
        super().__pydantic_init_subclass__(**kwargs)
        marked = cls._discriminator_fields()
        if len(marked) > 1:
            raise TypeError(f"Component '{cls.__name__}' marks multiple discriminator fields: {sorted(marked)}")
        module = sys.modules.get(cls.__module__)
        namespace = vars(module) if module else {}
        for name, field in cls.model_fields.items():
            annotation, _ = unwrap_optional(field.annotation, namespace)
            if isinstance(annotation, ForwardRef):
                raise TypeError(
                    f"{cls.__name__}.{name} is annotated with '{annotation.__forward_arg__}', which could not be "
                    f"resolved; define the class before it is referenced or declare the relation explicitly"
                )
            if Relation.from_annotation(annotation, namespace) is not None:
                raise TypeError(
                    f"{cls.__name__}.{name} is annotated with a Component class but was not collected as a "
                    f"relation; define the class before it is referenced or declare the relation explicitly"
                )
        InheritedSetting.install(cls)

    def __init__(self, /, **data: Any) -> None:
        """Bind the relation kwargs, validate everything else as fields.

        A kwarg named after a declared relation binds that relation instead of
        reaching Pydantic, which knows nothing about relations: a list or tuple
        binds every element, ``None`` binds nothing. Each binding is checked
        as it is made (see `bind`); whether every relation that must be
        bound is bound is a question for whoever assembles the graph, since
        components are wired piecewise (a source's assets read its bindings
        through it, reconstruction binds references once the document is whole), so
        `validate_relations` is called at those boundaries, not here.

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

    def model_post_init(self, context: Any) -> None:
        """Default ``id`` to a generated UUID if not provided.

        Args:
            context: The pydantic validation context, unused here.
        """
        if not self.id:
            self.id = str(uuid.uuid4())

    def __setattr__(self, name: str, value: Any) -> None:
        """Route a relation assignment to its descriptor, otherwise defer to Pydantic.

        Pydantic's own ``__setattr__`` bypasses descriptors other than
        ``property``, so a relation name is handed to ``object.__setattr__``,
        which runs `Relation.__set__` and rebinds. The rebinding logic
        lives there and only there.

        Args:
            name: The attribute being set.
            value: For a relation name, whatever `Relation.__set__`
                accepts; any other name is forwarded to Pydantic as a field
                assignment.
        """
        if name in type(self).relations:
            object.__setattr__(self, name, value)
            return
        super().__setattr__(name, value)

    # -- Identity --------------------------------------------------------------

    @property
    def parent(self) -> Component | None:
        """The component that owns this one, ``None`` when it stands alone.

        A source owns its assets; a connection, a config or a standalone asset
        has no owner. Ownership scopes the bare keys a relation declares, so it
        is what `identity` reads.
        """
        return self._parent

    @parent.setter
    def parent(self, value: Component | None) -> None:
        """Set the component that owns this one.

        Args:
            value: The owning component, or ``None`` to detach it.
        """
        self._parent = value

    def inherited(self, child: Component, name: str, own: Any) -> Any:
        """What an owned component reads for one of its inherited settings.

        The one seam a cascade goes through: an owned component never holds a
        copy of what it inherits, it asks its owner on every read. The base
        hands down this component's own effective value; an owner that names
        part of its children's location overrides it (see `Source.inherited`).

        Args:
            child: The owned component reading the setting.
            name: The setting's field name.
            own: The child's stored value, ``None`` while unset.

        Returns:
            *own* when set, else this component's own value under *name* when
            it declares such a field, else ``None``.
        """
        if own is not None:
            return own
        if name in type(self).model_fields:
            return getattr(self, name)
        return None

    @property
    def owned(self) -> list[Component]:
        """The components this one owns (a source's assets), which travel inside its own spec.

        Returns:
            The owned components, in field order; empty for a component that
            owns none.
        """
        return [child for children in self._owned_by_field().values() for child in children]

    @property
    def identity(self) -> ComponentIdentity:
        """What this component is for relation matching: its owner's key and its own."""
        return ComponentIdentity.of(self)

    @property
    def qualified_key(self) -> str:
        """This component's key, qualified by its owner's key when it has an owner."""
        return str(self.identity)

    @property
    def discriminator(self) -> str | None:
        """This instance's discriminator value, if declared and set.

        The value of the config field marked ``discriminator=True``: what
        distinguishes instances of the same component class (an ad account
        id, a site URL, ...). Drives the derived `instance_name` and,
        for sources, the per-instance asset table names.
        """
        field = self.discriminator_field()
        if field is None:
            return None
        value = getattr(self, field)
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

    def __str__(self) -> str:
        """Human-readable representation: ``Name (key: k, id: i)``.

        Returns:
            Formatted string with class name, key, and id.
        """
        return f"{type(self).__name__} (key: {self.key}, id: {self.id})"

    def event_metadata(
        self,
        metadata: Mapping[str, Any],
        window: PartitionWindow | None = None,
        **extra: Any,
    ) -> dict[str, Any]:
        """Build the metadata of an event about this component.

        One shape for every event: the run metadata, the component's identity, the window the
        event is scoped to (``None`` when unscoped), the parent's id when the component has one,
        and whatever the producer adds (a message, an error, a destination key, an attempt).

        Args:
            metadata: Run-level metadata (run id, backfill id, anything the caller passed).
            window: The window the event is scoped to, or ``None``.
            **extra: Producer-specific keys, written last so they win.

        Returns:
            The event metadata.
        """
        built: dict[str, Any] = {
            **metadata,
            "component_id": self.id,
            "component_kind": self.kind,
            "component_key": self.key,
            "qualified_key": self.qualified_key,
            "window": str(window) if window is not None else None,
        }
        if self._parent is not None:
            built["parent_id"] = self._parent.id
        built.update(extra)
        return built

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

    # -- Relations -------------------------------------------------------------

    def bind(self, name: str, *targets: Component) -> None:
        """Bind components to one of this component's declared relations.

        A ``many`` relation accumulates, skipping targets it already holds; a
        single-valued one replaces what it holds, so repointing it is a second
        `bind` and needs no `unbind` first.

        A target whose kind or key the relation does not accept, this
        component itself, and more than one target at once on a single-valued
        relation are all refused with a ``ConfigError`` from
        `_replace_binding`, which every write path goes through.

        Args:
            name: The relation name as declared on the class.
            *targets: The components to bind. Binding nothing is a no-op, so a
                caller may splat an empty list.
        """
        if not targets:
            return
        relation = self._relation(name)
        held = tuple(self._bound.get(name, [])) if relation.many else ()
        self._replace_binding(name, held + targets)

    def unbind(self, name: str, *targets: Component) -> None:
        """Detach components from one of this component's declared relations.

        Detaching nothing the relation holds is a no-op; otherwise what remains
        goes through the same write path as `bind`, so the same checks
        apply (a non-optional relation cannot be emptied, ``ConfigError``).

        Args:
            name: The relation name as declared on the class.
            *targets: The components to detach; ones the relation does not hold
                are ignored.
        """
        self._relation(name)
        current = self._bound.get(name, [])
        remaining = [held for held in current if all(held is not target for target in targets)]
        if len(remaining) == len(current):
            return
        self._replace_binding(name, tuple(remaining))

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

    def resolve(self, name: str) -> Any:
        """What one of this component's declared relations actually resolves to.

        Unlike `bound`, this reads through the owner (see `_targets`) and then
        falls back to what the relation can fill itself with, so a relation
        with a fallback (a config whose every field is defaulted, say) works
        without a binding.

        Args:
            name: The relation name as declared on the class.

        Returns:
            What the relation holds or reads through its owner, otherwise the
            relation's fallback for a single-valued relation and an empty list
            for a ``many`` one.
        """
        relation = self._relation(name)
        targets = self._targets(name)
        if relation.many:
            return targets
        return targets[0] if targets else relation.fallback()

    def _targets(self, name: str) -> list[Component]:
        """What one of this component's relations holds: its own binding, else what its owner hands it.

        An owned component never holds a copy of its owner's bindings; it
        reads them here. The owner's targets under the same name are filtered
        by what this relation `accepts`, every accepted one for a ``many``
        relation and the first otherwise.

        Args:
            name: The relation name as declared on the class.

        Returns:
            The own targets when any are bound; otherwise the owner's accepted
            targets; empty when neither holds any.
        """
        own = self._bound.get(name, [])
        owner = self._parent
        if own or owner is None or name not in type(owner).relations:
            return list(own)
        relation = self._relation(name)
        accepted = [
            target
            for target in owner._targets(name)
            if relation.accepts(target.kind, target.identity, owner=self.identity)
        ]
        return accepted if relation.many else accepted[:1]

    def validate_relations(self, nodes: Mapping[str, Component] | None = None) -> None:
        """Check that every relation this component holds is sound.

        Three checks per relation: it is bound, unless it is optional or
        has a fallback; a single-valued relation holds at most one target; every
        bound target is one the relation `accepts`. When
        *nodes* is given, a bound non-optional ``asset``-kind target must also
        be one of *nodes*.

        The check runs at the boundaries where a graph is whole: the DAG runs
        it with its nodes once resolution is done, and reconstruction runs it
        without *nodes* on every root of a document once its references are
        bound. Without *nodes* an unbound ``asset``-kind relation only the
        graph can fill is left alone: one reaching outside the owner's own
        source (`local`), or any one on an owner that
        has no source to fill it from. Nothing before the graph can fill such
        a relation, so nothing before the graph can call it unfilled either.

        Args:
            nodes: Every node materializing in the same run, keyed by id. When
                ``None``, the DAG-membership check is skipped and a relation
                only the graph can fill is left alone.

        Raises:
            ConfigError: If any relation is unbound and non-optional, holds
                several targets while single-valued, holds a target it does
                not accept, or (when *nodes* is given) points a non-optional
                relation at an asset absent from *nodes*.
        """
        problems: list[str] = []
        for name, relation in type(self).relations.items():
            targets = self._targets(name)
            if not targets:
                graph_filled = (
                    nodes is None and "asset" in relation.kinds and (not relation.local or self.parent is None)
                )
                if not relation.optional and not relation.has_fallback and not graph_filled:
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
                    and "asset" in relation.kinds
                    and target.id not in nodes
                ):
                    problems.append(f"'{name}' points at asset '{target.qualified_key}' which is not in the DAG")
        if problems:
            raise ConfigError(f"{type(self).__name__} '{self.qualified_key}': " + "; ".join(problems))

    # -- Reconfiguration -------------------------------------------------------

    def __call__(self, **changes: Any) -> Self:
        """Return a reconfigured copy: naming something changes it, leaving it out keeps it.

        A field name sets the field, validated as an assignment would be; on
        an inherited field ``None`` means "inherit again". A relation name
        rebinds through the write path: one component, a list, or ``None`` to
        clear. The copy keeps its own bindings (a new dict, the same targets)
        and its owner; the components it owns are copied and owned by the
        copy, still bound to each other, while every outside target they bind
        is shared, not duplicated.

        Args:
            **changes: Field and relation names to their new values.

        Returns:
            The reconfigured copy.

        Raises:
            TypeError: If a name is neither a field nor a declared relation.
        """
        cls = type(self)
        unknown = sorted(name for name in changes if name not in cls.model_fields and name not in cls.relations)
        if unknown:
            raise TypeError(f"{cls.__name__} has no field or relation named: {', '.join(unknown)}")
        copy = self.model_copy()
        copy._bound = {name: list(targets) for name, targets in self._bound.items()}
        owned = self._owned_by_field()
        if owned:
            children = [child for kids in owned.values() for child in kids]
            memo: dict[int, Any] = {id(self): copy}
            for component in (self, *children):
                for targets in component._bound.values():
                    for target in targets:
                        if all(target is not child for child in children):
                            memo[id(target)] = target
            for field in owned:
                copy.__dict__[field] = deepcopy(self.__dict__[field], memo)
        for name, value in changes.items():
            if name in cls.model_fields:
                cls.__pydantic_validator__.validate_assignment(copy, name, value)
        for name, value in changes.items():
            if name in cls.relations:
                setattr(copy, name, value)
        return copy

    # -- Serialization & resolution --------------------------------------------

    def to_spec(self, *, context: SerializationContext | None = None) -> Spec:
        """Serialize this instance to a reconstructible spec, relations and owned components included.

        What a relation holds sits in ``init`` under the relation's name, a
        list for a ``many`` relation and a single value otherwise, each target
        written as `SerializationContext.emit` decides; a relation with
        nothing bound is left out. The components this one owns (see
        `children`) travel inside this spec, under the field that holds
        them, as a map of key to init carrying the child's id: an owned
        component never has a spec of its own.

        Args:
            context: The state shared with the other roots of one document.
                ``None`` starts a context for this component alone.

        Returns:
            A Spec capturing this instance's state, identity and bindings.
        """
        context = SerializationContext() if context is None else context
        context.add(self)
        owned = self._owned_by_field()
        init = self._fields_init(without=owned)
        relations = type(self).relations
        for name, targets in self._bound.items():
            values = [value for value in (context.emit(target) for target in targets) if value is not None]
            if values:
                init[name] = values if relations[name].many else values[0]
        for field, children in owned.items():
            payload: dict[str, Any] = {}
            for child in context.carried(children):
                child_init = dict(child.to_spec(context=context).init or {})
                child_init["id"] = child.id
                payload[child.key] = child_init
            if payload:
                init[field] = payload
        return self._build_spec(init=init or None).model_copy(update={"id": self.id})

    @classmethod
    def from_init(cls, init: dict[str, Any], context: SerializationContext) -> Self:
        """Build a component from a loaded init, holding its references back for the document to bind.

        Args:
            init: Constructor keyword arguments, nested specs already built and
                every ``{"ref": id}`` still a mapping.
            context: The document being read: it keeps the held references and
                the instance, so a later reference finds it.

        Returns:
            The component, its references not yet bound.
        """
        instance = cls(**context.hold(init))
        context.add(instance)
        return instance

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

        The one place a class is described. A kind whose definition carries
        more (a source's assets, an asset's schema) builds its own model from
        this one: ``SourceDefinition(**dict(super().definition()), assets=...)``.
        Every ``FetchField`` provider the class declares is checked against its
        relations first, so a misnamed provider fails at catalog-build time
        rather than in a form.

        Returns:
            A ComponentDefinition with metadata derived from the class.
        """
        from interloper.resource.fields import validate_fetch_field_providers

        validate_fetch_field_providers(cls, cls.relations)
        return ComponentDefinition(
            kind=cls.kind,
            key=cls.key,
            path=cls.classpath(),
            name=cls.name or to_label(cls.__name__),
            icon=cls.icon,
            description=cls.__doc__ or "",
            tags=list(getattr(cls, "tags", [])),
            maturity=cls.maturity,
            config_schema=cls.config_schema(),
            state_schema=cls.state_model.model_json_schema() if cls.state_model else {},
            relations=dict(cls.relations),
        )

    # -- Settings --------------------------------------------------------------

    @classmethod
    def _sort_settings(cls, settings: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        """Sort a decorator's settings, the ``relations=`` channel included.

        Relations are written into the namespace's ``relations`` dict, which
        `_collect` merges when the class is created, so they win over the
        relations its annotations declare and are collected once.

        Args:
            settings: The decorator's settings, name to value; ``relations``
                maps relation names to a `Relation`, a component class, or a
                list of component classes narrowing a relation this class declares.

        Returns:
            The namespace entries, and the annotations to add for them.

        Raises:
            TypeError: If a setting names the identity field or a relation.
        """
        settings = dict(settings)
        relations = settings.pop("relations", None) or {}
        for name in settings:
            if name == "id":
                raise TypeError(f"{cls.__name__} does not accept 'id': an id is given to each instance")
            if name in cls.relations:
                raise TypeError(
                    f"{cls.__name__} declares '{name}' as a relation; pass it through relations={{\"{name}\": ...}}"
                )
        entries, annotations = super()._sort_settings(settings)
        if relations:
            entries["relations"] = cls._declared_relations(relations)
        return entries, annotations

    @classmethod
    def _subclass_declaring(cls, name: str) -> type[Serializable] | None:
        """The closest class of this kind declaring *name*, searched from the kind's anchor.

        A setting another class of the same kind declares is usually on a sibling,
        not a subclass: a `Connection` subclass refused ``oauth`` is pointed at
        `OAuthConnection`, which extends `Connection` beside it.

        Args:
            name: The setting looked for.

        Returns:
            The class, or ``None`` when no class of this kind declares it.
        """
        anchor = cls.anchor()
        if anchor is cls:
            return super()._subclass_declaring(name)
        return anchor._subclass_declaring(name)

    @classmethod
    def _declared_relations(cls, relations: dict[str, Any]) -> dict[str, Relation]:
        """Read the ``relations=`` channel into the relations it declares.

        Three value forms: a `Relation`, kept as written; a component class,
        the same shorthand the annotation form is (``Relation(cls)``); a
        non-empty list or tuple of component classes, meaning "the relation
        this class already declares under this name, its key list narrowed to
        these classes' keys".

        Args:
            relations: The channel as written, name to value.

        Returns:
            Name to relation, ready for the namespace.

        Raises:
            TypeError: If a value is none of the three forms, if a narrowing list
                names a relation this class does not declare, or if one of its
                classes is of a kind that relation does not accept.
        """
        declared: dict[str, Relation] = {}
        for name, value in relations.items():
            if isinstance(value, Relation):
                declared[name] = value
                continue
            if isinstance(value, type) and issubclass(value, Component):
                declared[name] = Relation(value)
                continue

            classes = list(value) if isinstance(value, (list, tuple)) else []
            if not classes or not all(isinstance(target, type) and issubclass(target, Component) for target in classes):
                raise TypeError(
                    f"Relation '{name}' accepts a Relation, a Component class, or a non-empty list of "
                    f"Component classes, got {value!r}"
                )
            relation = cls.relations.get(name)
            if relation is None:
                raise TypeError(
                    f"{cls.__name__} declares no relation named '{name}' to narrow; declared: {sorted(cls.relations)}"
                )
            for target in classes:
                if target.kind not in relation.kinds:
                    raise TypeError(
                        f"Relation '{name}' accepts kinds {relation.kinds}, not '{target.kind}' ({target.__name__})"
                    )
            declared[name] = relation.model_copy(update={"key": [target.key for target in classes]})
        return declared

    # -- Internals -------------------------------------------------------------

    @classmethod
    def _collect(cls) -> None:
        """Merge the class's declared relations and install their descriptors.

        The class-creation step, run from ``__init_subclass__`` and again by a
        decorator that adds relations to a built class; never called by hand.
        Three declaration forms feed one map, in increasing precedence:

        - an annotation naming a component class
          (``connection: PostgresConnection``), the shorthand for a relation
          that needs nothing said beyond what fills it;
        - a `Relation` value, which is where anything else the relation
          declares is written. Annotate it with what fills it
          (``destinations: list[Destination] = Relation("destination", many=True)``)
          so both the constructor kwarg and the attribute are typed; the
          annotation is then for the type checker only, since the relation
          itself says what it accepts. A bare ``config = Relation(BigQueryConfig)``
          declares the same relation untyped;
        - a ``relations`` dict written on the class body, which is what the
          decorators emit.

        The result merges over every base's map, so a subclass entry replaces
        the inherited entry of the same name and nothing an ancestor declared
        is ever lost.

        Each entry is copied, stamped with its name and installed under that
        name: a `Relation` is its own
        descriptor, so the class attribute reads as the declaration and the
        instance attribute as what is bound to it. The copy is what keeps a
        subclass's redeclaration off its parent's map.

        Annotated relations are dropped from the class's own annotations before
        Pydantic collects its fields, so a relation is never also a field.
        """
        inherited: dict[str, Relation] = {}
        for base in reversed(cls.__mro__[1:]):
            inherited.update(getattr(base, "relations", None) or {})

        # The live dict, not inspect.get_annotations()'s copy: relation names are
        # popped from it below so pydantic never sees them as fields.
        annotations: dict[str, Any] = cls.__dict__.get("__annotations__", {})  # noqa: RUF063
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
            setattr(cls, name, stamped)
            annotations.pop(name, None)

    @classmethod
    def discriminator_field(cls) -> str | None:
        """Name of the config field marked ``discriminator=True``, if any.

        Returns:
            The marked field name, or ``None`` when the class declares none.
        """
        marked = cls._discriminator_fields()
        return marked[0] if marked else None

    @classmethod
    def public_fields(cls) -> frozenset[str]:
        """Names of the config fields marked ``x-public``.

        A public field may be shown wherever the component is, even when the
        rest of a sensitive kind's config may not.

        Returns:
            The marked field names.
        """
        return frozenset(
            name
            for name, field in cls.model_fields.items()
            if isinstance(field.json_schema_extra, dict) and field.json_schema_extra.get("x-public")
        )

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

    def _check_targets(self, name: str, relation: Relation, targets: tuple[Component, ...]) -> None:
        """Check that *targets* are legal for one of this component's relations.

        Every target must be one the relation `accepts` and
        must be another component: a relation whose declared keys match the
        declaring component's own key (a wildcard, or an asset named after
        the key it reads) would otherwise let it fill itself. A single-valued
        relation may not receive more than one target at once. Performs no
        mutation, so `_replace_binding` can call it before touching
        ``_bound`` and a rejected replacement leaves the existing binding
        untouched.

        Args:
            name: The relation name as declared on the class.
            relation: The declared relation *targets* are checked against.
            targets: The candidate components to check.

        Raises:
            ConfigError: If a target is this component itself, if a target's
                kind or key is not one the relation accepts, or if a
                single-valued relation is given more than one target.
        """
        owner = self.identity
        for target in targets:
            if target is self:
                raise ConfigError(f"{type(self).__name__}.{name} cannot point at the component itself")
            if not relation.accepts(target.kind, target.identity, owner=owner):
                raise ConfigError(
                    f"{type(self).__name__}.{name} does not accept {target.kind} '{target.qualified_key}' "
                    f"(declared: kind {relation.kinds}, key {relation.keys or 'any'})"
                )
        if not relation.many and len(targets) > 1:
            raise ConfigError(f"{type(self).__name__}.{name} is single-valued and takes one target at a time")

    def _replace_binding(self, name: str, targets: tuple[Component, ...]) -> None:
        """Replace what is bound to one of this component's relations, atomically.

        The one write path: `bind` and `Relation.__set__` both
        land here, so the rules hold whichever way a binding is written.
        Everything is checked before ``_bound`` is touched, so a rejected
        write leaves the previous binding exactly as it was.

        Args:
            name: The relation name as declared on the class.
            targets: The components to hold, replacing whatever is held now;
                duplicates collapse to the first occurrence.

        Raises:
            ConfigError: If a target's kind or key is not one the relation
                accepts, if a single-valued relation is given more than one
                target, or if a non-optional relation would be left empty.
        """
        relation = self._relation(name)
        self._check_targets(name, relation, targets)
        if not targets and not relation.optional:
            raise ConfigError(f"{type(self).__name__}.{name} is non-optional and cannot be emptied")
        deduplicated: list[Component] = []
        for target in targets:
            if all(target is not held for held in deduplicated):
                deduplicated.append(target)
        self._bound[name] = deduplicated

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

    def _owned_by_field(self) -> dict[str, list[Component]]:
        """The components this one owns, keyed by the field that holds them; none for a component.

        Ownership is the part-of relationship, and a source's assets are its one
        case: `Source` overrides this. An owned component travels inside its
        owner's spec and is copied with it.

        Returns:
            Field name to the owned components it holds; empty.
        """
        return {}
