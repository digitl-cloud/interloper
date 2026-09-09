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
from typing import TYPE_CHECKING, Any, ClassVar, ForwardRef

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr
from typing_extensions import Self

from interloper.component.relation import ComponentIdentity, Relation, unwrap_optional
from interloper.errors import ConfigError
from interloper.registry import Registry
from interloper.serializable.base import IgnoredDescriptor, Serializable, SerializationContext, Spec
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
        cls._collect()

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        """Check the discriminator fields and the annotations Pydantic kept as fields.

        Runs once Pydantic has built the fields, which is the only moment the
        two collectors can be compared: :meth:`_collect` reads the annotations
        against the declaring module, Pydantic reads them against the full
        defining scope. An annotation naming a component class that
        :meth:`_collect` did not turn into a relation therefore became a plain
        field, silently: no trickle, no ``{ref}``, and two classes differing
        only in declaration order behaving differently. Both that and an
        annotation Pydantic could not resolve at all are refused here.

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

    def __init__(self, /, **data: Any) -> None:
        """Bind the relation kwargs, validate everything else as fields.

        A kwarg named after a declared relation binds that relation instead of
        reaching Pydantic, which knows nothing about relations: a list or tuple
        binds every element, ``None`` binds nothing. Each binding is checked
        as it is made (see :meth:`bind`); whether every relation that must be
        bound is bound is a question for whoever assembles the graph, since
        components are wired piecewise (a source trickles into its assets,
        reconstruction binds references once the document is whole), so
        :meth:`validate_relations` is called at those boundaries, not here.

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
        which runs :meth:`Relation.__set__` and rebinds. The rebinding logic
        lives there and only there.

        Args:
            name: The attribute being set.
            value: For a relation name, whatever :meth:`Relation.__set__`
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
    def owned(self) -> list[Component]:
        """The components this one owns, which travel inside its own spec.

        Ownership is not declared: a component held in one of this
        component's fields whose :attr:`parent` is this component is owned.
        A source's assets are the one case today.

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
        id, a site URL, ...). Drives the derived :meth:`instance_name` and,
        for sources, the per-instance asset table names.
        """
        marked = self._discriminator_fields()
        if not marked:
            return None
        value = getattr(self, marked[0])
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
        :meth:`bind` and needs no :meth:`unbind` first.

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
        goes through the same write path as :meth:`bind`, so the same checks
        apply (a non-optional relation cannot be emptied, ``ConfigError``) and
        :meth:`on_rebind` runs.

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

    def on_rebind(self, name: str) -> None:
        """React to one of this component's relations having been rebound.

        Called once after every write to a binding, whichever way it was
        written: a constructor kwarg, :meth:`bind`, :meth:`unbind`, attribute
        assignment, or a parent's :meth:`trickle`. The new binding is already
        in place, so :meth:`bound` reads it. The base does nothing; override
        it to cascade, the way a source trickles its bindings into its assets
        and destinations and a job into its targets.

        Binding on ``self`` from inside the hook re-enters it; bind on other
        components only.

        Args:
            name: The relation name whose binding changed.
        """

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

        Unlike :meth:`bound`, this falls back to what the relation can fill
        itself with when nothing is bound, so a relation with a fallback (a config
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

    def validate_relations(self, nodes: Mapping[str, Component] | None = None) -> None:
        """Check that every relation this component holds is sound.

        Three checks per relation: it is bound, unless it is optional or
        has a fallback; a single-valued relation holds at most one target; every
        bound target is one the relation :meth:`~Relation.accepts`. When
        *nodes* is given, a bound non-optional ``asset``-kind target must also
        be one of *nodes*.

        The check runs at the boundaries where a graph is whole: the DAG runs
        it with its nodes once resolution is done, and reconstruction runs it
        without *nodes* on every root of a document once its references are
        bound. Without *nodes* an unbound ``asset``-kind relation only the
        graph can fill is left alone: one reaching outside the owner's own
        source (:attr:`~Relation.local`), or any one on an owner that
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
            targets = self._bound.get(name, [])
            if not targets:
                graph_filled = (
                    nodes is None
                    and "asset" in relation.kinds
                    and (not relation.local or self.parent is None)
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

    # -- Serialization & resolution --------------------------------------------

    def to_spec(self, *, context: SerializationContext | None = None) -> Spec:
        """Serialize this instance to a reconstructible spec, relations and owned components included.

        What a relation holds sits in ``init`` under the relation's name, a
        list for a ``many`` relation and a single value otherwise, each target
        written as :meth:`SerializationContext.emit` decides; a relation with
        nothing bound is left out. The components this one owns (see
        :attr:`children`) travel inside this spec, under the field that holds
        them, as a map of key to init carrying the child's id: an owned
        component never has a spec of its own. A binding a child holds that
        is identical to this component's own is left out of the child's
        payload, since the same trickle refills it on reconstruction.

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
                for name in self._trickled(child):
                    child_init.pop(name, None)
                child_init["id"] = child.id
                payload[child.key] = child_init
            if payload:
                init[field] = payload
        return self._build_spec(init=init or None).model_copy(update={"id": self.id})

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
        - a :class:`Relation` value, which is where anything else the relation
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
        name: a :class:`~interloper.component.relation.Relation` is its own
        descriptor, so the class attribute reads as the declaration and the
        instance attribute as what is bound to it. The copy is what keeps a
        subclass's redeclaration off its parent's map.

        Annotated relations are dropped from the class's own annotations before
        Pydantic collects its fields, so a relation is never also a field.
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
            setattr(cls, name, stamped)
            annotations.pop(name, None)

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

        Every target must be one the relation :meth:`~Relation.accepts`, and a
        single-valued relation may not receive more than one target at once.
        Performs no mutation, so :meth:`_replace_binding` can call it before
        touching ``_bound`` and a rejected replacement leaves the existing
        binding untouched.

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
                    f"(declared: kind {relation.kinds}, key {relation.keys or 'any'})"
                )
        if not relation.many and len(targets) > 1:
            raise ConfigError(f"{type(self).__name__}.{name} is single-valued and takes one target at a time")

    def _replace_binding(self, name: str, targets: tuple[Component, ...]) -> None:
        """Replace what is bound to one of this component's relations, atomically.

        The one write path: :meth:`bind` and :meth:`Relation.__set__` both
        land here, so the rules hold whichever way a binding is written.
        Everything is checked before ``_bound`` is touched, so a rejected
        write leaves the previous binding exactly as it was, and
        :meth:`on_rebind` runs once the new binding is in place.

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
        self.on_rebind(name)

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
        """The owned components, keyed by the field that holds them.

        Returns:
            Field name to the owned components it holds; fields holding none
            are absent.
        """
        owned: dict[str, list[Component]] = {}
        for name in type(self).model_fields:
            value = getattr(self, name)
            items = value if isinstance(value, list) else [value]
            children = [item for item in items if isinstance(item, Component) and item.parent is self]
            if children:
                owned[name] = children
        return owned

    def _trickled(self, child: Component) -> set[str]:
        """The relation names a child holds exactly as this component holds them.

        A child receives a relation's binding only through :meth:`trickle`,
        which passes this component's own targets through unchanged; the
        target ids are what tell that binding apart from one the child bound
        on its own. Ids and not object identity, because a deep copy rebuilds
        an owner's own bindings and its children's separately.

        Args:
            child: An owned component.

        Returns:
            The names whose binding on the child is exactly this component's
            own list of targets, in order.
        """
        return {
            name
            for name, targets in self._bound.items()
            if targets and [t.id for t in child._bound.get(name, [])] == [t.id for t in targets]
        }
