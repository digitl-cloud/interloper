"""Base component: the fundamental building block of the interloper framework.

Two layers: :class:`Serializable` is anything that is "a class plus its
configuration" — serializable through :class:`Spec` and resolvable
from its import path; :class:`Component` extends it into a catalog citizen
with kind, identity and relations. ``KINDS`` maps each kind to its anchor
class — the single per-kind authority every kind-level question reads from.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any, ClassVar

from pydantic import BaseModel, Field
from typing_extensions import Self

from interloper.registry import Registry
from interloper.serializable.base import Serializable, Spec
from interloper.utils.imports import get_object_path
from interloper.utils.text import to_label, to_snake_case

if TYPE_CHECKING:
    from interloper.catalog.base import Catalog
    from interloper.resource.base import Resource


# -- Registry ------------------------------------------------------------------
_KINDS_ENTRY_POINT = "interloper.kinds"


def _adopt_kind(name: str, loaded: Any) -> tuple[str, type[Component]]:
    """Resolve a loaded kinds entry to its ``(kind, anchor)`` pair.

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


# -- Relations -----------------------------------------------------------------
class RelationSlot(BaseModel):
    """A declared slot on a slotted relation type.

    ``key`` names the expected component key for the slot (``""`` accepts any
    component of the relation's kinds); ``required`` distinguishes mandatory
    slots from optional ones.
    """

    key: str = ""
    required: bool = True


class RelationDefinition(BaseModel):
    """One relation type a component kind may declare toward other components.

    ``kinds`` lists the component kinds a relation of this type may point at
    (enforced when relations are written); ``keys`` optionally narrows the
    allowed destination keys — picker metadata for UIs, not write-enforced;
    ``slotted`` marks relations that carry a slot, and ``slots`` enumerates
    the slots a concrete class declares — together they fully describe the
    pickers a UI renders for the relation.

    ``field`` names the instance field that carries relations of this type;
    its shape follows the definition: slotted types are ``dict[slot, ...]``,
    unslotted ones ``list[...]``. ``inline`` declares what the field carries:
    embedded component instances (the default), or bare instance ids resolved
    at execution time (``inline=False``, e.g. asset dependencies).
    """

    kinds: list[str]
    field: str
    slotted: bool = False
    inline: bool = True
    keys: list[str] = Field(default_factory=list)
    slots: dict[str, RelationSlot] = Field(default_factory=dict)


# -- Definitions ---------------------------------------------------------------
class ComponentDefinition(BaseModel):
    """Read-only view of a Component class's metadata.

    Returned by ``Component.definition()``. Not a separate architectural
    entity — just a structured projection of the class for API consumers.
    Every kind is self-describing: ``config_schema`` is the JSON Schema of
    its user-configurable fields, ``relations`` the vocabulary of relation
    types it may declare toward other components.
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
    relations: dict[str, RelationDefinition] = Field(default_factory=dict)


# -- Component -----------------------------------------------------------------
class Component(Serializable):
    """Fundamental building block: identifiable, composable, serializable.

    Every catalog citizen extends ``Component``. On top of
    :class:`Serializable` it provides:

    - **Identity** — ``kind`` (class-level category) and ``id``
      (instance-level, overridable).
    - **Relations** — ``relation_types`` vocabulary toward other components
      and ``resources`` slot routing.
    - **Definition** — ``definition()`` exposes class metadata for API consumers.
    """

    kind: ClassVar[str] = ""
    icon: ClassVar[str] = ""
    resource_types: ClassVar[dict[str, type[Resource]]] = {}
    relation_types: ClassVar[dict[str, RelationDefinition]] = {}
    sensitive: ClassVar[bool] = False
    runnable: ClassVar[bool] = False
    state_model: ClassVar[type[BaseModel] | None] = None

    id: str = Field(default="")
    resources: dict[str, Any] = Field(default_factory=dict)

    # -- Construction ----------------------------------------------------------
    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Auto-derive ``kind``, merge the relation vocabulary, infer resources.

        ``kind`` is set only for direct children of ``Component``
        (``Source``, ``Asset``, ``Config``, ...).  Further subclasses
        inherit their parent's ``kind`` unless they explicitly declare one.
        (``key`` derivation comes from :class:`Serializable`.)

        A subclass's ``relation_types`` declaration is **extend-only**: it
        merges over the nearest ancestor's vocabulary (a redeclared type
        replaces that type's definition; nothing is ever removed), so the
        anchor stays the kind's shared minimum while concrete classes add
        the verbs only they act on (``TriggerHook`` adds ``target``).

        Annotations typed as ``Resource`` subclasses are automatically
        converted to ``ResourceRef`` descriptors, registering them in
        ``resource_types`` and providing typed attribute access.
        """
        super().__init_subclass__(**kwargs)
        if "kind" not in cls.__dict__ and any(base is Component for base in cls.__bases__):
            cls.kind = to_snake_case(cls.__name__)
        if own_relations := cls.__dict__.get("relation_types"):
            for base in cls.__mro__[1:]:
                inherited = getattr(base, "relation_types", None)
                if isinstance(inherited, dict):
                    cls.relation_types = {**inherited, **own_relations}
                    break
        cls._infer_resource_refs()

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        """Validate that at most one config field is marked as the discriminator.

        Raises:
            TypeError: If several fields carry ``discriminator=True``.
        """
        super().__pydantic_init_subclass__(**kwargs)
        marked = cls._discriminator_fields()
        if len(marked) > 1:
            raise TypeError(f"Component '{cls.__name__}' marks multiple discriminator fields: {sorted(marked)}")

    @classmethod
    def _infer_resource_refs(cls) -> None:
        """Convert Resource-typed annotations into ``ResourceRef`` descriptors.

        Scans *own* annotations (not inherited) for types that are
        ``Resource`` subclasses.  Each match is replaced with a
        ``ResourceRef`` descriptor, which registers itself in
        ``resource_types`` via ``__set_name__``.

        Annotations already backed by an explicit ``resource_types``
        entry or an existing ``ResourceRef`` descriptor are skipped.
        """
        import sys

        raw_annotations: dict[str, Any] = cls.__dict__.get("__annotations__", {})
        if not raw_annotations:
            return

        # Lazy import to break the circular dependency:
        # Resource extends Component, so this import fails while
        # Resource itself is being defined — bail out in that case.
        try:
            from interloper.resource.base import Resource
            from interloper.resource.ref import ResourceRef
        except ImportError:
            return

        explicit: dict[str, type] = cls.__dict__.get("resource_types", {})
        module = sys.modules.get(cls.__module__)
        module_globals = vars(module) if module else {}

        for attr_name, annotation in list(raw_annotations.items()):
            # Skip if already explicitly declared or already a ResourceRef.
            if attr_name in explicit:
                continue
            if isinstance(cls.__dict__.get(attr_name), ResourceRef):
                continue

            # Resolve string annotations (from __future__ import annotations).
            resolved = annotation
            if isinstance(annotation, str):
                resolved = module_globals.get(annotation, annotation)

            if isinstance(resolved, type) and issubclass(resolved, Resource):
                ref = ResourceRef(resolved)
                ref.__set_name__(cls, attr_name)
                setattr(cls, attr_name, ref)
                # Remove from annotations so Pydantic doesn't see it as a field.
                del raw_annotations[attr_name]

    def __init__(self, /, **data: Any) -> None:
        """Route kwargs named after declared resource slots into ``resources``.

        Resource-typed annotations are converted to ``ResourceRef`` descriptors
        and removed from the pydantic model, so a kwarg like
        ``BigQueryDestination(connection=...)`` would otherwise be silently
        dropped by pydantic. Such kwargs are validated against the slot's
        declared type and stored in ``resources`` instead.

        Unknown kwargs are a loud error rather than pydantic's silent
        ``extra="ignore"`` drop — a misnamed field would otherwise vanish
        (and a stale persisted config key surfaces as drift at load time,
        consistent with the fail-closed drift checks).

        Raises:
            TypeError: If a kwarg matches no field or resource slot, or a
                slot kwarg doesn't satisfy the slot's declared type.
            ValueError: If a slot is given both as a kwarg and in ``resources``.
        """
        unknown = [
            name for name in data if name not in type(self).model_fields and name not in type(self).resource_types
        ]
        if unknown:
            raise TypeError(f"{type(self).__name__} got unexpected keyword argument(s): {', '.join(sorted(unknown))}")
        slot_names = [n for n in data if n in type(self).resource_types and n not in type(self).model_fields]
        if slot_names:
            resources = data.get("resources")
            resources = dict(resources) if isinstance(resources, dict) else {}
            for name in slot_names:
                value = data.pop(name)
                expected = type(self).resource_types[name]
                if not isinstance(value, expected):
                    raise TypeError(
                        f"{type(self).__name__} resource '{name}' must be an instance of "
                        f"{expected.__name__}, got {type(value).__name__}"
                    )
                if name in resources:
                    raise ValueError(
                        f"{type(self).__name__} got resource '{name}' both as a keyword argument and in 'resources'"
                    )
                resources[name] = value
            data["resources"] = resources
        super().__init__(**data)

    def model_post_init(self, context: Any) -> None:
        """Default ``id`` to a generated UUID if not provided."""
        if not self.id:
            self.id = str(uuid.uuid4())

    # -- Instance discrimination -------------------------------------------------

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

        The value of the config field marked ``discriminator=True`` — what
        distinguishes instances of the same component class (an ad account
        id, a site URL, …). Drives the derived :meth:`instance_name` and, for
        sources, the per-instance asset table names.
        """
        field_name = type(self).discriminator_field()
        if field_name is None:
            return None
        value = getattr(self, field_name)
        return str(value) if value else None

    def instance_name(self) -> str:
        """Display name for this instance: its discriminator value.

        The class label is the fallback when no discriminator is declared or
        set — the type is already visible alongside the name everywhere the
        name is shown, so it isn't repeated in it.

        A derived *default*, not an identity: the persistence layer uses it to
        seed a blank component name, and users may override it freely. It never
        feeds physical naming.

        Returns:
            E.g. ``"act_123"``, or ``"Facebook Ads"`` without a discriminator.
        """
        return self.discriminator or type(self).name or to_label(type(self).__name__)

    # -- Resources -------------------------------------------------------------
    def trickle_resources(self, target: Component) -> None:
        """Fill a child component's empty resource slots from this component's resources.

        Resolution order per slot:
        1. By name — if this component has a resource with the same slot name
           that satisfies the slot's declared type.
        2. By type — if any of this component's resources is an instance of the
           required type.

        Only empty slots are filled; pre-existing entries are never overwritten.

        Args:
            target: The child component whose resource slots to fill.
        """
        for name, res_type in type(target).resource_types.items():
            if name in target.resources:
                continue
            # Match by name first; a same-named resource of the wrong type
            # falls through to the type match rather than filling the slot.
            source_res = self.resources.get(name)
            if isinstance(source_res, res_type):
                target.resources[name] = source_res
            else:
                # Fall back to type match.
                for sr in self.resources.values():
                    if isinstance(sr, res_type):
                        target.resources[name] = sr
                        break

    # -- Identity --------------------------------------------------------------
    def __str__(self) -> str:
        """Human-readable representation: ``Name (key: k, id: i)``.

        Returns:
            Formatted string with class name, key, and id.
        """
        return f"{type(self).__name__} (key: {type(self).key}, id: {self.id})"

    @classmethod
    def anchor(cls) -> type[Component]:
        """The class anchoring this component's kind.

        The anchor is the base-most class in the MRO that declares the
        kind (``Connection`` for any connection subclass), so any component
        class resolves to the single per-kind authority. The anchor's
        relation vocabulary is validated on the way out — errors propagate
        from ``_check_relation_fields``.

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
        anchor._check_relation_fields()
        return anchor

    @classmethod
    def _check_relation_fields(cls) -> None:
        """Check the class's relation vocabulary against its fields.

        Raises:
            ValueError: If a declared relation type's ``field`` does not
                exist on the class — a misdeclared field would silently
                drop persisted relations on reconstruction.
        """
        for type_, definition in cls.relation_types.items():
            if definition.field not in cls.model_fields:
                raise ValueError(
                    f"'{cls.__name__}' (kind '{cls.kind}') declares relation type '{type_}' "
                    f"with field '{definition.field}', but has no such field"
                )

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

        The key is looked up in *catalog* — or the settings-configured
        catalog, built lazily, when none is given — and the class it names
        is imported.

        Called on a subclass, the resolved class must be of that subclass
        (``Source.resolve_key("facebook_ads")``) — anything else raises
        ``TypeError``.

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

        Validates the class's (possibly extended) relation vocabulary on the
        way out — the per-class counterpart of the anchor check in
        :meth:`anchor`, run for every declared class at catalog build.
        Validation errors propagate from ``_check_relation_fields``.

        Returns:
            A ComponentDefinition with metadata derived from the class.
        """
        cls._check_relation_fields()
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
            relations=cls.relation_definitions(),
        )

    @classmethod
    def relation_definitions(cls) -> dict[str, RelationDefinition]:
        """The class's relation vocabulary, enriched with its declared slots.

        The class's merged ``relation_types`` gives the vocabulary (the
        anchor's shared minimum plus whatever the concrete class extends it
        with); the class also contributes the slots — resource slots come
        from ``resource_types`` here, subclasses layer in what they declare
        (dependency parameters, allowed destination keys).

        Returns:
            Relation type → enriched definition.
        """
        relations = dict(cls.relation_types)
        if "resource" in relations:
            relations["resource"] = relations["resource"].model_copy(
                update={"slots": {name: RelationSlot(key=res.key) for name, res in cls.resource_types.items()}}
            )
        return relations
