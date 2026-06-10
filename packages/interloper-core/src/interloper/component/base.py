"""Base component: the fundamental building block of the interloper framework."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any, ClassVar, cast

from pydantic import BaseModel, ConfigDict, Field
from typing_extensions import Self

from interloper.utils.imports import get_object_path, import_from_path
from interloper.utils.text import to_label, to_snake_case

if TYPE_CHECKING:
    from interloper.resource.base import Resource


class ComponentDescriptor:
    """Marker base class for descriptors that Pydantic should ignore on Components.

    Any descriptor extending this class is automatically excluded from Pydantic
    model field processing via ``Component.model_config["ignored_types"]``.
    """


def dump_spec_value(value: Any) -> Any:
    """Serialize a component field value for a :class:`ComponentSpec` init payload.

    The wire format is uniform: **anything with class identity is a
    Component** and serializes via its own spec; lists and dicts are walked;
    everything else must be a JSON-able scalar.

    Returns:
        A JSON-able value understood by ``ComponentSpec.reconstruct``.
    """
    from pydantic_core import to_jsonable_python

    if isinstance(value, Component):
        return value.to_spec().model_dump(mode="json")
    if isinstance(value, (list, tuple)):
        return [dump_spec_value(v) for v in value]
    if isinstance(value, dict):
        return {k: dump_spec_value(v) for k, v in value.items()}
    return to_jsonable_python(value)


class ComponentDefinition(BaseModel):
    """Read-only view of a Component class's metadata.

    Returned by ``Component.definition()``. Not a separate architectural
    entity — just a structured projection of the class for API consumers.
    """

    kind: str
    key: str = ""
    path: str
    name: str
    icon: str = ""
    description: str = ""


class ComponentSpec(BaseModel):
    """Serialized representation of a Component instance."""

    path: str
    id: str = ""
    init: dict[str, Any] | None = None

    def reconstruct(self) -> Component:
        """Import the component and rebuild the instance, walking nested specs.

        Returns:
            The reconstructed Component.
        """

        def load(v: Any) -> Any:
            if isinstance(v, dict):
                if "path" in v and v.keys() <= {"path", "id", "init"}:
                    return ComponentSpec(**v).reconstruct()
                return {k: load(x) for k, x in v.items()}
            if isinstance(v, list):
                return [load(x) for x in v]
            return v

        cls = import_from_path(self.path)
        kwargs: dict[str, Any] = {"id": self.id} if self.id else {}
        for k, v in (self.init or {}).items():
            kwargs[k] = load(v)
        return cls(**kwargs)


class Component(BaseModel):
    """Fundamental building block: identifiable, composable, serializable.

    Every entity in the framework extends ``Component``. It provides:

    - **Identity** — ``kind`` (class-level category), ``key`` (class-level
      snake_case name), and ``id`` (instance-level, overridable).
    - **Path** — fully qualified import path for dynamic reconstruction.
    - **Serialization** — ``to_spec()`` / ``from_spec()`` round-trip.
    - **Definition** — ``definition()`` exposes class metadata for API consumers.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, ignored_types=(ComponentDescriptor,))

    kind: ClassVar[str] = ""
    key: ClassVar[str] = ""
    name: ClassVar[str] = ""
    icon: ClassVar[str] = ""
    resource_types: ClassVar[dict[str, type[Resource]]] = {}

    id: str = Field(default="")
    resources: dict[str, Any] = Field(default_factory=dict)

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Auto-derive ``kind`` and ``key`` for subclasses.

        ``kind`` is set only for direct children of ``Component``
        (``Source``, ``Asset``, ``Config``, ...).  Further subclasses
        inherit their parent's ``kind`` unless they explicitly declare one.

        ``key`` is set for all subclasses as the snake_cased class name
        unless explicitly declared.

        Annotations typed as ``Resource`` subclasses are automatically
        converted to ``ResourceRef`` descriptors, registering them in
        ``resource_types`` and providing typed attribute access.
        """
        super().__init_subclass__(**kwargs)
        if "kind" not in cls.__dict__ and any(base is Component for base in cls.__bases__):
            cls.kind = to_snake_case(cls.__name__)
        if "key" not in cls.__dict__:
            cls.key = to_snake_case(cls.__name__)
        cls._infer_resource_refs()

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

    def model_post_init(self, context: Any) -> None:
        """Default ``id`` to a generated UUID if not provided."""
        if not self.id:
            self.id = uuid.uuid4().hex[:8]

    def trickle_resources(self, target: Component) -> None:
        """Fill a child component's empty resource slots from this component's resources.

        Resolution order per slot:
        1. By name — if this component has a resource with the same slot name.
        2. By type — if any of this component's resources is an instance of the
           required type.

        Only empty slots are filled; pre-existing entries are never overwritten.

        Args:
            target: The child component whose resource slots to fill.
        """
        for name, res_type in type(target).resource_types.items():
            if name in target.resources:
                continue
            # Match by name first.
            source_res = self.resources.get(name)
            if source_res is not None:
                target.resources[name] = source_res
            else:
                # Fall back to type match.
                for sr in self.resources.values():
                    if isinstance(sr, res_type):
                        target.resources[name] = sr
                        break

    @classmethod
    def has_own_field(cls, field: str) -> bool:
        """Check if this class declares a non-None default for a field.

        Returns:
            True if the class defines a non-None default for the field.
        """
        info = cls.model_fields.get(field)
        return info is not None and info.default is not None

    def __str__(self) -> str:
        """Human-readable representation: ``Name (key: k, id: i)``.

        Returns:
            Formatted string with class name, key, and id.
        """
        return f"{type(self).__name__} (key: {type(self).key}, id: {self.id})"

    def path(self) -> str:
        """Fully qualified import path for this component.

        Returns:
            Dotted path like ``"module.submodule.ClassName"``.
        """
        return get_object_path(type(self))

    def to_spec(self) -> ComponentSpec:
        """Serialize this instance to a reconstructible spec.

        Returns:
            A ComponentSpec capturing this instance's state.
        """
        init: dict[str, Any] = {}
        for name in type(self).model_fields:
            if name == "id":
                continue
            value = getattr(self, name)
            if value is None:
                continue
            init[name] = dump_spec_value(value)

        return ComponentSpec(path=self.path(), id=self.id, init=init or None)

    @classmethod
    def from_spec(cls, spec: ComponentSpec | dict[str, Any]) -> Self:
        """Reconstruct a component from a spec.

        Returns:
            The reconstructed component instance.
        """
        if isinstance(spec, dict):
            spec = ComponentSpec(**spec)
        return cast(Self, spec.reconstruct())

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
        )
