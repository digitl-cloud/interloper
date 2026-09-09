"""Serializable: class-plus-configuration objects and their wire format.

Anything whose instances are "a class plus its configuration" extends
:class:`Serializable`; :class:`Spec` is its serialized form, an envelope
of ``path`` (or catalog ``key``), optional ``id``, and the ``init``
payload. ``to_spec()`` / ``from_spec()`` round-trip between the two.

A ``Spec`` document is also a graph, not just a tree: a component appears
in full once and as a ``{"ref": id}`` reference everywhere else, so
reconstruction runs in two passes, building every inline component before
binding what the references name (see :meth:`Spec.reconstruct`).
"""

from __future__ import annotations

import copy
import os
import re
import uuid
from collections.abc import Callable, Collection, Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, cast

from pydantic import BaseModel, ConfigDict, model_validator
from pydantic.fields import FieldInfo
from typing_extensions import Self

from interloper.utils.imports import get_object_path, import_from_path
from interloper.utils.text import to_snake_case

if TYPE_CHECKING:
    from interloper.catalog.base import Catalog
    from interloper.component.base import Component


class IgnoredDescriptor:
    """Marker base class for descriptors that Pydantic should ignore on Components.

    Any descriptor extending this class is automatically excluded from Pydantic
    model field processing via ``Component.model_config["ignored_types"]``.
    """


# -- Specs ---------------------------------------------------------------------
_ENV_VAR_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


class Spec(BaseModel):
    """Serialized representation of a Component instance.

    A component is referenced by exactly one of two names, each keeping its
    own meaning: ``path`` is a fully qualified import path (what
    ``to_spec()`` emits), ``key`` is a catalog key, so hand-authored specs
    may reference components the way the catalog names them.
    """

    path: str = ""
    key: str = ""
    id: str = ""
    init: dict[str, Any] | None = None

    REFERENCE_KEY: ClassVar[str] = "ref"

    @model_validator(mode="after")
    def _check_reference(self) -> Spec:
        """Enforce that exactly one component reference is set.

        Returns:
            The validated spec, unchanged.

        Raises:
            ValueError: If neither or both of ``path`` and ``key`` are set.
        """
        if bool(self.path) == bool(self.key):
            raise ValueError("exactly one of 'path' or 'key' must be set")
        return self

    @classmethod
    def from_file(cls, path: str | Path) -> Spec:
        """Load the single spec a YAML file holds, interpolating ``${VAR}`` placeholders.

        Delegates to :meth:`all_from_file`, which documents the
        interpolation and the ``SpecError`` cases a malformed file raises.

        Args:
            path: Filesystem path to the YAML spec document, as a string or
                ``Path``.

        Returns:
            The validated spec.

        Raises:
            SpecError: If the file holds anything other than exactly one
                document.
        """
        from interloper.errors import SpecError

        specs = cls.all_from_file(path)
        if len(specs) != 1:
            raise SpecError(f"Spec file '{path}' must hold exactly one document, found {len(specs)}")
        return specs[0]

    @classmethod
    def all_from_file(cls, path: str | Path) -> list[Spec]:
        """Load every YAML document of a file as a spec, interpolating ``${VAR}`` placeholders.

        ``${VAR}`` placeholders in any string value are replaced from the
        process environment at load time, so credentials never need to live
        in the file. Unresolved variables are a hard error, reported for the
        whole file at once.

        Several documents are how a graph of roots travels in one file: they
        share one id space, so a ``{"ref": id}`` in one document may name a
        component another document carries.

        Args:
            path: Filesystem path to the YAML spec document(s), as a string
                or ``Path``.

        Returns:
            The validated specs, in document order.

        Raises:
            SpecError: If the file is missing, unparsable, holds a document
                that is not a mapping, references undefined environment
                variables, or holds an invalid spec.
        """
        import yaml
        from pydantic import ValidationError

        from interloper.errors import SpecError

        path = Path(path)
        try:
            text = path.read_text()
        except OSError as exception:
            raise SpecError(f"Cannot read spec file '{path}': {exception}") from exception
        try:
            documents = list(yaml.safe_load_all(text)) or [None]
        except yaml.YAMLError as exception:
            raise SpecError(f"Invalid YAML in spec file '{path}': {exception}") from exception
        if any(not isinstance(document, dict) for document in documents):
            raise SpecError(f"Spec file '{path}' must be a YAML mapping")

        missing: set[str] = set()
        documents = [cls._interpolate_env(document, missing) for document in documents]
        if missing:
            raise SpecError(
                f"Spec file '{path}' references undefined environment variable(s): {', '.join(sorted(missing))}"
            )

        try:
            return [cls.model_validate(document) for document in documents]
        except ValidationError as exception:
            raise SpecError(f"Invalid spec file '{path}': {exception}") from exception

    @classmethod
    def reference(cls, component_id: str) -> dict[str, str]:
        """Build the reference value that stands in for an already-emitted component.

        Args:
            component_id: Id of the component the reference names.

        Returns:
            The reference mapping, ``{"ref": id}``.
        """
        return {cls.REFERENCE_KEY: component_id}

    @classmethod
    def is_reference(cls, value: Any) -> bool:
        """Whether a loaded init value is a reference rather than a component.

        Args:
            value: The value to inspect, as the document carries it.

        Returns:
            True for a mapping whose only key is ``ref``.
        """
        return isinstance(value, dict) and set(value) == {cls.REFERENCE_KEY}

    @classmethod
    def dump_value(cls, value: Any) -> Any:
        """Serialize a component field value for a :class:`Spec` init payload.

        The wire format is uniform: **anything with class identity is
        Serializable** and serializes via its own spec; lists and dicts are
        walked; everything else must be a JSON-able scalar.

        Args:
            value: The field value to serialize: a ``Serializable``, a list
                or dict of values to walk, or a JSON-able scalar.

        Returns:
            A JSON-able value understood by ``Spec.reconstruct``.
        """
        from pydantic_core import to_jsonable_python

        if isinstance(value, Serializable):
            return value.to_spec().model_dump(mode="json")
        if isinstance(value, (list, tuple)):
            return [cls.dump_value(v) for v in value]
        if isinstance(value, dict):
            return {k: cls.dump_value(v) for k, v in value.items()}
        return to_jsonable_python(value)

    @classmethod
    def _interpolate_env(cls, value: Any, missing: set[str]) -> Any:
        """Recursively substitute ``${VAR}`` placeholders in string values.

        Unknown variables are collected into *missing* (and left in place) so
        the caller can report them all at once.

        Args:
            value: The loaded YAML value to walk: strings are substituted,
                dicts and lists are recursed into, anything else passes through.
            missing: Mutable accumulator that collects the names of environment
                variables referenced but not defined.

        Returns:
            The value with all resolvable placeholders substituted.
        """
        if isinstance(value, str):

            def sub(match: re.Match[str]) -> str:
                name = match.group(1)
                if name not in os.environ:
                    missing.add(name)
                    return match.group(0)
                return os.environ[name]

            return _ENV_VAR_RE.sub(sub, value)
        if isinstance(value, dict):
            return {k: cls._interpolate_env(v, missing) for k, v in value.items()}
        if isinstance(value, list):
            return [cls._interpolate_env(v, missing) for v in value]
        return value

    def reconstruct(
        self,
        catalog: Catalog | None = None,
        *,
        resolve: Callable[[str], Component] | None = None,
        context: SerializationContext | None = None,
    ) -> Serializable:
        """Import the class and rebuild the instance, walking nested specs.

        ``key`` references resolve through the catalog and must name
        components; ``path`` references import directly and accept any
        :class:`Serializable` class (a normalizer nested in an asset's config,
        for example).

        Reconstruction runs in two passes over one
        :class:`SerializationContext`. This method is the first: every inline
        component is built and added to the context, and a ``{"ref": id}``
        relation value is held back rather than constructed with, so a
        component is only ever built from the targets the document carries
        inline. The second pass, :meth:`SerializationContext.bind`, binds
        those references and validates what they complete. Without a
        *context* this call owns one and runs both passes; with one it
        contributes to a document the caller finishes.

        Args:
            catalog: Catalog used to resolve ``key`` references, passed down
                to nested specs. Defaults to the settings-configured catalog,
                built lazily when a key is first encountered.
            resolve: Called with the id of a reference the document itself
                does not carry, to reach a component that lives outside it.
                ``None`` makes such a reference an error. Only read when this
                call starts the context; a given *context* carries its own.
            context: The state shared by every spec of a multi-root document.
                ``None`` starts one for this spec alone and binds its
                references before returning.

        Returns:
            The reconstructed instance.
        """
        owns = context is None
        context = SerializationContext(resolve=resolve) if context is None else context

        def load(value: Any) -> Any:
            if isinstance(value, dict):
                if ("path" in value or "key" in value) and value.keys() <= {"path", "key", "id", "init"}:
                    return Spec(**value).reconstruct(catalog, context=context)
                return {name: load(entry) for name, entry in value.items()}
            if isinstance(value, list):
                return [load(entry) for entry in value]
            return value

        from interloper.component.base import Component

        cls = Component.resolve_key(self.key, catalog) if self.key else Serializable.resolve_path(self.path)
        kwargs: dict[str, Any] = {"id": self.id} if self.id else {}
        for name, value in (self.init or {}).items():
            kwargs[name] = load(value)

        if not issubclass(cls, Component):
            return cls(**kwargs)

        instance = cls(**context.hold(kwargs))
        context.add(instance)
        if owns:
            context.bind()
        return instance


# -- Serialization context -----------------------------------------------------
class SerializationContext:
    """The state one manifest's specs share, on the way out and on the way back in.

    A manifest nests each component under the one that owns it and writes any
    other occurrence as ``{"ref": id}``, so writing has to know what it has
    already written and reading has to know what it has already built. Both
    are the components the context carries. Writing consults them through
    :meth:`emit`, which decides between a full spec, a reference and nothing
    at all; reading fills them through :meth:`hold` and :meth:`add` and
    settles the references with :meth:`bind`.

    A context started with components is **closed**: it carries exactly those
    (a graph's nodes), writes the carried instance in place of any other copy
    of it (:meth:`carried`), and drops a reference to an owned component it
    does not carry, since nothing in the document could answer it. A context
    started empty is **open** and writes such a reference for whoever reads
    the document to resolve. Every public entry point (``to_spec()``,
    ``reconstruct()``, ``DAG.to_spec()``) starts its own context; one is
    passed by hand only where several roots must share one, which is the
    DAG's job.
    """

    def __init__(
        self,
        components: Iterable[Component] = (),
        *,
        resolve: Callable[[str], Component] | None = None,
    ) -> None:
        """Start a context, closed over *components* when any are given.

        Args:
            components: The components this context carries from the start,
                a graph's nodes. Given, the context is closed; empty, open.
            resolve: Called with the id of a reference the context does not
                carry when reading, to reach a component that lives outside
                the document. ``None`` makes such a reference an error.
        """
        self.components: dict[str, Component] = {component.id: component for component in components}
        self.closed = bool(self.components)
        self._resolve = resolve
        self._owed: list[tuple[str, str, list[Any]]] = []

    # -- Both directions -------------------------------------------------------
    def add(self, instance: Component) -> None:
        """Record that the context carries a component and the children it owns.

        Writing calls it for what it has written in full, reading for what it
        has built; either way a later mention becomes a reference. What the
        context already carries stays, and a closed context's owned
        components are fixed from the start: it learns of a parentless
        component it writes on the way (a destination, in full, once) but
        never of an owned one, since a graph's own copies of the assets it
        holds are what it writes, not the originals their source holds.

        Args:
            instance: The component just written or just built.
        """
        self.components.setdefault(instance.id, instance)
        if not self.closed:
            for child in instance.owned:
                self.components.setdefault(child.id, child)

    def carried(self, components: Iterable[Component]) -> list[Component]:
        """The instances a writer should write for some owned components.

        A closed context writes only the owned components it carries, and
        writes its own instance of each (a graph's copy of an asset, flagged
        read-only, rather than the source's original). An open context writes
        every one, as given.

        Args:
            components: The owned components as their owner holds them.

        Returns:
            The instances to write, in the given order.
        """
        if not self.closed:
            return list(components)
        return [self.components[component.id] for component in components if component.id in self.components]

    # -- Writing ---------------------------------------------------------------
    def emit(self, target: Component) -> dict[str, Any] | None:
        """Write one bound target: in full, as a reference, or not at all.

        The manifest rule in one place. A target that has an owner travels
        inside that owner's spec, so it is always a reference; a closed
        context that does not carry it drops it instead, since nothing in
        the document would answer the reference. A target without an owner
        is written in full the first time the traversal reaches it and as a
        reference afterwards.

        Args:
            target: The bound component to write.

        Returns:
            The target's spec as a JSON-able mapping, the ``{"ref": id}``
            standing in for it, or ``None`` when the document cannot carry
            it.
        """
        if target.parent is not None:
            if self.closed and target.id not in self.components:
                return None
            return Spec.reference(target.id)
        if target.id in self.components:
            return Spec.reference(target.id)
        return target.to_spec(context=self).model_dump(mode="json", exclude_defaults=True)

    # -- Reading ---------------------------------------------------------------
    def hold(self, init: dict[str, Any]) -> dict[str, Any]:
        """Take the references out of a loaded init, at every depth.

        A value holding a reference is a relation's targets, and the mapping
        holding that value is a component's init: the value is held back
        whole, in its original order (an inline target it mixes with stays
        in place, so what the document put first stays first), and the
        mapping is pinned to an id, generated when it declares none, so the
        component built from it can be found again by :meth:`bind`. A source's
        assets are such nested mappings, which is how an asset's reference
        travels without the source knowing references exist.

        Args:
            init: Constructor keyword arguments as :meth:`Spec.reconstruct`
                loaded them, every nested spec already an instance and every
                ``{"ref": id}`` still a mapping.

        Returns:
            The keyword arguments to construct with.
        """
        kept: dict[str, Any] = {}
        held: dict[str, list[Any]] = {}
        for name, value in init.items():
            entries = list(value) if isinstance(value, (list, tuple)) else [value]
            if any(Spec.is_reference(entry) for entry in entries):
                held[name] = [entry[Spec.REFERENCE_KEY] if Spec.is_reference(entry) else entry for entry in entries]
            elif isinstance(value, dict):
                kept[name] = self.hold(value)
            else:
                kept[name] = value
        if held:
            owner = kept.get("id") or str(uuid.uuid4())
            kept["id"] = owner
            self._owed.extend((owner, name, entries) for name, entries in held.items())
        return kept

    def bind(self) -> None:
        """Bind every held reference, then check each root.

        The second pass of reading. A reference resolves against the context
        first and through *resolve* only when the context does not carry it.
        Validation comes last, once nothing is missing, on every root: a
        parent cascades into the children it owns.

        Raises:
            SpecError: If a pinned init built no component, or a reference
                names one that neither the context nor *resolve* supplies.
        """
        from interloper.errors import SpecError

        owed, self._owed = self._owed, []
        for owner_id, name, entries in owed:
            owner = self.components.get(owner_id)
            if owner is None:
                raise SpecError(f"no component was built for '{owner_id}', which holds a reference under '{name}'")
            owner.bind(name, *(self._lookup(entry) if isinstance(entry, str) else entry for entry in entries))
        for component in list(self.components.values()):
            if component.parent is None:
                component.validate_relations()

    def _lookup(self, reference: str) -> Component:
        """Find the component a reference names.

        Args:
            reference: The referenced component's id.

        Returns:
            The referenced component.

        Raises:
            SpecError: If neither the context nor *resolve* supplies it.
        """
        from interloper.errors import SpecError

        target = self.components.get(reference)
        if target is None and self._resolve is not None:
            target = self._resolve(reference)
        if target is None:
            raise SpecError(f"unresolved reference '{reference}'")
        return target


# -- Serializable --------------------------------------------------------------
class Serializable(BaseModel):
    """A class-identified, serializable configuration object.

    Anything whose instances are "a class plus its configuration" extends
    ``Serializable``: it round-trips through :class:`Spec`
    (``to_spec()`` / ``from_spec()``), resolves back from its import path,
    exposes a JSON Schema of its user-configurable fields, and rejects
    unknown constructor kwargs loudly. Runners, schemas and normalizers are
    ``Serializable``; catalog citizens extend :class:`Component`, which adds
    kind, identity and relations on top.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, ignored_types=(IgnoredDescriptor,))

    key: ClassVar[str] = ""
    name: ClassVar[str] = ""
    internal_fields: ClassVar[frozenset[str]] = frozenset()

    # -- Construction ----------------------------------------------------------
    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Auto-derive ``key`` as the snake_cased class name unless declared.

        Args:
            **kwargs: Class-creation keyword arguments, forwarded untouched to
                ``super().__init_subclass__``.
        """
        super().__init_subclass__(**kwargs)
        if "key" not in cls.__dict__:
            cls.key = to_snake_case(cls.__name__)

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        """Restore declaration order for fields that shadow a parent ClassVar.

        Pydantic merges annotations across the MRO with dict-update
        semantics, so a subclass field whose name is also *annotated* on a
        parent, e.g. a schema column named ``name``, which every
        Serializable declares as a ClassVar, is hoisted to the parent's
        annotation slot, landing first in ``model_fields`` regardless of
        where the subclass declares it. Rebuild the order from each class's
        own annotations, counting only occurrences that are actual fields
        on that class, so a ClassVar annotation no longer pins a position.

        Args:
            **kwargs: Class-creation keyword arguments, forwarded untouched to
                ``super().__pydantic_init_subclass__``.
        """
        super().__pydantic_init_subclass__(**kwargs)
        fields = cls.__pydantic_fields__
        order: dict[str, None] = {}
        for klass in reversed(cls.__mro__):
            klass_fields = getattr(klass, "__pydantic_fields__", {})
            for field_name in klass.__dict__.get("__annotations__", {}):
                if field_name in fields and field_name in klass_fields:
                    order.setdefault(field_name, None)
        reordered = {field_name: fields[field_name] for field_name in order}
        reordered.update({field_name: info for field_name, info in fields.items() if field_name not in reordered})
        if list(reordered) == list(fields):
            return
        cls.__pydantic_fields__ = reordered
        # With unresolved forward refs the core schema is built lazily and
        # picks up the reordered dict; rebuilding now would fail resolution.
        if cls.__pydantic_fields_complete__:
            cls.model_rebuild(force=True)

    def __init__(self, /, **data: Any) -> None:
        """Validate kwargs strictly against the model's fields.

        Unknown kwargs are a loud error rather than pydantic's silent
        ``extra="ignore"`` drop: a misnamed field would otherwise vanish.

        Args:
            **data: Field values, keyed by field name. Every key must name a
                field declared on the model.

        Raises:
            TypeError: If a kwarg matches no field.
        """
        unknown = [name for name in data if name not in type(self).model_fields]
        if unknown:
            raise TypeError(f"{type(self).__name__} got unexpected keyword argument(s): {', '.join(sorted(unknown))}")
        super().__init__(**data)

    @classmethod
    def build_class(
        cls,
        decorated: type,
        *,
        classvars: dict[str, Any] | None = None,
        fields: dict[str, Any] | None = None,
    ) -> type[Self]:
        """Build a subclass of this class from a decorated class.

        The decorator-support factory. The invariant: **decorators build
        classes, they never mutate finalized ones.**  Field defaults always
        pass through the Pydantic metaclass so they become real field
        definitions: a plain ``setattr`` on a built pydantic class would
        leave ``model_fields`` (and therefore every instance) on the old
        default.

        Two construction paths:

        - The decorated class already extends the receiving class: field
          defaults (when present) produce a new subclass via
          :func:`pydantic.create_model` with the parent's annotations;
          ClassVars are stamped on the result (plain class attributes, no
          pydantic machinery involved).
        - The decorated class does **not** extend it: a new class is
          created via ``type()`` that inherits from the receiving class and
          carries over the decorated class's members and annotations, with
          decorator fields annotated from the receiver's field definitions.

        Args:
            decorated: The decorated class to transform.
            classvars: Class-level attributes (key, name, tags, …).
            fields: Field default overrides (dataset, normalizer, …); every
                key must be an existing field on the receiving class.

        Returns:
            A subclass of the receiving class.

        Raises:
            TypeError: If a decorator field is not a field of the receiving
                class.
        """
        from pydantic import create_model

        already_subclass = any(isinstance(b, type) and issubclass(b, cls) for b in decorated.__bases__)

        # A decorated subclass may carry fields the receiving class doesn't
        # declare (e.g. DatabaseDestination traits behind @destination), so
        # validate against the class whose definitions the overrides target.
        target = cast("type[Self]", decorated) if already_subclass else cls
        for field_name in fields or {}:
            if field_name not in target.model_fields:
                raise TypeError(f"Decorator field '{field_name}' is not a field of {target.__name__}.")

        if already_subclass:
            result_cls = cast("type[Self]", decorated)
            if fields:
                # Override only the default, keeping the parent FieldInfo:
                # a bare (annotation, value) pair would build a fresh
                # FieldInfo and silently drop the field's title, description
                # and json_schema_extra (x-widget, x-info, …).
                field_definitions: dict[str, Any] = {
                    name: (info.annotation, info) for name, info in cls._override_defaults(result_cls, fields).items()
                }
                result_cls = create_model(
                    decorated.__name__,
                    __base__=result_cls,
                    __module__=decorated.__module__,
                    **field_definitions,
                )
                result_cls.__qualname__ = decorated.__qualname__
                result_cls.__doc__ = decorated.__doc__
            if classvars:
                for name, value in classvars.items():
                    setattr(result_cls, name, value)
            return result_cls

        # Plain class: build a new class that inherits from the receiver.
        namespace: dict[str, Any] = {}

        for name, value in decorated.__dict__.items():
            if name.startswith("__"):
                continue
            namespace[name] = value

        if classvars:
            namespace.update(classvars)
        if fields:
            # Merged FieldInfos, not bare values: a bare `field = value` in
            # the namespace replaces the receiver's FieldInfo wholesale,
            # dropping title/description/json_schema_extra.
            namespace.update(cls._override_defaults(cls, fields))

        namespace["__module__"] = decorated.__module__
        namespace["__qualname__"] = decorated.__qualname__

        # Carry over annotations so Pydantic sees declared fields.
        if "__annotations__" in decorated.__dict__:
            namespace.setdefault("__annotations__", {}).update(decorated.__dict__["__annotations__"])

        # Annotate decorator fields from the receiver's field definitions so
        # the metaclass registers them as field default overrides.
        if fields:
            annotations = namespace.setdefault("__annotations__", {})
            for field_name in fields:
                if field_name not in annotations:
                    annotations[field_name] = cls.model_fields[field_name].annotation

        # Annotate classvars as ClassVar so Pydantic doesn't treat them as
        # model fields.  Without this, a bare `tags = ["Cloud"]` in the
        # namespace triggers a PydanticUserError for non-annotated attributes.
        if classvars:
            annotations = namespace.setdefault("__annotations__", {})
            for cv_name in classvars:
                if cv_name not in annotations:
                    annotations[cv_name] = ClassVar

        result_cls = type(decorated.__name__, (cls,), namespace)

        if decorated.__doc__:
            result_cls.__doc__ = decorated.__doc__

        return cast("type[Self]", result_cls)

    # -- Identity --------------------------------------------------------------

    @staticmethod
    def _override_defaults(owner: type[BaseModel], overrides: dict[str, Any]) -> dict[str, FieldInfo]:
        """Copy *owner*'s FieldInfos with new defaults, keeping all other metadata.

        Args:
            owner: The model whose ``model_fields`` supply the FieldInfo to copy;
                every override key must name one of its fields.
            overrides: Field name → new default value. Any ``default_factory``
                on the copied FieldInfo is cleared so the value wins.

        Returns:
            Field name → copied FieldInfo carrying the override as its default.
        """
        infos: dict[str, FieldInfo] = {}
        for name, value in overrides.items():
            info = copy.deepcopy(owner.model_fields[name])
            info.default = value
            info.default_factory = None
            infos[name] = info
        return infos

    def __str__(self) -> str:
        """Human-readable representation: ``Name (key: k)``.

        Returns:
            Formatted string with class name and key.
        """
        return f"{type(self).__name__} (key: {self.key})"

    @classmethod
    def classpath(cls) -> str:
        """Fully qualified import path for this class.

        Returns:
            Dotted path like ``"module.submodule.ClassName"``.
        """
        return get_object_path(cls)

    def path(self) -> str:
        """Fully qualified import path for this instance's class.

        Returns:
            The class's :meth:`classpath`.
        """
        return type(self).classpath()

    # -- Serialization & resolution --------------------------------------------
    def to_spec(self) -> Spec:
        """Serialize this instance to a reconstructible spec.

        Returns:
            A Spec capturing this instance's state.
        """
        return self._build_spec(init=self._fields_init() or None)

    def _build_spec(self, *, init: dict[str, Any] | None) -> Spec:
        """Build the spec envelope an instance of this class serializes into.

        The one construction every ``to_spec`` builds from, so none writes
        ``Spec(path=..., ...)`` by hand: a ``Component`` also carries an
        ``id``, which it patches onto the result afterwards.

        Args:
            init: The init payload to carry, or ``None`` for an instance
                with nothing to configure.

        Returns:
            The spec envelope, ``id`` left at its default.
        """
        return Spec(path=self.path(), init=init)

    def _fields_init(self, *, without: Collection[str] = ()) -> dict[str, Any]:
        """Serialize this instance's fields into a spec init payload.

        Args:
            without: Field names to leave out, for an owner that writes some
                of them itself.

        Returns:
            Field name to JSON-able value. ``id`` rides the spec envelope
            rather than the payload, and a ``None`` value is omitted, so
            neither ever appears here.
        """
        init: dict[str, Any] = {}
        for name in type(self).model_fields:
            if name == "id" or name in without:
                continue
            value = getattr(self, name)
            if value is None:
                continue
            init[name] = Spec.dump_value(value)
        return init

    @classmethod
    def resolve_path(cls, path: str) -> type[Self]:
        """Resolve an import path to a class of this (sub)class.

        Accepts dotted and composite paths (``module.Class``,
        ``module:Source.Asset``). Called on a subclass, the resolved class
        must be of that subclass; anything else raises ``TypeError``.

        Args:
            path: Dotted or composite import path to resolve.

        Returns:
            The resolved class.
        """
        return cls._resolve_import(path, ref=path)

    @classmethod
    def _resolve_import(cls, path: str, *, ref: str) -> type[Self]:
        """Import *path* and check the result against the receiving class.

        Args:
            path: Dotted or composite import path to import.
            ref: The reference as the caller wrote it, quoted in the error
                message: a catalog key, say, rather than the resolved path.

        Returns:
            The resolved class.

        Raises:
            TypeError: If the import is not a subclass of the receiving class.
        """
        resolved = import_from_path(path)
        if not isinstance(resolved, type) or not issubclass(resolved, cls):
            raise TypeError(f"'{ref}' does not resolve to a {cls.__name__} class")
        return cast("type[Self]", resolved)

    @classmethod
    def from_spec(
        cls,
        spec: Spec | dict[str, Any],
        catalog: Catalog | None = None,
        *,
        resolve: Callable[[str], Component] | None = None,
    ) -> Self:
        """Reconstruct an instance from a spec.

        Called on a subclass, the reconstructed instance must be of that
        subclass: ``Source.from_spec(spec)``.

        Args:
            spec: The spec (or its dict payload) to reconstruct.
            catalog: Catalog used to resolve ``key`` references. Defaults to
                the settings-configured catalog, built lazily.
            resolve: Called with the id of a reference the spec itself does
                not carry; ``None`` makes such a reference an error.

        Returns:
            The reconstructed instance.

        Raises:
            TypeError: If the spec reconstructs to an instance that is not
                of the receiving class.
        """
        if isinstance(spec, dict):
            spec = Spec(**spec)
        instance = spec.reconstruct(catalog, resolve=resolve)
        if not isinstance(instance, cls):
            raise TypeError(f"'{spec.key or spec.path}' does not reconstruct to a {cls.__name__}")
        return instance

    @classmethod
    def from_spec_file(
        cls,
        path: str | Path,
        catalog: Catalog | None = None,
        *,
        resolve: Callable[[str], Component] | None = None,
    ) -> Self:
        """Reconstruct an instance from a spec file.

        Loads a :class:`Spec` document from YAML (with ``${VAR}``
        env interpolation) and reconstructs it, with the same
        subclass-scoped check as :meth:`from_spec`: invalid documents
        surface as ``SpecError``, mismatched kinds as ``TypeError``.

        Args:
            path: Filesystem path to the YAML spec document, as a string or
                ``Path``.
            catalog: Catalog used to resolve ``key`` references. Defaults to
                the settings-configured catalog, built lazily.
            resolve: Called with the id of a reference the document itself
                does not carry; ``None`` makes such a reference an error.

        Returns:
            The reconstructed instance.
        """
        return cls.from_spec(Spec.from_file(path), catalog, resolve=resolve)

    # -- Definition ------------------------------------------------------------
    @classmethod
    def config_schema(cls) -> dict[str, Any]:
        """JSON Schema of the class's user-configurable fields.

        Returns:
            The stripped schema, or ``{}`` when no configurable field remains.
        """
        from interloper.resource.fields import strip_internal_fields

        raw = cls.model_json_schema() if hasattr(cls, "model_json_schema") else {}
        schema = strip_internal_fields(raw, extra=cls.internal_fields)
        return schema if schema.get("properties") else {}


