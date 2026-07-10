"""Serializable: class-plus-configuration objects and their wire format.

Anything whose instances are "a class plus its configuration" extends
:class:`Serializable`; :class:`Spec` is its serialized form — an envelope
of ``path`` (or catalog ``key``), optional ``id``, and the ``init``
payload. ``to_spec()`` / ``from_spec()`` round-trip between the two.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, cast

from pydantic import BaseModel, ConfigDict, model_validator
from typing_extensions import Self

from interloper.utils.imports import get_object_path, import_from_path
from interloper.utils.text import to_snake_case

if TYPE_CHECKING:
    from interloper.catalog.base import Catalog


class IgnoredDescriptor:
    """Marker base class for descriptors that Pydantic should ignore on Components.

    Any descriptor extending this class is automatically excluded from Pydantic
    model field processing via ``Component.model_config["ignored_types"]``.
    """


def dump_spec_value(value: Any) -> Any:
    """Serialize a component field value for a :class:`Spec` init payload.

    The wire format is uniform: **anything with class identity is
    Serializable** and serializes via its own spec; lists and dicts are
    walked; everything else must be a JSON-able scalar.

    Returns:
        A JSON-able value understood by ``Spec.reconstruct``.
    """
    from pydantic_core import to_jsonable_python

    if isinstance(value, Serializable):
        return value.to_spec().model_dump(mode="json")
    if isinstance(value, (list, tuple)):
        return [dump_spec_value(v) for v in value]
    if isinstance(value, dict):
        return {k: dump_spec_value(v) for k, v in value.items()}
    return to_jsonable_python(value)


# -- Specs ---------------------------------------------------------------------
_ENV_VAR_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def _interpolate_env(value: Any, missing: set[str]) -> Any:
    """Recursively substitute ``${VAR}`` placeholders in string values.

    Unknown variables are collected into *missing* (and left in place) so
    the caller can report them all at once.

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
        return {k: _interpolate_env(v, missing) for k, v in value.items()}
    if isinstance(value, list):
        return [_interpolate_env(v, missing) for v in value]
    return value


class Spec(BaseModel):
    """Serialized representation of a Component instance.

    A component is referenced by exactly one of two names, each keeping its
    own meaning: ``path`` is a fully qualified import path (what
    ``to_spec()`` emits), ``key`` is a catalog key — hand-authored specs may
    reference components the way the catalog names them.
    """

    path: str = ""
    key: str = ""
    id: str = ""
    init: dict[str, Any] | None = None

    @model_validator(mode="after")
    def _check_reference(self) -> Spec:
        if bool(self.path) == bool(self.key):
            raise ValueError("exactly one of 'path' or 'key' must be set")
        return self

    @classmethod
    def from_file(cls, path: str | Path) -> Spec:
        """Load a spec from a YAML file, interpolating ``${VAR}`` placeholders.

        ``${VAR}`` placeholders in any string value are replaced from the
        process environment at load time, so credentials never need to live
        in the file. Unresolved variables are a hard error.

        Returns:
            The validated spec.

        Raises:
            SpecError: If the file is missing, unparsable, references
                undefined environment variables, or is not a valid spec.
        """
        import yaml
        from pydantic import ValidationError

        from interloper.errors import SpecError

        path = Path(path)
        try:
            text = path.read_text()
        except OSError as exc:
            raise SpecError(f"Cannot read spec file '{path}': {exc}") from exc
        try:
            data = yaml.safe_load(text)
        except yaml.YAMLError as exc:
            raise SpecError(f"Invalid YAML in spec file '{path}': {exc}") from exc
        if not isinstance(data, dict):
            raise SpecError(f"Spec file '{path}' must be a YAML mapping")

        missing: set[str] = set()
        data = _interpolate_env(data, missing)
        if missing:
            raise SpecError(
                f"Spec file '{path}' references undefined environment variable(s): {', '.join(sorted(missing))}"
            )

        try:
            return cls.model_validate(data)
        except ValidationError as exc:
            raise SpecError(f"Invalid spec file '{path}': {exc}") from exc

    def reconstruct(self, catalog: Catalog | None = None) -> Serializable:
        """Import the class and rebuild the instance, walking nested specs.

        ``key`` references resolve through the catalog and must name
        components; ``path`` references import directly and accept any
        :class:`Serializable` class (a normalizer nested in an asset's config,
        for example).

        Args:
            catalog: Catalog used to resolve ``key`` references, passed down
                to nested specs. Defaults to the settings-configured catalog,
                built lazily when a key is first encountered.

        Returns:
            The reconstructed instance.
        """

        def load(v: Any) -> Any:
            if isinstance(v, dict):
                if ("path" in v or "key" in v) and v.keys() <= {"path", "key", "id", "init"}:
                    return Spec(**v).reconstruct(catalog)
                return {k: load(x) for k, x in v.items()}
            if isinstance(v, list):
                return [load(x) for x in v]
            return v

        from interloper.component.base import Component

        cls = Component.resolve_key(self.key, catalog) if self.key else Serializable.resolve_path(self.path)
        kwargs: dict[str, Any] = {"id": self.id} if self.id else {}
        for k, v in (self.init or {}).items():
            kwargs[k] = load(v)
        return cls(**kwargs)


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
        """Auto-derive ``key`` as the snake_cased class name unless declared."""
        super().__init_subclass__(**kwargs)
        if "key" not in cls.__dict__:
            cls.key = to_snake_case(cls.__name__)

    def __init__(self, /, **data: Any) -> None:
        """Validate kwargs strictly against the model's fields.

        Unknown kwargs are a loud error rather than pydantic's silent
        ``extra="ignore"`` drop — a misnamed field would otherwise vanish.

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
        definitions — a plain ``setattr`` on a built pydantic class would
        leave ``model_fields`` (and therefore every instance) on the old
        default.

        Two construction paths:

        - The decorated class already extends the receiving class: field
          defaults (when present) produce a new subclass via
          :func:`pydantic.create_model` with the parent's annotations;
          ClassVars are stamped on the result (plain class attributes — no
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

        for field_name in fields or {}:
            if field_name not in cls.model_fields:
                raise TypeError(f"Decorator field '{field_name}' is not a field of {cls.__name__}.")

        already_subclass = any(isinstance(b, type) and issubclass(b, cls) for b in decorated.__bases__)

        if already_subclass:
            result_cls = cast("type[Self]", decorated)
            if fields:
                field_definitions: dict[str, Any] = {
                    name: (result_cls.model_fields[name].annotation, value) for name, value in fields.items()
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

        # Plain class — build a new class that inherits from the receiver.
        namespace: dict[str, Any] = {}

        for name, value in decorated.__dict__.items():
            if name.startswith("__"):
                continue
            namespace[name] = value

        if classvars:
            namespace.update(classvars)
        if fields:
            namespace.update(fields)

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
    @classmethod
    def has_own_field(cls, field: str) -> bool:
        """Check if this class declares a non-None default for a field.

        Returns:
            True if the class defines a non-None default for the field.
        """
        info = cls.model_fields.get(field)
        return info is not None and info.default is not None

    def __str__(self) -> str:
        """Human-readable representation: ``Name (key: k)``.

        Returns:
            Formatted string with class name and key.
        """
        return f"{type(self).__name__} (key: {type(self).key})"

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
        init: dict[str, Any] = {}
        for name in type(self).model_fields:
            if name == "id":  # identity rides the spec envelope, not the init payload
                continue
            value = getattr(self, name)
            if value is None:
                continue
            init[name] = dump_spec_value(value)

        return Spec(path=self.path(), init=init or None)

    @classmethod
    def resolve_path(cls, path: str) -> type[Self]:
        """Resolve an import path to a class of this (sub)class.

        Accepts dotted and composite paths (``module.Class``,
        ``module:Source.Asset``). Called on a subclass, the resolved class
        must be of that subclass — anything else raises ``TypeError``.

        Returns:
            The resolved class.
        """
        return cls._resolve_import(path, ref=path)

    @classmethod
    def _resolve_import(cls, path: str, *, ref: str) -> type[Self]:
        """Import *path* and check the result against the receiving class.

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
    def from_spec(cls, spec: Spec | dict[str, Any], catalog: Catalog | None = None) -> Self:
        """Reconstruct an instance from a spec.

        Called on a subclass, the reconstructed instance must be of that
        subclass: ``Source.from_spec(spec)``.

        Args:
            spec: The spec (or its dict payload) to reconstruct.
            catalog: Catalog used to resolve ``key`` references. Defaults to
                the settings-configured catalog, built lazily.

        Returns:
            The reconstructed instance.

        Raises:
            TypeError: If the spec reconstructs to an instance that is not
                of the receiving class.
        """
        if isinstance(spec, dict):
            spec = Spec(**spec)
        instance = spec.reconstruct(catalog)
        if not isinstance(instance, cls):
            raise TypeError(f"'{spec.key or spec.path}' does not reconstruct to a {cls.__name__}")
        return instance

    @classmethod
    def from_spec_file(cls, path: str | Path, catalog: Catalog | None = None) -> Self:
        """Reconstruct an instance from a spec file.

        Loads a :class:`Spec` document from YAML (with ``${VAR}``
        env interpolation) and reconstructs it, with the same
        subclass-scoped check as :meth:`from_spec` — invalid documents
        surface as ``SpecError``, mismatched kinds as ``TypeError``.

        Returns:
            The reconstructed instance.
        """
        return cls.from_spec(Spec.from_file(path), catalog)

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

