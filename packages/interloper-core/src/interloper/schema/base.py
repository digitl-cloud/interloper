"""Schema: component for defining asset output structure, with inference, validation, and reconciliation."""

from __future__ import annotations

import types
import warnings
from dataclasses import dataclass
from typing import Any, ClassVar, Union, get_args, get_origin

from pydantic import BaseModel, ConfigDict, ValidationError, create_model
from typing_extensions import Self

from interloper.component import Component
from interloper.errors import SchemaError

warnings.filterwarnings("ignore", message=r'Field name ".*" in ".*" shadows an attribute in parent "Schema"')


@dataclass(frozen=True)
class FieldSpec:
    """Backend-agnostic description of a single schema field.

    This is the canonical type contract extracted from a :class:`Schema` via
    :meth:`Schema.field_specs`.  Integration packages map specs to their native
    type systems (BigQuery ``SchemaField``, pandas dtypes, ...) so that the
    type-mapping knowledge lives in exactly one place per backend.

    Attributes:
        name: Field name.
        type: The unwrapped Python type (``int``, ``float``, ``str``, ``bool``,
            ``datetime.date``, ``datetime.datetime``, ``Decimal``, ``bytes``,
            a ``BaseModel`` subclass for nested records, ...) or ``typing.Any``
            when the type is unknown or ambiguous.
        nullable: Whether the field accepts ``None`` (declared as ``T | None``).
        repeated: Whether the field is a list of *type* (declared as ``list[T]``).
        fields: Sub-field specs when *type* is a nested model, else ``None``.
        description: Human-readable field description (from ``Field(description=...)``),
            else ``None``.
    """

    name: str
    type: Any
    nullable: bool
    repeated: bool = False
    fields: tuple[FieldSpec, ...] | None = None
    description: str | None = None


def _field_spec(name: str, annotation: Any, description: str | None = None) -> FieldSpec:
    """Build a FieldSpec from a field name, type annotation, and description.

    Returns:
        The extracted spec, with ``Optional``/``list`` wrappers unwrapped.
    """
    nullable = False

    # Unwrap Optional / unions with None
    if get_origin(annotation) in (Union, types.UnionType):
        args = get_args(annotation)
        non_none = [a for a in args if a is not type(None)]
        nullable = len(non_none) < len(args)
        annotation = non_none[0] if len(non_none) == 1 else Any

    # Unwrap list[T] into a repeated field
    repeated = False
    if get_origin(annotation) is list:
        repeated = True
        inner = get_args(annotation)
        annotation = inner[0] if inner else Any
        if get_origin(annotation) in (Union, types.UnionType):
            non_none = [a for a in get_args(annotation) if a is not type(None)]
            annotation = non_none[0] if len(non_none) == 1 else Any

    # Nested model -> sub-field specs
    fields: tuple[FieldSpec, ...] | None = None
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        fields = tuple(_field_spec(n, f.annotation, f.description) for n, f in annotation.model_fields.items())

    return FieldSpec(
        name=name, type=annotation, nullable=nullable, repeated=repeated, fields=fields, description=description
    )


class Schema(Component):
    """A component that defines the expected output structure of an asset.

    Subclass to declare output fields::

        class UserSchema(Schema):
            id: int
            name: str
            email: str

    Class methods provide schema operations on ``list[dict]`` data::

        Schema.infer(rows)
        UserSchema.validate_rows(rows)
        UserSchema.reconcile(rows)

    Note: ``id`` is excluded from Schema's model fields so subclasses
    can freely declare ``id`` as a data column with any type.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    # Shadow Component.id so it's not a model field — schemas are structural
    # definitions, not runtime instances that need identity.
    id: ClassVar[str] = ""

    # Override Component.model_post_init to avoid setting an instance id.
    def model_post_init(self, context: Any) -> None:
        """No-op: schemas don't need instance identity."""

    @classmethod
    def field_specs(cls) -> list[FieldSpec]:
        """Extract backend-agnostic field specs from this schema.

        Fields inherited from :class:`Component` (e.g. ``resources``) are
        framework plumbing, not data columns, and are excluded.  Fields are
        returned in the subclass's declaration order: pydantic positions a
        field that shadows a ``Component`` attribute (``id``, ``name``, ...)
        at the *parent's* annotation slot, so ``model_fields`` order alone
        would scramble the author's column order.

        Returns:
            One :class:`FieldSpec` per declared data field, in declaration order.
        """
        data_fields = cls._data_fields()
        names = [n for n in cls.model_fields if n in data_fields]
        own_order = [n for n in cls.__dict__.get("__annotations__", {}) if n in data_fields]
        ordered = own_order + [n for n in names if n not in own_order]
        return [
            _field_spec(name, cls.model_fields[name].annotation, cls.model_fields[name].description) for name in ordered
        ]

    @classmethod
    def infer(
        cls,
        rows: list[dict[str, Any]],
        name: str = "InferredSchema",
    ) -> type[Self]:
        """Infer a Schema subclass from a list of row dicts.

        Examines the values across all rows for each key and maps Python types
        to Pydantic field types.  All fields are ``Optional`` because any key
        may be absent in some rows.

        Args:
            rows: Non-empty list of dicts to infer from.
            name: Class name for the generated model.

        Returns:
            A dynamically created Schema subclass.

        Raises:
            SchemaError: If *rows* is empty.
        """
        if not rows:
            raise SchemaError("Cannot infer schema from empty data.")

        # Collect all non-None types seen for each key
        key_types: dict[str, set[type]] = {}
        for row in rows:
            for k, v in row.items():
                if k not in key_types:
                    key_types[k] = set()
                if v is not None:
                    key_types[k].add(type(v))

        # Build field definitions: (type | None, default_value)
        field_definitions: dict[str, Any] = {}
        for key, types_seen in key_types.items():
            field_type = _resolve_field_type(types_seen)
            field_definitions[key] = (field_type | None, None)

        return create_model(name, __base__=cls, **field_definitions)

    @classmethod
    def validate_rows(
        cls,
        rows: list[dict[str, Any]],
        *,
        strict: bool = False,
    ) -> None:
        """Validate each row against this schema.

        Stops at the first row that fails validation.

        Args:
            rows: List of row dicts.
            strict: When ``True``, reject rows that contain keys not defined
                in the schema and rows that are missing required schema fields.

        Raises:
            SchemaError: If any row fails validation.
        """
        schema_fields = cls._data_fields() if strict else None
        for i, row in enumerate(rows):
            if schema_fields is not None:
                extra = set(row.keys()) - schema_fields
                if extra:
                    raise SchemaError(
                        f"Schema validation failed on row {i}: extra fields not in schema: {sorted(extra)}"
                    )
                missing = schema_fields - set(row.keys())
                required_missing = {k for k in missing if cls.model_fields[k].is_required()}
                if required_missing:
                    raise SchemaError(
                        f"Schema validation failed on row {i}: missing required fields: {sorted(required_missing)}"
                    )
            try:
                cls.model_validate(row)
            except ValidationError as e:
                raise SchemaError(f"Schema validation failed on row {i}: {e}") from e

    @classmethod
    def reconcile(
        cls,
        rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Reconcile rows against this schema.

        For each row:
        1. Filter to only the keys defined in the schema (drop extras).
        2. For missing keys that have a default, omit them so Pydantic applies
           the default.  For missing *required* keys, supply ``None`` — Pydantic
           will accept it when the field is nullable (e.g. ``str | None``) and
           reject it otherwise, which is the desired behaviour.
        3. Coerce values to the schema's types using ``model_validate()``.

        This is more permissive than :meth:`validate` — it actively
        transforms data to match the schema rather than rejecting mismatches.

        Args:
            rows: List of row dicts.

        Returns:
            A new list of row dicts with columns aligned and types coerced.

        Raises:
            SchemaError: If any row cannot be coerced (e.g. ``"abc"`` → ``int``)
                or a required non-nullable field is missing.
        """
        if not rows:
            return []

        schema_fields = cls._data_fields()

        result: list[dict[str, Any]] = []
        for i, row in enumerate(rows):
            filtered = {k: row[k] for k in schema_fields if k in row}
            for k in schema_fields - filtered.keys():
                if cls.model_fields[k].is_required():
                    filtered[k] = None
            try:
                instance = cls.model_validate(filtered)
            except ValidationError as e:
                raise SchemaError(f"Reconciliation failed on row {i}: {e}") from e
            result.append(instance.model_dump(include=schema_fields))
        return result

    @classmethod
    def _data_fields(cls) -> set[str]:
        """Return the names of the schema's data fields.

        Excludes fields inherited from :class:`Component` (e.g. ``resources``),
        which are framework plumbing — they must not appear in reconciled rows
        or count as schema columns.
        """
        return {name for name in cls.model_fields if name not in Schema.model_fields}


def _resolve_field_type(types_seen: set[type]) -> type:
    """Resolve a set of observed Python types into a single Pydantic-compatible type.

    Rules:
    - Empty set (all values None) → ``Any``
    - Single type → that type
    - ``{int, float}`` → ``float`` (numeric widening)
    - Multiple incompatible types → ``Any``

    Returns:
        The resolved Pydantic-compatible field type.
    """
    if not types_seen:
        return Any

    if len(types_seen) == 1:
        return types_seen.pop()

    # Numeric widening: int + float -> float
    if types_seen == {int, float}:
        return float

    return Any
