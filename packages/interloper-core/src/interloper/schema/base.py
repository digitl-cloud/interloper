"""Schema: component for defining asset output structure, with inference, validation, and reconciliation."""

from __future__ import annotations

import json
import logging
import types
import warnings
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Union, get_args, get_origin

from pydantic import BaseModel, ConfigDict, TypeAdapter, ValidationError, create_model
from typing_extensions import Self

from interloper.errors import SchemaError
from interloper.serializable import Serializable

warnings.filterwarnings("ignore", message=r'Field name ".*" in ".*" shadows an attribute in parent "Schema"')

logger = logging.getLogger(__name__)


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

    @classmethod
    def from_annotation(cls, name: str, annotation: Any, description: str | None = None) -> FieldSpec:
        """Build a FieldSpec from a field name, type annotation, and description.

        Args:
            name: Field name.
            annotation: The declared type annotation, optionally wrapped in
                ``T | None`` and/or ``list[T]``.
            description: Human-readable field description, or ``None`` when the
                field declares none. Defaults to ``None``.

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
            fields = tuple(
                cls.from_annotation(n, f.annotation, f.description) for n, f in annotation.model_fields.items()
            )

        return FieldSpec(
            name=name, type=annotation, nullable=nullable, repeated=repeated, fields=fields, description=description
        )


class Schema(Serializable):
    """Defines the expected output structure of an asset.

    Subclass to declare output fields::

        class UserSchema(Schema):
            id: int
            name: str
            email: str

    Class methods provide schema operations on ``list[dict]`` data::

        Schema.infer(rows)
        UserSchema.validate_rows(rows)
        UserSchema.reconcile(rows)
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    # -- Introspection ---------------------------------------------------------

    @classmethod
    def field_specs(cls) -> list[FieldSpec]:
        """Extract backend-agnostic field specs from this schema.

        Fields declared by :class:`Schema` itself (framework plumbing, if
        any) are excluded — only the subclass's data columns count.  Fields
        come out in declaration order: :class:`Serializable` normalizes
        ``model_fields`` so a column shadowing a parent ClassVar (``name``,
        ``key``) is not hoisted out of place.

        Returns:
            One :class:`FieldSpec` per declared data field, in declaration order.
        """
        data_fields = cls._data_fields()
        return [
            FieldSpec.from_annotation(name, info.annotation, info.description)
            for name, info in cls.model_fields.items()
            if name in data_fields
        ]

    @classmethod
    def json_schema(cls) -> dict[str, Any]:
        """JSON Schema for this schema's data fields only.

        Like :meth:`pydantic.BaseModel.model_json_schema` but restricted to
        the subclass's data columns, in declaration order (see
        :meth:`field_specs`).

        Returns:
            A JSON Schema dict whose ``properties`` are the data columns.
        """
        schema = cls.model_json_schema()
        data_order = [spec.name for spec in cls.field_specs()]
        properties = schema.get("properties", {})
        schema["properties"] = {name: properties[name] for name in data_order if name in properties}
        if "required" in schema:
            required = [name for name in schema["required"] if name in properties]
            if required:
                schema["required"] = required
            else:
                del schema["required"]
        return schema

    # -- Data operations -------------------------------------------------------

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
            field_type = cls._resolve_field_type(types_seen)
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
                # Non-strict validation tolerates extra columns; strict mode
                # has already rejected them above. Filter before validating so
                # unknown keys never reach component construction.
                cls.model_validate({k: v for k, v in row.items() if k in cls.model_fields})
            except ValidationError as e:
                raise SchemaError(f"Schema validation failed on row {i}: {e}") from e

    @classmethod
    def reconcile(
        cls,
        rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Reconcile rows against this schema.

        Fields are coerced one by one through per-field ``TypeAdapter``s —
        the row-wise mirror of the DataFrame conformer's column-wise casts.
        Extra keys are dropped (their union logged as a warning), missing
        fields get their default (or ``None``, which pydantic accepts only
        for nullable fields), and scalar ``str`` fields are stringified
        up front since pydantic's lax mode never coerces *to* ``str``.

        This is more permissive than :meth:`validate` — it actively
        transforms data to match the schema rather than rejecting mismatches.

        Args:
            rows: List of row dicts.

        Returns:
            A new list of row dicts with columns aligned and types coerced.

        Raises:
            SchemaError: If a value cannot be coerced (e.g. ``"abc"`` → ``int``)
                or a required non-nullable field is missing.
        """
        if not rows:
            return []

        specs = cls.field_specs()
        fields = {spec.name: cls.model_fields[spec.name] for spec in specs}
        adapters = {name: TypeAdapter(field.annotation) for name, field in fields.items()}
        coercers = {
            spec.name: RECONCILERS[spec.type]
            for spec in specs
            if spec.type in RECONCILERS and not spec.repeated and spec.fields is None
        }

        dropped: set[str] = set()
        result: list[dict[str, Any]] = []
        for i, row in enumerate(rows):
            dropped.update(row.keys() - fields.keys())
            out: dict[str, Any] = {}
            for name, adapter in adapters.items():
                if name not in row and not fields[name].is_required():
                    out[name] = fields[name].get_default(call_default_factory=True)
                    continue
                value = row.get(name)
                if name in coercers:
                    value = coercers[name](value)
                try:
                    # dump_python round-trips nested models back to plain dicts.
                    out[name] = adapter.dump_python(adapter.validate_python(value))
                except ValidationError as e:
                    raise SchemaError(f"Reconciliation failed on row {i}: {e}") from e
            result.append(out)
        if dropped:
            logger.warning(
                "Reconciliation to schema '%s' dropped fields not in the schema: %s", cls.__name__, sorted(dropped)
            )
        return result

    # -- Internals -------------------------------------------------------------

    @staticmethod
    def _resolve_field_type(types_seen: set[type]) -> type:
        """Resolve a set of observed Python types into a single Pydantic-compatible type.

        Rules:
        - Empty set (all values None) → ``Any``
        - Single type → that type
        - ``{int, float}`` → ``float`` (numeric widening)
        - Multiple incompatible types → ``Any``

        Args:
            types_seen: The distinct non-``None`` Python types observed for one
                key across all rows. Consumed in place.

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

    @classmethod
    def _data_fields(cls) -> set[str]:
        """Return the names of the schema's data fields.

        Excludes any field declared by :class:`Schema` itself — framework
        plumbing must not appear in reconciled rows or count as schema
        columns.

        Returns:
            The subclass-declared field names, unordered.
        """
        return {name for name in cls.model_fields if name not in Schema.model_fields}


def _coerce_str_value(value: Any) -> Any:
    """Coerce a value bound for a scalar ``str`` field to a string.

    ``None`` passes through (nullability is pydantic's call), nested
    ``list``/``dict`` values are JSON-encoded, anything else is stringified.

    Args:
        value: The raw row value bound for a scalar ``str`` field.

    Returns:
        The coerced value.
    """
    if value is None or isinstance(value, str):
        return value
    if isinstance(value, (list, dict)):
        return json.dumps(value)
    return str(value)


#: Pre-coercions applied before pydantic validation, keyed by the scalar
#: spec type. Fills the gaps in pydantic's lax conversion table — which
#: coerces between scalars in every direction except *to* ``str``.
RECONCILERS: dict[type, Callable[[Any], Any]] = {
    str: _coerce_str_value,
}

