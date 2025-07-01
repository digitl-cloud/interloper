"""This module contains the AssetSchema class."""
import datetime as dt
from abc import ABCMeta
from dataclasses import dataclass, fields, make_dataclass
from typing import Any, Literal, overload

PYTHON_TO_SQL_TYPE = {
    int: "INTEGER",
    float: "FLOAT",
    str: "VARCHAR",
    bool: "BOOLEAN",
    dt.date: "DATE",
    dt.datetime: "TIMESTAMP",
}


class AssetSchemaMeta(ABCMeta):
    """Metaclass for AssetSchema. Forces subclasses to be dataclasses."""

    def __new__(mcs, name: str, bases: tuple[type, ...], namespace: dict[str, Any]) -> type:
        """Create a new class."""
        cls = super().__new__(mcs, name, bases, namespace)

        # Ignore the base class
        if name == "AssetSchema":
            return cls

        # Forces subclasses to be dataclasses
        return dataclass(cls)  # type: ignore


@dataclass
class AssetSchema(metaclass=AssetSchemaMeta):
    """The base class for asset schemas."""

    @classmethod
    def from_dict(cls, schema: dict[str, type], name: str | None = None) -> type["AssetSchema"]:
        """Create a new AssetSchema from a dictionary.

        Args:
            schema: The dictionary to create the schema from.
            name: The name of the schema.

        Returns:
            A new AssetSchema.
        """
        fields = [(name, field_type) for name, field_type in schema.items()]
        return make_dataclass(name or cls.__name__, fields, bases=(cls,))

    @classmethod
    def to_dict(cls) -> dict[str, dict[str, Any]]:
        """Convert the schema to a dictionary.

        Returns:
            A dictionary representation of the schema.
        """
        return {
            f.name: {
                "type": f.type,
                "description": f.metadata.get("description"),
            }
            for f in fields(cls)
        }

    @classmethod
    @overload
    def to_tuple(
        cls, format: Literal["python"], types: dict[type, str] = PYTHON_TO_SQL_TYPE
    ) -> tuple[tuple[str, type], ...]: ...

    @classmethod
    @overload
    def to_tuple(
        cls, format: Literal["sql"], types: dict[type, str] = PYTHON_TO_SQL_TYPE
    ) -> tuple[tuple[str, str], ...]: ...

    @classmethod
    def to_tuple(
        cls,
        format: Literal["python", "sql"] = "python",
        types: dict[type, str] = PYTHON_TO_SQL_TYPE,
    ) -> tuple[tuple[str, Any], ...]:
        """Convert the schema to a tuple.

        Args:
            format: The format to convert to.
            types: The types to use for SQL conversion.

        Returns:
            A tuple representation of the schema.

        Raises:
            ValueError: If the format is invalid.
        """
        if format not in ("python", "sql"):
            raise ValueError(f"Invalid format {format}, must be one of: python, sql")

        return tuple(
            (f.name, f.type) if format == "python" else (f.name, types[f.type])  # type: ignore
            for f in fields(cls)
        )

    @classmethod
    def to_sql(cls, types: dict[type, str] = PYTHON_TO_SQL_TYPE) -> str:
        """Convert the schema to a SQL string.

        Args:
            types: The types to use for SQL conversion.

        Returns:
            A SQL string representation of the schema.

        Raises:
            ValueError: If a type is not supported.
        """
        columns = []
        for f in fields(cls):
            if f.type not in types:
                raise ValueError(f"Unsupported type {f.type} for field {f.name}")

            assert isinstance(f.type, type)
            sql_type = types[f.type]
            columns.append(f"{f.name} {sql_type}")

        return ",\n".join(columns)

    @classmethod
    def equals(cls, other: type["AssetSchema"]) -> bool:
        """Check if two schemas are equal.

        Args:
            other: The other schema to compare to.

        Returns:
            True if the schemas are equal, False otherwise.
        """
        if not issubclass(other, AssetSchema):
            return False

        self_fields = {f.name: f.type for f in fields(cls)}
        other_fields = {f.name: f.type for f in fields(other)}
        return self_fields == other_fields

    @classmethod
    def compare(cls, other: type["AssetSchema"]) -> tuple[bool, dict]:
        """Compare two schemas.

        Args:
            other: The other schema to compare to.

        Returns:
            A tuple containing a boolean indicating if the schemas are equal and a dictionary with the differences.
        """
        cls_fields = {f.name: f.type for f in fields(cls)}
        other_fields = {f.name: f.type for f in fields(other)}

        if cls_fields == other_fields:
            return True, {}

        diff = {
            f"missing_in_{cls.__name__}": list(set(other_fields.keys()) - set(cls_fields.keys())),
            f"missing_in_{other.__name__}": list(set(cls_fields.keys()) - set(other_fields.keys())),
            "type_mismatches": {
                field: (cls_fields[field], other_fields[field])
                for field in cls_fields.keys() & other_fields.keys()
                if cls_fields[field] != other_fields[field]
            },
        }
        return False, diff

    @classmethod
    def print_implementation(cls) -> None:
        """Print the implementation of the schema."""
        class_name = cls.__name__
        lines = [
            "import interloper as itlp",
            "",
            f"class {class_name}(itlp.AssetSchema):",
        ]
        for f in fields(cls):
            type_name = f.type.__name__  # type: ignore
            lines.append(f"    {f.name}: {type_name}")

        print("\n".join(lines))
