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


class TableSchemaMeta(ABCMeta):
    def __new__(mcs, name: str, bases: tuple[type, ...], namespace: dict[str, Any]) -> type:
        cls = super().__new__(mcs, name, bases, namespace)

        # Ignore the base class
        if name == "TableSchema":
            return cls

        # Forces subclasses to be dataclasses
        return dataclass(cls)


@dataclass
class TableSchema(metaclass=TableSchemaMeta):
    @classmethod
    def from_dict(cls, schema: dict[str, type], name: str | None = None) -> type["TableSchema"]:
        fields = [(name, field_type) for name, field_type in schema.items()]
        return make_dataclass(name or cls.__name__, fields, bases=(cls,))

    @classmethod
    def to_dict(cls) -> dict[str, dict[str, Any]]:
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
        if format not in ("python", "sql"):
            raise ValueError(f"Invalid format {format}, must be one of: python, sql")

        return tuple(
            (f.name, f.type) if format == "python" else (f.name, types[f.type])  # type: ignore
            for f in fields(cls)
        )

    @classmethod
    def to_sql(cls, types: dict[type, str] = PYTHON_TO_SQL_TYPE) -> str:
        columns = []
        for f in fields(cls):
            if f.type not in types:
                raise ValueError(f"Unsupported type {f.type} for field {f.name}")

            assert isinstance(f.type, type)
            sql_type = types[f.type]
            columns.append(f"{f.name} {sql_type}")

        return ",\n".join(columns)

    @classmethod
    def equals(cls, other: type["TableSchema"]) -> bool:
        if not issubclass(other, TableSchema):
            return False

        self_fields = {f.name: f.type for f in fields(cls)}
        other_fields = {f.name: f.type for f in fields(other)}
        return self_fields == other_fields

    @classmethod
    def compare(cls, other: type["TableSchema"]) -> tuple[bool, dict]:
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
        class_name = cls.__name__
        lines = [
            "from dataclasses import dataclass",
            "import interloper as itlp",
            "",
            "@dataclass",
            f"class {class_name}(TableSchema):",
        ]
        for f in fields(cls):
            type_name = f.type.__name__  # type: ignore
            lines.append(f"    {f.name}: {type_name}")
