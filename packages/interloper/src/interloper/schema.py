import datetime as dt
from abc import ABC
from dataclasses import dataclass, fields, make_dataclass

PYTHON_TO_SQL_TYPE = {
    int: "INTEGER",
    float: "FLOAT",
    str: "VARCHAR",
    bool: "BOOLEAN",
    dt.date: "DATE",
    dt.datetime: "TIMESTAMP",
}


@dataclass
class TableSchema(ABC):
    @classmethod
    def from_dict(cls, schema: dict[str, type], name: str | None = None) -> type["TableSchema"]:
        fields = [(name, field_type) for name, field_type in schema.items()]
        return make_dataclass(name or cls.__name__, fields, bases=(cls,))

    @classmethod
    def to_dict(cls) -> dict:
        return {
            f.name: {
                "type": f.type,
                "description": f.metadata.get("description"),
            }
            for f in fields(cls)
        }

    @classmethod
    def to_sql(cls, types: dict[type, str] = PYTHON_TO_SQL_TYPE) -> str:
        columns = []
        for f in fields(cls):
            if f.type not in types:
                raise ValueError(f"Unsupported type {f.type} for field {f.name}")

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
