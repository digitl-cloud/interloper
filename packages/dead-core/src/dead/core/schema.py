import datetime as dt
from abc import ABC
from dataclasses import dataclass, fields

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
    def to_dict(cls) -> dict:
        return {
            name: {"type": field.type, "description": field.metadata.get("description")}
            for name, field in cls.__dataclass_fields__.items()
        }

    @classmethod
    def to_sql(cls) -> str:
        """Generates a Standard SQL schema representation of the dataclass."""

        columns = []
        for field in fields(cls):
            if field.type not in PYTHON_TO_SQL_TYPE:
                raise ValueError(f"Unsupported type {field.type} for field {field.name}")

            sql_type = PYTHON_TO_SQL_TYPE[field.type]
            columns.append(f"{field.name} {sql_type}")

        return ",\n".join(columns)


TTableSchema = type[TableSchema]
