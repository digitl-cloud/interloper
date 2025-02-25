# import datetime as dt
# from dataclasses import fields

# from sqlalchemy import Boolean, Column, Date, DateTime, Float, Integer, MetaData, String, Table

# from interloper.core.schema import TableSchema, TTableSchema

# PYTHON_TO_SQLALCHEMY_TYPE = {
#     int: Integer,
#     float: Float,
#     str: String,
#     bool: Boolean,
#     dt.date: Date,
#     dt.datetime: DateTime,
# }


# @classmethod
# def to_sqlalchemy_table(cls: TTableSchema, table_name: str, metadata: MetaData = MetaData()) -> Table:
#     """Generates an SQLAlchemy Table object from the dataclass schema."""

#     columns = []
#     for field in fields(cls):
#         if field.type not in PYTHON_TO_SQLALCHEMY_TYPE:
#             raise ValueError(f"Unsupported type {field.type} for field {field.name}")

#         sql_type = PYTHON_TO_SQLALCHEMY_TYPE[field.type]
#         column = Column(field.name, sql_type, primary_key=field.metadata.get("primary_key", False))
#         columns.append(column)

#     return Table(table_name, metadata, *columns)


# TableSchema.to_sqlalchemy_table = to_sqlalchemy_table
