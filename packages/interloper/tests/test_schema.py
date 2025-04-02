from dataclasses import field

import pytest

from interloper.schema import AssetSchema


class TestAssetSchema:
    def test_schema(self):
        class Schema(AssetSchema):
            a: int
            b: str

        assert Schema.to_sql() == "a INTEGER,\nb VARCHAR"
        assert Schema.to_dict() == {
            "a": {"type": int, "description": None},
            "b": {"type": str, "description": None},
        }
        assert Schema.equals(Schema)
        assert Schema.compare(Schema) == (True, {})

    def test_schema_with_description(self):
        class Schema(AssetSchema):
            a: int = field(metadata={"description": "a description"})
            b: str = field(metadata={"description": "b description"})

        assert Schema.to_sql() == "a INTEGER,\nb VARCHAR"
        assert Schema.to_dict() == {
            "a": {"type": int, "description": "a description"},
            "b": {"type": str, "description": "b description"},
        }

    def test_schema_with_invalid_type(self):
        class Schema(AssetSchema):
            a: list[int]

        with pytest.raises(ValueError):
            Schema.to_sql()

    def test_schema_to_tuple(self):
        class Schema(AssetSchema):
            a: int
            b: str

        assert Schema.to_tuple() == (("a", int), ("b", str))
        assert Schema.to_tuple("sql") == (("a", "INTEGER"), ("b", "VARCHAR"))

    def test_schema_to_tuple_invalid_format(self):
        class Schema(AssetSchema):
            a: int
            b: str

        with pytest.raises(ValueError):
            Schema.to_tuple("invalid")
