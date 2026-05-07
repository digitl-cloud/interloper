"""Tests for DataFrameNormalizer."""

import pandas as pd
import pytest
from pydantic import BaseModel

from interloper_pandas.normalizer import DataFrameNormalizer


class TestDataFrameNormalize:
    """Tests for DataFrameNormalizer.normalize()."""

    def test_dataframe_in_dataframe_out(self):
        n = DataFrameNormalizer(normalize_columns_names=False, fill_missing=False, infer=False)
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        result = n.normalize(df)
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == ["a", "b"]
        assert result["a"].tolist() == [1, 2]

    def test_columns_normalized(self):
        n = DataFrameNormalizer(fill_missing=False, infer=False)
        df = pd.DataFrame({"UserName": ["alice"], "UserAge": [30]})
        result = n.normalize(df)
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == ["user_name", "user_age"]

    def test_flatten_nested_dicts(self):
        n = DataFrameNormalizer(flatten_max_level=None, infer=False)
        df = pd.DataFrame({"user": [{"name": "alice", "age": 30}]})
        result = n.normalize(df)
        assert isinstance(result, pd.DataFrame)
        assert "user_name" in result.columns
        assert "user_age" in result.columns

    def test_list_dict_coerced_to_dataframe(self):
        n = DataFrameNormalizer(normalize_columns_names=False, fill_missing=False, infer=False)
        result = n.normalize([{"a": 1}, {"a": 2}])
        assert isinstance(result, pd.DataFrame)
        assert result["a"].tolist() == [1, 2]

    def test_single_dict_coerced(self):
        n = DataFrameNormalizer(normalize_columns_names=False, fill_missing=False, infer=False)
        result = n.normalize({"a": 1, "b": 2})
        assert isinstance(result, pd.DataFrame)
        assert result["a"].tolist() == [1]

    def test_pydantic_model_coerced(self):
        class User(BaseModel):
            name: str
            age: int

        n = DataFrameNormalizer(normalize_columns_names=False, fill_missing=False, infer=False)
        result = n.normalize(User(name="alice", age=30))
        assert isinstance(result, pd.DataFrame)
        assert result["name"].tolist() == ["alice"]

    def test_none_returns_empty_df(self):
        n = DataFrameNormalizer(infer=False)
        result = n.normalize(None)
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_empty_list_returns_empty_df(self):
        n = DataFrameNormalizer(infer=False)
        result = n.normalize([])
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_unsupported_type_raises(self):
        n = DataFrameNormalizer(infer=False)
        with pytest.raises(TypeError, match="does not support type"):
            n.normalize(42)

    def test_all_disabled(self):
        n = DataFrameNormalizer(
            normalize_columns_names=False,
            fill_missing=False,
            infer=False,
        )
        df = pd.DataFrame({"A": [1], "B": [2]})
        result = n.normalize(df)
        assert list(result.columns) == ["A", "B"]

    def test_flatten_then_normalize(self):
        n = DataFrameNormalizer(flatten_max_level=None, infer=False)
        df = pd.DataFrame({"userData": [{"firstName": "alice"}]})
        result = n.normalize(df)
        assert "user_data_first_name" in result.columns


class TestDataFrameInferSchema:
    """Tests for DataFrameNormalizer.infer_schema()."""

    def test_infers_from_dataframe(self):
        n = DataFrameNormalizer()
        df = pd.DataFrame({"name": ["alice", "bob"], "age": [30, 25]})
        schema = n.infer_schema(df)
        assert issubclass(schema, BaseModel)
        assert "name" in schema.model_fields
        assert "age" in schema.model_fields

    def test_empty_dataframe_raises(self):
        n = DataFrameNormalizer()
        df = pd.DataFrame()
        with pytest.raises(ValueError, match="Cannot infer schema from empty data"):
            n.infer_schema(df)


class TestDataFrameValidateSchema:
    """Tests for DataFrameNormalizer.validate_schema()."""

    def test_valid_data_passes(self):
        from interloper.schema import Schema

        class UserSchema(Schema):
            name: str
            age: int | None = None

        n = DataFrameNormalizer()
        df = pd.DataFrame({"name": ["alice"], "age": [30]})
        n.validate_schema(df, UserSchema)  # should not raise

    def test_invalid_data_raises(self):
        from interloper.schema import Schema

        class UserSchema(Schema):
            name: str

        n = DataFrameNormalizer()
        df = pd.DataFrame({"name": [123]})
        with pytest.raises(ValueError, match="Schema validation failed"):
            n.validate_schema(df, UserSchema)


class TestDataFrameReconcile:
    """Tests for DataFrameNormalizer.reconcile()."""

    def test_reconcile_returns_dataframe(self):
        from interloper.schema import Schema

        class UserSchema(Schema):
            name: str
            age: int | None = None

        n = DataFrameNormalizer()
        df = pd.DataFrame({"name": ["alice"], "age": [30]})
        result = n.reconcile(df, UserSchema)
        assert isinstance(result, pd.DataFrame)
        assert "name" in result.columns
        assert "age" in result.columns

    def test_reconcile_coerces_types(self):
        from interloper.schema import Schema

        class ValueSchema(Schema):
            value: int

        n = DataFrameNormalizer()
        df = pd.DataFrame({"value": ["123", "456"]})
        result = n.reconcile(df, ValueSchema)
        assert isinstance(result, pd.DataFrame)
        assert result["value"].tolist() == [123, 456]

    def test_reconcile_drops_extra_columns(self):
        from interloper.schema import Schema

        class NameSchema(Schema):
            name: str

        n = DataFrameNormalizer()
        df = pd.DataFrame({"name": ["alice"], "extra": ["drop_me"]})
        result = n.reconcile(df, NameSchema)
        assert isinstance(result, pd.DataFrame)
        assert "extra" not in result.columns
