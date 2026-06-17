"""Tests for DataFrameNormalizer."""

import pandas as pd
import pytest
from pydantic import BaseModel

from interloper_pandas.normalizer import DataFrameNormalizer


class TestDataFrameNormalize:
    """Tests for DataFrameNormalizer.normalize()."""

    def test_dataframe_in_dataframe_out(self):
        n = DataFrameNormalizer(normalize_columns_names=False, fill_missing=False)
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        result = n.normalize(df)
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == ["a", "b"]
        assert result["a"].tolist() == [1, 2]

    def test_columns_normalized(self):
        n = DataFrameNormalizer(fill_missing=False)
        df = pd.DataFrame({"UserName": ["alice"], "UserAge": [30]})
        result = n.normalize(df)
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == ["user_name", "user_age"]

    def test_flatten_nested_dicts(self):
        n = DataFrameNormalizer(flatten_max_level=None)
        df = pd.DataFrame({"user": [{"name": "alice", "age": 30}]})
        result = n.normalize(df)
        assert isinstance(result, pd.DataFrame)
        assert "user_name" in result.columns
        assert "user_age" in result.columns

    def test_list_dict_coerced_to_dataframe(self):
        n = DataFrameNormalizer(normalize_columns_names=False, fill_missing=False)
        result = n.normalize([{"a": 1}, {"a": 2}])
        assert isinstance(result, pd.DataFrame)
        assert result["a"].tolist() == [1, 2]

    def test_single_dict_coerced(self):
        n = DataFrameNormalizer(normalize_columns_names=False, fill_missing=False)
        result = n.normalize({"a": 1, "b": 2})
        assert isinstance(result, pd.DataFrame)
        assert result["a"].tolist() == [1]

    def test_pydantic_model_coerced(self):
        class User(BaseModel):
            name: str
            age: int

        n = DataFrameNormalizer(normalize_columns_names=False, fill_missing=False)
        result = n.normalize(User(name="alice", age=30))
        assert isinstance(result, pd.DataFrame)
        assert result["name"].tolist() == ["alice"]

    def test_none_returns_empty_df(self):
        n = DataFrameNormalizer()
        result = n.normalize(None)
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_empty_list_returns_empty_df(self):
        n = DataFrameNormalizer()
        result = n.normalize([])
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_unsupported_type_raises(self):
        n = DataFrameNormalizer()
        with pytest.raises(TypeError, match="does not support type"):
            n.normalize(42)

    def test_all_disabled(self):
        n = DataFrameNormalizer(
            normalize_columns_names=False,
            fill_missing=False,
        )
        df = pd.DataFrame({"A": [1], "B": [2]})
        result = n.normalize(df)
        assert list(result.columns) == ["A", "B"]

    def test_flatten_then_normalize(self):
        n = DataFrameNormalizer(flatten_max_level=None)
        df = pd.DataFrame({"userData": [{"firstName": "alice"}]})
        result = n.normalize(df)
        assert "user_data_first_name" in result.columns

    def test_replace_empty_strings(self):
        n = DataFrameNormalizer(normalize_columns_names=False, fill_missing=False, replace_empty_strings=True)
        result = n.normalize(pd.DataFrame({"a": ["", "x"]}))
        assert pd.isna(result["a"].iloc[0])
        assert result["a"].iloc[1] == "x"

    def test_replace_empty_dicts(self):
        n = DataFrameNormalizer(
            normalize_columns_names=False, fill_missing=False, flatten_max_level=0, replace_empty_dicts=True
        )
        result = n.normalize(pd.DataFrame({"a": [{}, {"k": 1}]}))
        assert result["a"].iloc[0] is None
