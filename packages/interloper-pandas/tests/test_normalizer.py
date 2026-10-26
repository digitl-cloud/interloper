"""Tests for DataFrameNormalizer."""

import pandas as pd
import pytest
from interloper.errors import NormalizerError

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

    def test_empty_list_returns_empty_df(self):
        n = DataFrameNormalizer()
        result = n.normalize([])
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_anything_but_rows_or_a_frame_is_an_explicit_error(self):
        n = DataFrameNormalizer()
        with pytest.raises(NormalizerError, match=r"expects list\[dict\] or a DataFrame.*dict"):
            n.normalize({"a": 1})
        with pytest.raises(NormalizerError):
            n.normalize(None)

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

    def test_epoch_columns_become_utc_instants_in_the_declared_unit(self):
        n = DataFrameNormalizer(epoch_columns=["created_at", "starts_at"], epoch_unit="ms")
        result = n.normalize([{"createdAt": 1785179053888, "startsAt": 1785179053888}, {"createdAt": None}])
        assert result["created_at"].tolist()[0] == pd.Timestamp("2026-07-27T19:04:13.888", tz="UTC")
        assert pd.isna(result["created_at"].tolist()[1])
        assert result["starts_at"].tolist()[0] == pd.Timestamp("2026-07-27T19:04:13.888", tz="UTC")

    def test_epoch_seconds_are_read_as_seconds(self):
        n = DataFrameNormalizer(epoch_columns=["created_at"], epoch_unit="s")
        result = n.normalize([{"created_at": 1785179053}])
        assert result["created_at"].tolist() == [pd.Timestamp("2026-07-27T19:04:13", tz="UTC")]

    def test_an_epoch_column_absent_from_the_frame_is_skipped(self):
        n = DataFrameNormalizer(epoch_columns=["created_at"], epoch_unit="ms")
        assert list(n.normalize([{"other": 1}]).columns) == ["other"]

    def test_epoch_columns_need_a_declared_unit(self):
        with pytest.raises(ValueError, match="epoch_unit"):
            DataFrameNormalizer(epoch_columns=["created_at"])

    def test_the_epoch_declaration_survives_the_spec_roundtrip(self):
        n = DataFrameNormalizer(flatten_max_level=1, epoch_columns=["created_at"], epoch_unit="ms")
        restored = DataFrameNormalizer.from_spec(n.to_spec())
        assert isinstance(restored, DataFrameNormalizer)
        assert (restored.epoch_columns, restored.epoch_unit) == (["created_at"], "ms")

    def test_replace_empty_dicts(self):
        n = DataFrameNormalizer(
            normalize_columns_names=False, fill_missing=False, flatten_max_level=0, replace_empty_dicts=True
        )
        result = n.normalize(pd.DataFrame({"a": [{}, {"k": 1}]}))
        assert result["a"].iloc[0] is None
