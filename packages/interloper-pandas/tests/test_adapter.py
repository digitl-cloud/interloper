"""Tests for DataFrameAdapter."""

import pandas as pd
import pytest

from interloper_pandas.adapter import DataFrameAdapter


class TestDataFrameAdapterToRows:
    """Tests for DataFrameAdapter.to_rows()."""

    def test_dataframe_to_rows(self):
        adapter = DataFrameAdapter()
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        rows = adapter.to_rows(df)
        assert rows == [{"a": 1, "b": 3}, {"a": 2, "b": 4}]

    def test_empty_dataframe(self):
        adapter = DataFrameAdapter()
        df = pd.DataFrame()
        rows = adapter.to_rows(df)
        assert rows == []

    def test_rejects_non_dataframe(self):
        adapter = DataFrameAdapter()
        with pytest.raises(TypeError, match="DataFrameAdapter expects a pandas DataFrame"):
            adapter.to_rows([{"a": 1}])  # ty: ignore[invalid-argument-type]


class TestDataFrameAdapterFromRows:
    """Tests for DataFrameAdapter.from_rows()."""

    def test_rows_to_dataframe(self):
        adapter = DataFrameAdapter()
        rows = [{"a": 1, "b": 3}, {"a": 2, "b": 4}]
        df = adapter.from_rows(rows)
        assert isinstance(df, pd.DataFrame)
        assert list(df.columns) == ["a", "b"]
        assert df["a"].tolist() == [1, 2]

    def test_empty_rows(self):
        adapter = DataFrameAdapter()
        df = adapter.from_rows([])
        assert isinstance(df, pd.DataFrame)
        assert df.empty
