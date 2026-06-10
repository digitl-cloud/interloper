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


class TestNullSafety:
    """Missing values convert to None, not NaN."""

    def test_nan_and_nat_become_none(self):
        import numpy as np

        adapter = DataFrameAdapter()
        df = pd.DataFrame(
            {
                "f": [1.5, np.nan],
                "t": [pd.Timestamp("2024-01-01"), pd.NaT],
                "s": ["x", None],
            }
        )
        rows = adapter.to_rows(df)
        assert rows[1]["f"] is None
        assert rows[1]["t"] is None
        assert rows[1]["s"] is None

    def test_nullable_extension_dtypes_become_none(self):
        adapter = DataFrameAdapter()
        df = pd.DataFrame({"i": pd.array([1, None], dtype="Int64")})
        rows = adapter.to_rows(df)
        assert rows == [{"i": 1}, {"i": None}]

    def test_can_handle(self):
        adapter = DataFrameAdapter()
        assert adapter.can_handle(pd.DataFrame())
        assert not adapter.can_handle([{"a": 1}])
