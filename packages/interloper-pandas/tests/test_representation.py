"""Tests for the DataFrame representation."""

import numpy as np
import pandas as pd
from interloper.conformer import Conformer

from interloper_pandas.representation import DataFrameRepresentation


class TestDataFrameRepresentation:
    """Generic table views on pandas DataFrames."""

    def test_matches_dataframes_only(self):
        rep = DataFrameRepresentation()
        assert rep.matches(pd.DataFrame())
        assert not rep.matches([{"a": 1}])

    def test_to_records_maps_missing_values_to_none(self):
        rep = DataFrameRepresentation()
        df = pd.DataFrame(
            {
                "f": [1.5, np.nan],
                "t": [pd.Timestamp("2024-01-01"), pd.NaT],
                "s": ["x", None],
                "i": pd.array([1, None], dtype="Int64"),
            }
        )
        rows = rep.to_records(df)
        assert rows[1] == {"f": None, "t": None, "s": None, "i": None}
        assert rows[0]["i"] == 1

    def test_from_records_materializes_dataframe(self):
        df = DataFrameRepresentation().from_records([{"a": 1}, {"a": 2}])
        assert isinstance(df, pd.DataFrame)
        assert df["a"].tolist() == [1, 2]

    def test_columns(self):
        df = pd.DataFrame({"a": [1], "b": [2]})
        assert DataFrameRepresentation().columns(df) == ["a", "b"]

    def test_filter_eq_compares_as_strings(self):
        df = pd.DataFrame({"d": ["2024-01-01", "2024-01-02"], "v": [1, 2]})
        out = DataFrameRepresentation().filter_eq(df, "d", "2024-01-02")
        assert out["v"].tolist() == [2]

    def test_conformer_is_dataframe_conformer(self):
        from interloper_pandas.conformer import DataFrameConformer

        conformer = DataFrameRepresentation().conformer
        assert isinstance(conformer, Conformer)
        assert isinstance(conformer, DataFrameConformer)
