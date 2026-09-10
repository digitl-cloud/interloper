"""Tests for the DataFrame representation."""

import datetime
import json

import numpy as np
import pandas as pd
import pytest
from interloper.errors import SchemaError
from interloper.representation import Representation
from interloper.schema import Schema
from pydantic import Field

from interloper_pandas.representation import DataFrameRepresentation


class UserSchema(Schema):
    user_id: int | None = Field(...)
    name: str | None = Field(...)


class TypedSchema(Schema):
    id: int | None = Field(...)
    cost: float | None = Field(...)
    day: datetime.date | None = Field(...)
    name: str | None = Field(...)


class TestResolution:
    """The DataFrame representation claims DataFrames."""

    def test_dataframe_resolves_to_the_dataframe_representation(self):
        assert isinstance(Representation.of(pd.DataFrame()).representation, DataFrameRepresentation)


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

    def test_filter_range_is_half_open(self):
        import datetime as dt

        df = pd.DataFrame({"d": ["2024-01-31", "2024-02-01", "2024-02-15", "2024-03-01"], "v": [0, 1, 2, 3]})
        out = DataFrameRepresentation().filter_range(df, "d", dt.date(2024, 2, 1), dt.date(2024, 3, 1))
        assert out["v"].tolist() == [1, 2]

    def test_filter_eq_compares_as_strings(self):
        df = pd.DataFrame({"d": ["2024-01-01", "2024-01-02"], "v": [1, 2]})
        out = DataFrameRepresentation().filter_eq(df, "d", "2024-01-02")
        assert out["v"].tolist() == [2]


class TestDataFrameValidate:
    """Null-safe validation on DataFrames."""

    def test_valid_data_passes(self):
        DataFrameRepresentation().validate(pd.DataFrame({"user_id": [1], "name": ["a"]}), UserSchema)

    def test_invalid_data_raises(self):
        with pytest.raises(SchemaError, match="Schema validation failed"):
            DataFrameRepresentation().validate(pd.DataFrame({"user_id": [1], "name": [123]}), UserSchema)

    def test_nan_in_nullable_int_column_passes(self):
        import numpy as np

        class S(Schema):
            id: int | None = Field(...)

        # NaN forces float64 dtype; the records view must yield None, not nan
        DataFrameRepresentation().validate(pd.DataFrame({"id": [1.0, np.nan]}), S)


class TestDataFrameReconcile:
    """Spec-driven vectorized reconcile."""

    def test_casts_to_nullable_dtypes(self):
        import numpy as np

        df = pd.DataFrame(
            {"id": [1.0, np.nan], "cost": ["1.5", None], "day": ["2024-01-01", None], "name": ["x", None]}
        )
        out = DataFrameRepresentation().reconcile(df, TypedSchema)
        assert str(out["id"].dtype) == "Int64"
        assert out["id"].tolist()[0] == 1
        assert out["cost"].tolist()[0] == 1.5
        assert out["day"].tolist()[0] == datetime.date(2024, 1, 1)
        assert out["name"].tolist()[0] == "x"

    def test_missing_nullable_column_added_and_extra_dropped(self):
        out = DataFrameRepresentation().reconcile(pd.DataFrame({"id": [1], "extra": ["drop"]}), TypedSchema)
        assert list(out.columns) == ["id", "cost", "day", "name"]
        assert out["cost"].isna().all()

    def test_missing_required_column_raises(self):

        class Req(Schema):
            must: int

        with pytest.raises(SchemaError, match="required column 'must' is missing"):
            DataFrameRepresentation().reconcile(pd.DataFrame({"other": [1]}), Req)

    def test_uncastable_value_raises(self):
        df = pd.DataFrame({"id": ["abc"], "cost": [1.0], "day": ["2024-01-01"], "name": ["x"]})
        with pytest.raises(SchemaError, match="cannot cast"):
            DataFrameRepresentation().reconcile(df, TypedSchema)

    def test_int_values_cast_to_string_field(self):
        # Vendor APIs and pd.read_csv routinely yield numeric ids for str-typed
        # fields; reconcile must cast them rather than reject the rows.
        out = DataFrameRepresentation().reconcile(pd.DataFrame({"name": [143007073]}), TypedSchema)
        assert out["name"].tolist() == ["143007073"]

    def test_non_nullable_column_with_nulls_raises(self):

        class Req(Schema):
            must: int

        with pytest.raises(SchemaError, match="non-nullable column 'must' contains null"):
            DataFrameRepresentation().reconcile(pd.DataFrame({"must": [1, None]}), Req)

    def test_dropped_columns_logged(self, caplog):
        with caplog.at_level("WARNING", logger="interloper_pandas.conformer"):
            DataFrameRepresentation().reconcile(pd.DataFrame({"id": [1], "extra": ["drop"]}), TypedSchema)
        assert "dropped columns" in caplog.text
        assert "extra" in caplog.text


class JsonSchema(Schema):
    """A scalar ``str`` field that receives nested API values."""

    id: str | None = Field(...)
    tracking_specs: str | None = Field(...)


class TestDataFrameJsonEncoding:
    """``str``-typed fields receiving list/dict values are JSON-encoded."""

    def test_validate_accepts_list_for_str_field(self):
        df = pd.DataFrame({"id": ["1"], "tracking_specs": [[{"action.type": ["x"]}]]})
        DataFrameRepresentation().validate(df, JsonSchema)

    def test_validate_accepts_dict_for_str_field(self):
        df = pd.DataFrame({"id": ["1"], "tracking_specs": [{"a": 1}]})
        DataFrameRepresentation().validate(df, JsonSchema)

    def test_reconcile_serializes_to_valid_json_not_repr(self):
        df = pd.DataFrame({"id": ["1"], "tracking_specs": [[{"a": 1}]]})
        out = DataFrameRepresentation().reconcile(df, JsonSchema)
        assert out["tracking_specs"].dtype == "string"
        encoded = out["tracking_specs"].iloc[0]
        # Valid JSON (double-quoted) round-trips; a Python repr would not.
        assert json.loads(encoded) == [{"a": 1}]
        assert "'" not in encoded

    def test_scalar_strings_and_nulls_pass_through(self):
        df = pd.DataFrame({"id": ["1", "2"], "tracking_specs": ["plain", None]})
        DataFrameRepresentation().validate(df, JsonSchema)
        out = DataFrameRepresentation().reconcile(df, JsonSchema)
        assert out["tracking_specs"].iloc[0] == "plain"
        assert pd.isna(out["tracking_specs"].iloc[1])


class TestDataFrameInfer:
    """Dtype-based inference, no row materialization."""

    def test_dtype_mapping(self):
        import numpy as np

        df = pd.DataFrame(
            {
                "i": [1, 2],
                "f": [1.0, np.nan],
                "b": [True, False],
                "t": pd.to_datetime(["2024-01-01", "2024-01-02"]),
                "s": ["x", "y"],
            }
        )
        specs = {s.name: s for s in DataFrameRepresentation().infer(df).field_specs()}
        assert specs["i"].type is int
        assert specs["f"].type is float
        assert specs["b"].type is bool
        assert specs["t"].type is datetime.datetime
        assert specs["s"].type is str
        assert all(s.nullable for s in specs.values())

    def test_empty_dataframe_raises(self):
        with pytest.raises(SchemaError, match="Cannot infer schema from a DataFrame with no columns"):
            DataFrameRepresentation().infer(pd.DataFrame())
