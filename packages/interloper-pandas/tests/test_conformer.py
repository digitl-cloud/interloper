"""Tests for the DataFrame conformer."""

import datetime
import json

import pandas as pd
import pytest
from interloper.errors import SchemaError
from interloper.representation import Representation
from interloper.schema import Schema
from pydantic import Field

from interloper_pandas.conformer import DataFrameConformer


class UserSchema(Schema):
    user_id: int | None = Field(...)
    name: str | None = Field(...)


class TypedSchema(Schema):
    id: int | None = Field(...)
    cost: float | None = Field(...)
    day: datetime.date | None = Field(...)
    name: str | None = Field(...)


class TestResolution:
    """The DataFrame representation carries the pandas conformer."""

    def test_dataframe_resolves_to_dataframe_conformer(self):
        assert isinstance(Representation.of(pd.DataFrame()).conformer, DataFrameConformer)


class TestDataFrameConformerValidate:
    """Null-safe validation on DataFrames."""

    def test_valid_data_passes(self):
        DataFrameConformer().validate(pd.DataFrame({"user_id": [1], "name": ["a"]}), UserSchema)

    def test_invalid_data_raises(self):
        with pytest.raises(SchemaError, match="Schema validation failed"):
            DataFrameConformer().validate(pd.DataFrame({"user_id": [1], "name": [123]}), UserSchema)

    def test_nan_in_nullable_int_column_passes(self):
        import numpy as np

        class S(Schema):
            id: int | None = Field(...)

        # NaN forces float64 dtype; the records view must yield None, not nan
        DataFrameConformer().validate(pd.DataFrame({"id": [1.0, np.nan]}), S)


class TestDataFrameConformerReconcile:
    """Spec-driven vectorized reconcile."""

    def test_casts_to_nullable_dtypes(self):
        import numpy as np

        df = pd.DataFrame(
            {"id": [1.0, np.nan], "cost": ["1.5", None], "day": ["2024-01-01", None], "name": ["x", None]}
        )
        out = DataFrameConformer().reconcile(df, TypedSchema)
        assert str(out["id"].dtype) == "Int64"
        assert out["id"].tolist()[0] == 1
        assert out["cost"].tolist()[0] == 1.5
        assert out["day"].tolist()[0] == datetime.date(2024, 1, 1)
        assert out["name"].tolist()[0] == "x"

    def test_missing_nullable_column_added_and_extra_dropped(self):
        out = DataFrameConformer().reconcile(pd.DataFrame({"id": [1], "extra": ["drop"]}), TypedSchema)
        assert list(out.columns) == ["id", "cost", "day", "name"]
        assert out["cost"].isna().all()

    def test_missing_required_column_raises(self):

        class Req(Schema):
            must: int

        with pytest.raises(SchemaError, match="required column 'must' is missing"):
            DataFrameConformer().reconcile(pd.DataFrame({"other": [1]}), Req)

    def test_uncastable_value_raises(self):
        df = pd.DataFrame({"id": ["abc"], "cost": [1.0], "day": ["2024-01-01"], "name": ["x"]})
        with pytest.raises(SchemaError, match="cannot cast"):
            DataFrameConformer().reconcile(df, TypedSchema)


class JsonSchema(Schema):
    """A scalar ``str`` field that receives nested API values."""

    id: str | None = Field(...)
    tracking_specs: str | None = Field(...)


class TestDataFrameConformerJsonEncoding:
    """``str``-typed fields receiving list/dict values are JSON-encoded."""

    def test_validate_accepts_list_for_str_field(self):
        df = pd.DataFrame({"id": ["1"], "tracking_specs": [[{"action.type": ["x"]}]]})
        DataFrameConformer().validate(df, JsonSchema)

    def test_validate_accepts_dict_for_str_field(self):
        df = pd.DataFrame({"id": ["1"], "tracking_specs": [{"a": 1}]})
        DataFrameConformer().validate(df, JsonSchema)

    def test_reconcile_serializes_to_valid_json_not_repr(self):
        df = pd.DataFrame({"id": ["1"], "tracking_specs": [[{"a": 1}]]})
        out = DataFrameConformer().reconcile(df, JsonSchema)
        assert out["tracking_specs"].dtype == "string"
        encoded = out["tracking_specs"].iloc[0]
        # Valid JSON (double-quoted) round-trips; a Python repr would not.
        assert json.loads(encoded) == [{"a": 1}]
        assert "'" not in encoded

    def test_scalar_strings_and_nulls_pass_through(self):
        df = pd.DataFrame({"id": ["1", "2"], "tracking_specs": ["plain", None]})
        DataFrameConformer().validate(df, JsonSchema)
        out = DataFrameConformer().reconcile(df, JsonSchema)
        assert out["tracking_specs"].iloc[0] == "plain"
        assert pd.isna(out["tracking_specs"].iloc[1])


class TestDataFrameConformerInfer:
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
        specs = {s.name: s for s in DataFrameConformer().infer(df).field_specs()}
        assert specs["i"].type is int
        assert specs["f"].type is float
        assert specs["b"].type is bool
        assert specs["t"].type is datetime.datetime
        assert specs["s"].type is str
        assert all(s.nullable for s in specs.values())

    def test_empty_dataframe_raises(self):
        with pytest.raises(SchemaError, match="Cannot infer schema from a DataFrame with no columns"):
            DataFrameConformer().infer(pd.DataFrame())
