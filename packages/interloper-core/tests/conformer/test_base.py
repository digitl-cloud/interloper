"""Tests for ``interloper.conformer.base``."""

import pytest
from pydantic import BaseModel, Field

from interloper.conformer import RowsConformer
from interloper.errors import NormalizerError, SchemaError
from interloper.schema import Schema


class UserSchema(Schema):
    user_id: int | None = Field(...)
    name: str | None = Field(...)


class TestRowsConformer:
    """Row-wise schema operations and canonicalization."""

    def test_prepare_passes_lists_through(self):
        rows = [{"a": 1}]
        assert RowsConformer().prepare(rows) is rows

    def test_prepare_coerces_dict(self):
        assert RowsConformer().prepare({"a": 1}) == [{"a": 1}]

    def test_prepare_coerces_model(self):
        class Row(BaseModel):
            a: int

        assert RowsConformer().prepare(Row(a=1)) == [{"a": 1}]

    def test_prepare_consumes_generator(self):
        def gen():
            yield {"a": 1}
            yield {"a": 2}

        assert RowsConformer().prepare(gen()) == [{"a": 1}, {"a": 2}]

    def test_prepare_rejects_non_tabular(self):
        with pytest.raises(NormalizerError, match="does not support type"):
            RowsConformer().prepare(42)

    def test_validate_passes(self):
        RowsConformer().validate([{"user_id": 1, "name": "a"}], UserSchema)

    def test_validate_fails_on_missing_required(self):
        with pytest.raises(SchemaError, match="Field required"):
            RowsConformer().validate([{"user_id": 1}], UserSchema)

    def test_reconcile_coerces_and_aligns(self):
        rows = RowsConformer().reconcile([{"user_id": "1", "name": "a", "extra": True}], UserSchema)
        assert rows == [{"user_id": 1, "name": "a"}]

    def test_infer(self):
        inferred = RowsConformer().infer([{"a": 1, "b": "x"}])
        specs = {s.name: s for s in inferred.field_specs()}
        assert specs["a"].type is int
        assert specs["b"].type is str
