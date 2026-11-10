"""Tests for ``interloper.asset.output``."""

from typing import Any

import pytest
from pydantic import BaseModel

import interloper as il
from interloper.asset.output import Output
from interloper.errors import AssetError
from interloper.normalizer import MaterializationStrategy


class Row(il.Schema):
    user_id: int
    name: str


@il.asset
def untyped() -> None:
    """An asset declaring no schema."""


@il.asset(schema=Row)
def typed() -> None:
    """An asset declaring a schema."""


@il.asset(schema=Row, materialization_strategy=MaterializationStrategy.STRICT)
def strict() -> None:
    """An asset declaring a schema it enforces strictly."""


class TestRows:
    def test_none_is_no_rows(self):
        assert Output.from_result(None, untyped()).data == []

    def test_a_generator_is_consumed(self):
        assert Output.from_result(({"a": i} for i in range(2)), untyped()).data == [{"a": 0}, {"a": 1}]

    def test_a_model_is_dumped(self):
        class Item(BaseModel):
            a: int

        assert Output.from_result([Item(a=1)], untyped()).data == [{"a": 1}]

    def test_a_dict_is_one_row(self):
        assert Output.from_result({"a": 1}, untyped()).data == [{"a": 1}]


class TestConform:
    def test_non_tabular_data_without_a_schema_passes_through(self):
        payload = object()
        output = Output.from_result(payload, untyped())
        assert output.data is payload
        assert output.schema is None

    def test_non_tabular_data_with_a_schema_is_an_actionable_error(self):
        with pytest.raises(AssetError, match="declares a schema but returned data that cannot be checked"):
            Output.from_result(object(), typed())

    def test_strict_returns_the_validated_data_and_its_schema(self):
        rows = [{"user_id": 1, "name": "x"}]
        output = Output.from_result(rows, strict())
        assert output.data == rows
        assert output.schema is Row

    def test_strict_without_a_schema_is_refused(self):
        @il.asset(materialization_strategy=MaterializationStrategy.STRICT)
        def loose() -> Any:
            return None

        with pytest.raises(AssetError, match="requires a schema"):
            Output.from_result([{"a": 1}], loose())

    def test_without_a_schema_the_schema_is_inferred(self):
        output = Output.from_result([{"user_id": 1, "name": "a"}], untyped())
        assert output.schema is not None
        assert [spec.name for spec in output.schema.field_specs()] == ["user_id", "name"]

    def test_failed_inference_leaves_no_schema(self, monkeypatch: pytest.MonkeyPatch):
        from interloper.representation import RowsRepresentation

        monkeypatch.setattr(
            RowsRepresentation, "infer", lambda self, data: (_ for _ in ()).throw(RuntimeError("cannot infer"))
        )
        output = Output.from_result([{"a": 1}], untyped())
        assert output.data == [{"a": 1}]
        assert output.schema is None

    def test_the_normalizer_receives_the_rows(self):
        received: list[Any] = []

        class Recording(il.Normalizer):
            def normalize(self, data: Any) -> list[dict[str, Any]]:
                received.append(data)
                return data

        Output.from_result({"a": 1}, untyped()(normalizer=Recording()))
        assert received == [[{"a": 1}]]
