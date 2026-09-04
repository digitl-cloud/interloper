"""Tests for ``interloper.utils.data``."""

from collections.abc import Iterator
from typing import Any

import pytest
from pydantic import BaseModel

from interloper.errors import NormalizerError
from interloper.utils.data import coerce_to_records, is_empty


class _FrameLike:
    """Stand-in for a pandas/polars frame: boolean ``.empty``, ambiguous ``bool()``."""

    def __init__(self, empty: bool) -> None:
        self.empty = empty

    def __bool__(self) -> bool:  # pragma: no cover - must never be called by is_empty
        raise ValueError("The truth value of a frame is ambiguous")


class TestIsEmpty:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (None, True),
            ([], True),
            ({}, True),
            ("", True),
            ([{"a": 1}], False),
            ({"a": 1}, False),
            ("x", False),
            (0, False),  # not None, no len → can't confirm empty, treat as non-empty
        ],
    )
    def test_confirms_only_known_empties(self, value: Any, expected: bool):
        assert is_empty(value) is expected

    def test_defers_to_frame_empty_flag(self):
        assert is_empty(_FrameLike(empty=True)) is True
        assert is_empty(_FrameLike(empty=False)) is False

    def test_does_not_consume_generator(self):
        def gen() -> Iterator[int]:
            yield 1

        g = gen()
        assert is_empty(g) is False
        assert next(g) == 1  # generator left intact


class TestCoerceToRecords:
    """Every supported asset return shape becomes ``list[dict]``."""

    def test_none_is_no_rows(self):
        assert coerce_to_records(None) == []

    def test_a_dict_becomes_one_row(self):
        assert coerce_to_records({"a": 1}) == [{"a": 1}]

    def test_a_list_of_dicts_passes_through(self):
        rows = [{"a": 1}, {"a": 2}]

        assert coerce_to_records(rows) is rows

    def test_an_empty_list_is_no_rows(self):
        assert coerce_to_records([]) == []

    def test_a_pydantic_model_is_dumped(self):
        class Row(BaseModel):
            a: int

        assert coerce_to_records(Row(a=1)) == [{"a": 1}]

    def test_a_list_of_models_is_dumped(self):
        class Row(BaseModel):
            a: int

        assert coerce_to_records([Row(a=1), Row(a=2)]) == [{"a": 1}, {"a": 2}]

    def test_a_generator_is_consumed(self):
        assert coerce_to_records({"a": i} for i in range(2)) == [{"a": 0}, {"a": 1}]

    def test_an_unsupported_list_element_names_the_type(self):
        with pytest.raises(NormalizerError, match=r"list\[int\], expected list\[dict\]"):
            coerce_to_records([1, 2])

    def test_an_unsupported_type_is_rejected(self):
        with pytest.raises(NormalizerError):
            coerce_to_records(object())
