"""Tests for ``interloper.utils.data``."""

from collections.abc import Iterator
from typing import Any

import pytest

from interloper.utils.data import is_empty


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
