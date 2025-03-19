from collections.abc import Generator
from tkinter import N
from typing import Any

import pytest

from interloper.errors import AssetNormalizationError
from interloper.normalizer import JSONNormalizer, Normalizer


@pytest.fixture
def normalizer():
    class CustomNormalizer(Normalizer):
        def normalize(self, data: Any) -> Any: ...

        def infer_schema(self, data: Any) -> Any: ...

    return CustomNormalizer()


class TestNormalizer:
    def test_column_name_basic(self, normalizer: Normalizer):
        assert normalizer.column_name("Some Column") == "some_column"

    def test_column_name_special_chars(self, normalizer: Normalizer):
        assert normalizer.column_name("Some@Col#umn!") == "some_col_umn"

    def test_column_name_trailing_underscores(self, normalizer: Normalizer):
        assert normalizer.column_name("__Some__Column__") == "some_column"

    def test_column_name_percentage(self, normalizer: Normalizer):
        assert normalizer.column_name("Win %") == "win_pct"

    def test_column_name_multiple_special_chars(self, normalizer: Normalizer):
        assert normalizer.column_name("Data% Quality@ Score!") == "data_pct_quality_score"


class TestJSONNormalizer:
    def test_validate_list(self):
        normalizer = JSONNormalizer()
        data = [{"a": 1}, {"b": 2}]
        assert normalizer._validate(data) == data

    def test_validate_dict(self):
        normalizer = JSONNormalizer()
        data = {"a": 1, "b": 2}
        assert normalizer._validate(data) == [{"a": 1, "b": 2}]

    def test_validate_generator(self):
        normalizer = JSONNormalizer()

        def gen():
            yield {"a": 1}
            yield {"b": 2}

        assert normalizer._validate(gen()) == [{"a": 1}, {"b": 2}]

    def test_validate_invalid(self):
        normalizer = JSONNormalizer()
        with pytest.raises(AssetNormalizationError, match="Data is not JSON-serializable"):
            normalizer._validate(set([1, 2, 3]))

    def test_flatten_simple_dict(self):
        normalizer = JSONNormalizer()
        data = {"a": 1, "b": 2}
        expected = {"a": 1, "b": 2}
        assert normalizer._flatten(data) == expected

    def test_flatten_nested_dict(self):
        normalizer = JSONNormalizer(separator="_")
        data = {"a": {"b": {"c": 1}}}
        expected = {"a": {"b": {"c": 1}}}
        assert normalizer._flatten(data) == expected

    def test_flatten_nested_dict_max_level_1(self):
        normalizer = JSONNormalizer(separator="_", max_level=1)
        data = {"a": {"b": {"c": 1}}}
        expected = {"a_b": {"c": 1}}  # Since max_level=1 by default
        assert normalizer._flatten(data) == expected

    def test_flatten_nested_dict_max_level_2(self):
        normalizer = JSONNormalizer(separator="_", max_level=2)
        data = {"a": {"b": {"c": 1}}}
        expected = {"a_b_c": 1}  # Max level increased, so it fully flattens
        assert normalizer._flatten(data) == expected

    def test_flatten_list_of_dicts(self):
        normalizer = JSONNormalizer()
        data = {"a": 1, "b": {"c": 2}}
        expected = [{"a": 1, "b": {"c": 2}}]
        assert normalizer.normalize(data) == expected

    def test_normalize_with_generator(self):
        normalizer = JSONNormalizer()

        def gen() -> Generator[Any, None, None]:
            yield {"a": 1}
            yield {"b": 2}

        expected = [{"a": 1, "b": None}, {"a": None, "b": 2}]
        assert normalizer.normalize(gen()) == expected

    def test_normalize_add_missing_keys(self):
        normalizer = JSONNormalizer(add_missing_columns=True)
        data = [{"a": 1}, {"b": 2}]
        expected = [{"a": 1, "b": None}, {"a": None, "b": 2}]
        assert normalizer.normalize(data) == expected

    def test_normalize_does_not_add_missing_keys(self):
        normalizer = JSONNormalizer(add_missing_columns=False)
        data = [{"a": 1}, {"b": 2}]
        expected = [{"a": 1}, {"b": 2}]
        assert normalizer.normalize(data) == expected

    def test_normalize_renames_columns(self):
        normalizer = JSONNormalizer(rename_columns=True)
        data = [{"Some Column": 1}, {"Another Column": 2}]
        expected = [
            {"some_column": 1, "another_column": None},
            {"some_column": None, "another_column": 2},
        ]
        assert normalizer.normalize(data) == expected

    def test_normalize_does_not_rename_columns(self):
        normalizer = JSONNormalizer(rename_columns=False)
        data = [{"Some Column": 1}, {"Another Column": 2}]
        expected = [
            {"Some Column": 1, "Another Column": None},
            {"Some Column": None, "Another Column": 2},
        ]
        assert normalizer.normalize(data) == expected
