"""Tests for ``interloper.representation.base``."""

import pytest

from interloper.errors import NormalizerError
from interloper.representation import REPRESENTATIONS, Representation, RowsRepresentation


class TestRegistry:
    """Registry loading: built-ins plus installed entry points."""

    def test_rows_is_builtin(self):
        assert "rows" in REPRESENTATIONS

    def test_entry_points_are_discovered(self):
        # interloper-pandas is installed in the workspace and declares the
        # "dataframe" representation as an entry point — this asserts the
        # discovery mechanism end to end, without any explicit import.
        assert "dataframe" in REPRESENTATIONS

    def test_lookup_by_key(self):
        assert isinstance(REPRESENTATIONS["rows"], RowsRepresentation)

    def test_unknown_key_raises_actionable_error(self):
        with pytest.raises(KeyError, match="'polars' is not registered"):
            REPRESENTATIONS["polars"]


class TestResolution:
    """Representation.of resolves from the data itself."""

    def test_lists_resolve_to_rows(self):
        assert isinstance(Representation.of([{"a": 1}]), RowsRepresentation)

    def test_unmatched_data_falls_back_to_rows(self):
        assert isinstance(Representation.of("anything"), RowsRepresentation)

    def test_dataframes_resolve_to_the_pandas_representation(self):
        pd = pytest.importorskip("pandas")
        assert Representation.of(pd.DataFrame()).key == "dataframe"


class TestRowsRepresentation:
    """Generic table views on list[dict] records."""

    def test_matches_lists_only(self):
        rep = RowsRepresentation()
        assert rep.matches([{"a": 1}])
        assert rep.matches([])
        assert not rep.matches({"a": 1})
        assert not rep.matches("x")

    def test_to_records_passes_lists_through(self):
        rows = [{"a": 1}]
        assert RowsRepresentation().to_records(rows) is rows

    def test_to_records_coerces_tabular_shapes(self):
        assert RowsRepresentation().to_records({"a": 1}) == [{"a": 1}]

    def test_to_records_rejects_non_tabular(self):
        with pytest.raises(NormalizerError, match="does not support type"):
            RowsRepresentation().to_records(42)

    def test_from_records_is_identity(self):
        rows = [{"a": 1}]
        assert RowsRepresentation().from_records(rows) is rows

    def test_columns(self):
        assert RowsRepresentation().columns([{"a": 1, "b": 2}]) == ["a", "b"]
        assert RowsRepresentation().columns([]) == []

    def test_filter_eq_compares_as_strings(self):
        rows = [{"d": "2024-01-01", "v": 1}, {"d": "2024-01-02", "v": 2}]
        assert RowsRepresentation().filter_eq(rows, "d", "2024-01-02") == [{"d": "2024-01-02", "v": 2}]

    def test_conformer_is_rows_conformer(self):
        from interloper.conformer import RowsConformer

        assert isinstance(RowsRepresentation().conformer, RowsConformer)
