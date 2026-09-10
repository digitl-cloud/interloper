"""Tests for ``interloper.representation.base``."""

import pytest
from pydantic import Field

from interloper.errors import RepresentationError, SchemaError
from interloper.representation import REPRESENTATIONS, Representation, RowsRepresentation, View
from interloper.schema import Schema


class UserSchema(Schema):
    user_id: int | None = Field(...)
    name: str | None = Field(...)


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
    """Representation.of resolves from the data itself and binds to it."""

    def test_lists_resolve_to_rows(self):
        view = Representation.of([{"a": 1}])
        assert isinstance(view, View)
        assert isinstance(view.representation, RowsRepresentation)
        assert view.key == "rows"

    def test_unmatched_data_is_an_explicit_error(self):
        with pytest.raises(RepresentationError, match="No representation matches int.*dataframe.*rows"):
            Representation.of(42)

    def test_a_dict_is_not_a_table(self):
        with pytest.raises(RepresentationError, match="dict"):
            Representation.of({"a": 1})

    def test_dataframes_resolve_to_the_pandas_representation(self):
        pd = pytest.importorskip("pandas")
        assert Representation.of(pd.DataFrame()).key == "dataframe"


class TestView:
    """The bound view delegates to its representation with the data applied."""

    def test_records(self):
        assert Representation.of([{"a": 1}]).records == [{"a": 1}]

    def test_columns(self):
        assert Representation.of([{"a": 1, "b": 2}]).columns == ["a", "b"]

    def test_schema_ops_apply_to_the_bound_data(self):
        view = Representation.of([{"user_id": "1", "name": "a", "extra": True}])
        view.validate(UserSchema)
        assert view.reconcile(UserSchema) == [{"user_id": 1, "name": "a"}]
        assert {s.name: s.type for s in view.infer().field_specs()} == {"user_id": str, "name": str, "extra": bool}

    def test_filters(self):
        rows = [{"day": "2024-01-01", "n": 1}, {"day": "2024-01-02", "n": 2}]
        view = Representation.of(rows)
        assert view.filter_eq("day", "2024-01-02") == [rows[1]]
        assert view.filter_range("day", "2024-01-01", "2024-01-02") == [rows[0]]

    def test_to_the_same_representation_returns_the_data_itself(self):
        rows = [{"a": 1}]
        assert Representation.of(rows).to("rows") is rows

    def test_to_dataframe_builds_from_records(self):
        pd = pytest.importorskip("pandas")
        frame = Representation.of([{"a": 1}, {"a": 2}]).to("dataframe")
        pd.testing.assert_frame_equal(frame, pd.DataFrame({"a": [1, 2]}))

    def test_a_dataframe_to_dataframe_is_the_same_object(self):
        pd = pytest.importorskip("pandas")
        frame = pd.DataFrame({"a": [1]})
        assert Representation.of(frame).to("dataframe") is frame

    def test_a_dataframe_to_rows_goes_through_records(self):
        pd = pytest.importorskip("pandas")
        frame = pd.DataFrame({"a": [1.0, float("nan")]})
        assert Representation.of(frame).to("rows") == [{"a": 1.0}, {"a": None}]

    def test_unknown_target_names_the_registry(self):
        with pytest.raises(KeyError, match="'polars' is not registered"):
            Representation.of([{"a": 1}]).to("polars")


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

    def test_from_records_is_identity(self):
        rows = [{"a": 1}]
        assert RowsRepresentation().from_records(rows) is rows

    def test_columns(self):
        assert RowsRepresentation().columns([{"a": 1, "b": 2}]) == ["a", "b"]
        assert RowsRepresentation().columns([]) == []

    def test_filter_range_is_half_open(self):
        import datetime as dt

        rows = [{"d": "2024-01-31"}, {"d": "2024-02-01"}, {"d": "2024-02-15"}, {"d": "2024-03-01"}]
        out = RowsRepresentation().filter_range(rows, "d", dt.date(2024, 2, 1), dt.date(2024, 3, 1))
        assert out == [{"d": "2024-02-01"}, {"d": "2024-02-15"}]

    def test_filter_range_spans_dates_and_datetimes(self):
        import datetime as dt

        # A date bound is an ISO prefix of any datetime inside the period, so
        # string comparison keeps the half-open range exact at both ends.
        rows = [
            {"d": "2024-01-31T23:00:00"},
            {"d": "2024-02-01 00:30:00"},
            {"d": dt.datetime(2024, 2, 29, 23, 59)},  # noqa: DTZ001
            {"d": "2024-03-01T00:00:00"},
        ]
        out = RowsRepresentation().filter_range(rows, "d", dt.date(2024, 2, 1), dt.date(2024, 3, 1))
        assert [str(r["d"])[:7] for r in out] == ["2024-02", "2024-02"]

    def test_iso_label_normalizes_the_separator(self):
        import datetime as dt

        from interloper.representation import iso_label

        assert iso_label(dt.datetime(2024, 2, 1, 10, 30)) == "2024-02-01T10:30:00"  # noqa: DTZ001
        assert iso_label("2024-02-01 10:30:00") == "2024-02-01T10:30:00"
        assert iso_label(dt.date(2024, 2, 1)) == "2024-02-01"

    def test_filter_eq_compares_as_strings(self):
        rows = [{"d": "2024-01-01", "v": 1}, {"d": "2024-01-02", "v": 2}]
        assert RowsRepresentation().filter_eq(rows, "d", "2024-01-02") == [{"d": "2024-01-02", "v": 2}]

    def test_validate_passes(self):
        RowsRepresentation().validate([{"user_id": 1, "name": "a"}], UserSchema)

    def test_validate_fails_on_missing_required(self):
        with pytest.raises(SchemaError, match="Field required"):
            RowsRepresentation().validate([{"user_id": 1}], UserSchema)

    def test_reconcile_coerces_and_aligns(self):
        rows = RowsRepresentation().reconcile([{"user_id": "1", "name": "a", "extra": True}], UserSchema)
        assert rows == [{"user_id": 1, "name": "a"}]

    def test_infer(self):
        inferred = RowsRepresentation().infer([{"a": 1, "b": "x"}])
        specs = {s.name: s for s in inferred.field_specs()}
        assert specs["a"].type is int
        assert specs["b"].type is str
