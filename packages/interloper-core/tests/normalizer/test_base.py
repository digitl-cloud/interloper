"""Tests for ``interloper.normalizer.base``."""

from interloper.normalizer import Normalizer


class TestColumnName:
    """Column-name conversion conventions."""

    def test_default_snake_case(self):
        n = Normalizer()
        assert n.column_name("campaignBudgetCurrencyCode") == "campaign_budget_currency_code"

    def test_digits_attached_by_default(self):
        n = Normalizer()
        assert n.column_name("acosClicks14d") == "acos_clicks14d"

    def test_snake_case_digits_splits_letter_digit_boundary(self):
        n = Normalizer(snake_case_digits=True)
        assert n.column_name("acosClicks14d") == "acos_clicks_14d"
        assert n.column_name("purchases1d") == "purchases_1d"
        assert n.column_name("kindleEditionNormalizedPagesRead14d") == "kindle_edition_normalized_pages_read_14d"

    def test_snake_case_digits_does_not_split_digit_letter(self):
        n = Normalizer(snake_case_digits=True)
        # the digit group keeps its unit suffix attached: 14d, not 14_d
        assert n.column_name("sales30d") == "sales_30d"

    def test_column_overrides_win(self):
        n = Normalizer(column_overrides={"eCPAddToCart": "ecp_add_to_cart"})
        assert n.column_name("eCPAddToCart") == "ecp_add_to_cart"
        assert n.column_name("eCPBrandSearch") == "e_cp_brand_search"  # no override -> default rule

    def test_overrides_apply_during_normalize(self):
        n = Normalizer(column_overrides={"eCPAddToCart": "ecp_add_to_cart"})
        rows = n.normalize([{"eCPAddToCart": 1.0, "campaignName": "x"}])
        assert rows == [{"ecp_add_to_cart": 1.0, "campaign_name": "x"}]

    def test_replace_empty_strings(self):
        n = Normalizer(normalize_columns_names=False, fill_missing=False, replace_empty_strings=True)
        assert n.normalize([{"a": "", "b": "x"}]) == [{"a": None, "b": "x"}]

    def test_replace_empty_dicts(self):
        n = Normalizer(normalize_columns_names=False, fill_missing=False, flatten_max_level=0, replace_empty_dicts=True)
        assert n.normalize([{"a": {}, "b": {"k": 1}}]) == [{"a": None, "b": {"k": 1}}]


class TestFlatten:
    """Nested dicts are flattened into separator-joined columns."""

    def test_no_flattening_by_default(self):
        rows = Normalizer().normalize([{"a": {"b": 1}}])

        assert rows == [{"a": {"b": 1}}]

    def test_one_level(self):
        rows = Normalizer(flatten_max_level=1).normalize([{"a": {"b": 1}, "c": 2}])

        assert rows == [{"a_b": 1, "c": 2}]

    def test_the_level_bounds_the_depth(self):
        rows = Normalizer(flatten_max_level=1).normalize([{"a": {"b": {"c": 1}}}])

        assert rows == [{"a_b": {"c": 1}}]

    def test_none_flattens_all_the_way_down(self):
        rows = Normalizer(flatten_max_level=None).normalize([{"a": {"b": {"c": 1}}}])

        assert rows == [{"a_b_c": 1}]

    def test_the_separator_is_configurable(self):
        rows = Normalizer(
            flatten_max_level=None, flatten_separator="__", normalize_columns_names=False
        ).normalize([{"a": {"b": 1}}])

        assert rows == [{"a__b": 1}]


class TestFillMissing:
    """Rows are aligned to one key set so the destination sees a rectangle."""

    def test_gaps_become_none(self):
        rows = Normalizer().normalize([{"a": 1}, {"b": 2}])

        assert rows == [{"a": 1, "b": None}, {"a": None, "b": 2}]

    def test_key_insertion_order_is_preserved(self):
        rows = Normalizer().normalize([{"b": 1}, {"a": 2}])

        assert list(rows[0]) == ["b", "a"]

    def test_disabled_leaves_the_rows_ragged(self):
        rows = Normalizer(fill_missing=False).normalize([{"a": 1}, {"b": 2}])

        assert rows == [{"a": 1}, {"b": 2}]

    def test_no_rows_is_a_no_op(self):
        assert Normalizer().normalize([]) == []


class TestDropNaColumns:
    """All-null columns can be dropped before the schema is inferred."""

    def test_a_wholly_null_column_is_dropped(self):
        rows = Normalizer(drop_na_columns=True).normalize([{"a": 1, "b": None}, {"a": 2, "b": None}])

        assert rows == [{"a": 1}, {"a": 2}]

    def test_a_partially_null_column_is_kept(self):
        rows = Normalizer(drop_na_columns=True).normalize([{"a": 1, "b": None}, {"a": 2, "b": 3}])

        assert rows == [{"a": 1, "b": None}, {"a": 2, "b": 3}]

    def test_nothing_to_drop_returns_the_rows_unchanged(self):
        rows = Normalizer(drop_na_columns=True).normalize([{"a": 1}, {"a": 2}])

        assert rows == [{"a": 1}, {"a": 2}]

    def test_disabled_by_default(self):
        rows = Normalizer().normalize([{"a": None}])

        assert rows == [{"a": None}]
