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
