"""Tests for ``interloper.utils.text``."""

import pytest

from interloper.utils.text import to_identifier, to_label, to_slug_case, to_snake_case, validate_key


class TestToIdentifier:
    def test_valid_identifier_passes_through(self):
        assert to_identifier("ads_stats__act_123") == "ads_stats__act_123"

    def test_lowercases(self):
        assert to_identifier("Act_123DE") == "act_123de"

    def test_invalid_runs_collapse_to_single_underscore(self):
        assert to_identifier("act 123-DE/x") == "act_123_de_x"

    def test_double_underscore_separator_preserved(self):
        # Unlike ``to_snake_case``, ``__`` runs are kept verbatim — they
        # separate the asset key from the instance alias in table names.
        assert to_identifier("ads__act-1") == "ads__act_1"

    def test_edges_stripped(self):
        assert to_identifier("-act_123-") == "act_123"

    def test_empty_returns_empty(self):
        assert to_identifier("") == ""


class TestValidateKey:
    def test_valid_key_passes(self):
        validate_key("ads_stats__123")

    @pytest.mark.parametrize("key", ["", "1ads", "_ads", "ads-stats", "ads stats"])
    def test_invalid_key_raises(self, key):
        with pytest.raises(ValueError, match="invalid"):
            validate_key(key)


class TestToSlugCase:
    """URL/DNS-friendly slugs."""

    def test_camel_case_splits_on_the_boundary(self):
        assert to_slug_case("adsStatsByCountry") == "ads-stats-by-country"

    def test_separators_collapse_to_one_dash(self):
        assert to_slug_case("ads _ stats__by  country") == "ads-stats-by-country"

    def test_edges_are_stripped(self):
        assert to_slug_case("_ads stats_") == "ads-stats"

    def test_empty_returns_empty(self):
        assert to_slug_case("") == ""


class TestToLabel:
    """Human-readable titles for the UI."""

    def test_snake_case_becomes_title_case(self):
        assert to_label("ads_stats_by_country") == "Ads Stats By Country"

    def test_camel_case_splits_on_the_boundary(self):
        assert to_label("adsStats") == "Ads Stats"

    def test_repeated_separators_collapse(self):
        assert to_label("ads__stats  by-country") == "Ads Stats By Country"

    def test_empty_returns_empty(self):
        assert to_label("") == ""


class TestToSnakeCase:
    """The column-name convention.

    Acronyms keep together (``httpURL`` → ``http_url``, not ``http_u_r_l``)
    because vendor payloads are full of them.
    """

    def test_camel_case(self):
        assert to_snake_case("campaignBudget") == "campaign_budget"

    def test_an_acronym_stays_one_word(self):
        assert to_snake_case("httpURLTarget") == "http_url_target"

    def test_already_snake_case_passes_through(self):
        assert to_snake_case("ads_stats") == "ads_stats"

    def test_empty_returns_empty(self):
        assert to_snake_case("") == ""
