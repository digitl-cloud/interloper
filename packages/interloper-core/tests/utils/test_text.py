"""Tests for ``interloper.utils.text``."""

import pytest

from interloper.utils.text import to_identifier, validate_key


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
