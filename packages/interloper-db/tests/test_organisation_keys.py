"""Tests for organisation keys (``interloper_db.organisation_keys``)."""

from __future__ import annotations

import pytest
from interloper.errors import ConfigError

from interloper_db.organisation_keys import MAX_LENGTH, RESERVED, derive_key, unique_key, validate_key


class TestDeriveKey:
    def test_derive_key_lowercases_and_hyphenates(self):
        assert derive_key("Swarovski AG") == "swarovski-ag"

    def test_runs_of_punctuation_collapse_to_one_hyphen(self):
        assert derive_key("  Acme -- & Sons!  ") == "acme-sons"

    def test_accents_fold_to_ascii(self):
        assert derive_key("Müller Café") == "muller-cafe"

    @pytest.mark.parametrize(
        ("name", "key"),
        [
            ("Straße", "strasse"),
            ("Ørsted", "orsted"),
            ("Ærø", "aero"),
            ("Œuvre", "oeuvre"),
            ("Đặng", "dang"),
            ("Łódź", "lodz"),
            ("Þór", "thor"),
        ],
    )
    def test_letters_without_a_decomposition_are_transliterated(self, name: str, key: str):
        assert derive_key(name) == key

    def test_a_name_with_no_letters_falls_back(self):
        assert derive_key("!!! ???") == "organisation"

    def test_is_cut_to_the_maximum_length_without_a_trailing_hyphen(self):
        key = derive_key("a" * (MAX_LENGTH - 1) + " b")
        assert len(key) <= MAX_LENGTH
        assert not key.endswith("-")


class TestValidateKey:
    @pytest.mark.parametrize("key", ["swarovski", "swarovski-ag", "a", "a1-b2", "x" * MAX_LENGTH])
    def test_accepts_well_formed_keys(self, key: str):
        validate_key(key)

    @pytest.mark.parametrize(
        "key", ["", "Swarovski", "-a", "a-", "a--b", "a b", "a_b", "a.b", "ä", "x" * (MAX_LENGTH + 1)]
    )
    def test_refuses_malformed_keys(self, key: str):
        with pytest.raises(ConfigError):
            validate_key(key)

    def test_reserved_refused(self):
        with pytest.raises(ConfigError, match="reserved"):
            validate_key("settings")

    def test_reserved_covers_the_top_level_routes(self):
        assert {"settings", "admin", "login", "invite", "auth", "welcome", "api", "mcp", "oauth"} <= RESERVED
        assert {".well-known", "_nuxt", "authorize"} <= RESERVED


class TestUniqueKey:
    def test_is_the_derived_key_when_free(self):
        assert unique_key("Swarovski AG", set()) == "swarovski-ag"

    def test_takes_the_next_free_number_when_taken(self):
        assert unique_key("Acme", {"acme", "acme-2"}) == "acme-3"

    def test_a_reserved_word_gets_a_suffix(self):
        assert unique_key("Settings", set()) == "settings-2"

    def test_the_suffix_fits_within_the_maximum_length(self):
        base = "a" * MAX_LENGTH
        key = unique_key(base, {base})
        assert key.endswith("-2")
        assert len(key) <= MAX_LENGTH
