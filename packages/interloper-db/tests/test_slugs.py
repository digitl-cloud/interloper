"""Tests for organisation slugs (``interloper_db.slugs``)."""

from __future__ import annotations

import pytest
from interloper.errors import ConfigError

from interloper_db.slugs import MAX_LENGTH, RESERVED, slugify, unique_slug, validate_slug


class TestSlugify:
    def test_slugify_lowercases_and_hyphenates(self):
        assert slugify("Swarovski AG") == "swarovski-ag"

    def test_runs_of_punctuation_collapse_to_one_hyphen(self):
        assert slugify("  Acme -- & Sons!  ") == "acme-sons"

    def test_accents_fold_to_ascii(self):
        assert slugify("Müller Café") == "muller-cafe"

    @pytest.mark.parametrize(
        ("name", "slug"),
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
    def test_letters_without_a_decomposition_are_transliterated(self, name: str, slug: str):
        assert slugify(name) == slug

    def test_a_name_with_no_letters_falls_back(self):
        assert slugify("!!! ???") == "organisation"

    def test_is_cut_to_the_maximum_length_without_a_trailing_hyphen(self):
        slug = slugify("a" * (MAX_LENGTH - 1) + " b")
        assert len(slug) <= MAX_LENGTH
        assert not slug.endswith("-")


class TestValidateSlug:
    @pytest.mark.parametrize("slug", ["swarovski", "swarovski-ag", "a", "a1-b2", "x" * MAX_LENGTH])
    def test_accepts_well_formed_slugs(self, slug: str):
        validate_slug(slug)

    @pytest.mark.parametrize(
        "slug", ["", "Swarovski", "-a", "a-", "a--b", "a b", "a_b", "a.b", "ä", "x" * (MAX_LENGTH + 1)]
    )
    def test_refuses_malformed_slugs(self, slug: str):
        with pytest.raises(ConfigError):
            validate_slug(slug)

    def test_reserved_refused(self):
        with pytest.raises(ConfigError, match="reserved"):
            validate_slug("settings")

    def test_reserved_covers_the_top_level_routes(self):
        assert {"settings", "admin", "login", "invite", "auth", "welcome", "api", "mcp", "oauth"} <= RESERVED
        assert {".well-known", "_nuxt", "authorize"} <= RESERVED


class TestUniqueSlug:
    def test_is_the_slugified_name_when_free(self):
        assert unique_slug("Swarovski AG", set()) == "swarovski-ag"

    def test_takes_the_next_free_number_when_taken(self):
        assert unique_slug("Acme", {"acme", "acme-2"}) == "acme-3"

    def test_a_reserved_word_gets_a_suffix(self):
        assert unique_slug("Settings", set()) == "settings-2"

    def test_the_suffix_fits_within_the_maximum_length(self):
        base = "a" * MAX_LENGTH
        slug = unique_slug(base, {base})
        assert slug.endswith("-2")
        assert len(slug) <= MAX_LENGTH
