"""Tests for ``interloper.component.maturity``."""

from __future__ import annotations

import pytest

from interloper.component.maturity import Maturity


class TestOf:
    def test_a_member_is_returned_as_is(self):
        assert Maturity.of(Maturity.BETA) is Maturity.BETA

    def test_a_string_value_is_coerced(self):
        assert Maturity.of("alpha") is Maturity.ALPHA

    def test_an_unknown_value_raises_listing_the_members(self):
        with pytest.raises(
            TypeError, match=r"Unknown maturity 'gamma'; expected one of 'deprecated', 'alpha', 'beta', 'stable'"
        ):
            Maturity.of("gamma")


class TestLeast:
    def test_returns_the_least_mature(self):
        assert Maturity.least(Maturity.STABLE, Maturity.ALPHA, Maturity.BETA) is Maturity.ALPHA

    def test_deprecated_wins_over_every_other_level(self):
        assert Maturity.least(Maturity.ALPHA, Maturity.DEPRECATED, Maturity.STABLE) is Maturity.DEPRECATED

    def test_a_single_value_is_returned(self):
        assert Maturity.least(Maturity.STABLE) is Maturity.STABLE

    def test_the_value_serializes_as_its_string(self):
        assert Maturity.BETA == "beta"
