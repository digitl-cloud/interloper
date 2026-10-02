"""Tests for ``interloper.component.maturity``."""

from __future__ import annotations

from interloper.component.maturity import Maturity


class TestLeast:
    def test_returns_the_least_mature(self):
        assert Maturity.least(Maturity.STABLE, Maturity.ALPHA, Maturity.BETA) is Maturity.ALPHA

    def test_deprecated_wins_over_every_other_level(self):
        assert Maturity.least(Maturity.ALPHA, Maturity.DEPRECATED, Maturity.STABLE) is Maturity.DEPRECATED

    def test_a_single_value_is_returned(self):
        assert Maturity.least(Maturity.STABLE) is Maturity.STABLE

    def test_the_value_serializes_as_its_string(self):
        assert Maturity.BETA == "beta"
