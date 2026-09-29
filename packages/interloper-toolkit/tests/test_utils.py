"""Tests for ``interloper_toolkit.utils``."""

from __future__ import annotations

from interloper_toolkit.utils import clip


class TestClip:
    def test_short_text_and_none_pass_through(self):
        assert clip("abc", 3) == "abc"
        assert clip(None, 3) is None

    def test_head_is_kept_and_the_cut_is_marked(self):
        assert clip("abcdef", 4) == "abcd…[+2 chars]"

    def test_tail_is_kept_when_asked(self):
        assert clip("abcdef", 4, tail=True) == "…[+2 chars]cdef"
