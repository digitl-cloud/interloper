"""Tests for ``interloper.serializable.body``."""


import pytest

import interloper as il
from interloper.serializable import ClassBody


class PlainConfig:
    """A plain class a decorator reads."""

    timeout: int = 30

    def helper(self) -> int:
        return 1


class TestFromClass:
    def test_a_plain_class_contributes_its_members_and_annotations(self):
        body = ClassBody.from_class(PlainConfig, il.Config)

        assert body.base is None
        assert (body.name, body.module, body.qualname) == ("PlainConfig", __name__, "PlainConfig")
        assert body.doc == "A plain class a decorator reads."
        assert body.members["timeout"] == 30
        assert callable(body.members["helper"])
        assert "__dict__" not in body.members
        assert body.annotations == {"timeout": int}

    def test_a_subclass_of_the_anchor_is_the_base_with_an_empty_body(self):
        class FakeTimedConfig(il.Config):
            """A config already on its anchor."""

            timeout: int = 30

        body = ClassBody.from_class(FakeTimedConfig, il.Config)

        assert body.base is FakeTimedConfig
        assert (body.members, body.annotations) == ({}, {})
        assert body.doc == "A config already on its anchor."


class TestOf:
    def test_a_class_is_read_as_a_class(self):
        assert ClassBody.of(PlainConfig, il.Config).members["timeout"] == 30

    def test_a_function_is_read_by_the_anchor(self):
        def probe() -> None:
            return None

        with pytest.raises(TypeError, match=r"Config decorates a class, not a function"):
            ClassBody.of(probe, il.Config)


class TestFromFunction:
    def test_takes_its_identity_from_the_function(self):
        def rows() -> None:
            """Rows of something."""

        body = ClassBody.from_function(rows, members={"x": 1}, annotations={"x": int})

        assert (body.name, body.module, body.doc) == ("rows", __name__, "Rows of something.")
        assert body.qualname.endswith("rows")
        assert (body.members, body.annotations, body.base) == ({"x": 1}, {"x": int}, None)
