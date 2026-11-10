"""Tests for ``interloper.utils.annotations``."""

from __future__ import annotations

import datetime as dt
from typing import ClassVar

import pytest
from pydantic import ValidationError

from interloper.utils.annotations import classvar_type, is_classvar, type_adapter


class Owner:
    """Annotation owner whose module resolves ``dt``."""


class Opaque:
    """A plain class Pydantic has no schema for."""


class TestIsClassvar:
    @pytest.mark.parametrize("hint", [ClassVar, ClassVar[int], "ClassVar[int]", "typing.ClassVar[str]", "ClassVar"])
    def test_recognises_classvar_forms(self, hint):
        assert is_classvar(hint)

    @pytest.mark.parametrize("hint", [int, "int", "list[ClassVar]", "Optional[int]"])
    def test_rejects_everything_else(self, hint):
        assert not is_classvar(hint)


class TestClassvarType:
    def test_resolves_a_string_in_the_owners_module(self):
        assert classvar_type("ClassVar[dt.timedelta]", Owner) is dt.timedelta

    def test_reads_an_evaluated_annotation(self):
        assert classvar_type(ClassVar[list[str]], Owner) == list[str]

    def test_a_bare_classvar_has_no_type(self):
        assert classvar_type("ClassVar", Owner) is None

    def test_an_unresolvable_annotation_has_no_type(self):
        assert classvar_type("ClassVar[NotDefinedAnywhere]", Owner) is None


class TestTypeAdapter:
    def test_validates_against_the_type(self):
        assert type_adapter(list[str]).validate_python(["a"]) == ["a"]
        with pytest.raises(ValidationError):
            type_adapter(list[str]).validate_python("a")

    def test_is_cached_per_type(self):
        assert type_adapter(int) is type_adapter(int)

    def test_a_plain_class_is_checked_by_instance(self):
        opaque = Opaque()
        assert type_adapter(Opaque).validate_python(opaque) is opaque
        with pytest.raises(ValidationError):
            type_adapter(Opaque).validate_python(1)
