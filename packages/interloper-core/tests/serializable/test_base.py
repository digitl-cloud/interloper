"""Tests for ``interloper.serializable.base``."""

from __future__ import annotations

from typing import Any

import pytest

import interloper as il
from interloper.errors import SpecError
from interloper.normalizer import Normalizer
from interloper.serializable import Serializable, Spec, dump_spec_value
from interloper.source.base import Source

# -- Fixtures ------------------------------------------------------------------


class FakeSerializable(Serializable):
    """Minimal class-plus-configuration fixture."""

    text: str = ""


class FakeOtherSerializable(Serializable):
    """Second class, used to exercise type-mismatch scenarios."""

    value: str = ""


# -- The Spec envelope ---------------------------------------------------------


class TestSpecEnvelope:
    """One total wire format: ``path`` XOR ``key``, optional ``id``, ``init``."""

    def test_roundtrip_preserves_class_and_config(self):
        spec = FakeSerializable(text="abc").to_spec()
        rebuilt = FakeSerializable.from_spec(spec)
        assert type(rebuilt) is FakeSerializable
        assert rebuilt.text == "abc"

    def test_serializable_spec_carries_no_id(self):
        assert FakeSerializable(text="abc").to_spec().id == ""

    def test_path_and_key_are_exclusive(self):
        with pytest.raises(ValueError, match="exactly one of 'path' or 'key'"):
            Spec(path="a.B", key="b")
        with pytest.raises(ValueError, match="exactly one of 'path' or 'key'"):
            Spec()

    def test_non_serializable_path_raises(self):
        with pytest.raises(TypeError, match="does not resolve to a Serializable class"):
            Spec(path="interloper.errors.SpecError").reconstruct()


class TestSpecFromYamlFile:
    """Spec documents load from YAML with env interpolation."""

    def test_loads_and_reconstructs(self, tmp_path):
        file = tmp_path / "spec.yaml"
        file.write_text(f"path: {FakeSerializable().path()}\ninit: {{text: abc}}\n")
        instance = Spec.from_file(file).reconstruct()
        assert isinstance(instance, FakeSerializable)
        assert instance.text == "abc"

    def test_env_placeholders_interpolate(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FAKE_SPEC_TEXT", "from-env")
        file = tmp_path / "spec.yaml"
        file.write_text(f"path: {FakeSerializable().path()}\ninit:\n  text: ${{FAKE_SPEC_TEXT}}\n")
        instance = Spec.from_file(file).reconstruct()
        assert isinstance(instance, FakeSerializable)
        assert instance.text == "from-env"

    def test_missing_env_variable_raises(self, tmp_path):
        file = tmp_path / "spec.yaml"
        file.write_text(f"path: {FakeSerializable().path()}\ninit:\n  text: ${{FAKE_SPEC_MISSING}}\n")
        with pytest.raises(SpecError, match="undefined environment variable"):
            Spec.from_file(file)

    def test_invalid_yaml_raises(self, tmp_path):
        file = tmp_path / "spec.yaml"
        file.write_text("path: [unclosed")
        with pytest.raises(SpecError, match="Invalid YAML"):
            Spec.from_file(file)

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(SpecError, match="Cannot read spec file"):
            Spec.from_file(tmp_path / "nope.yaml")

    def test_non_mapping_raises(self, tmp_path):
        file = tmp_path / "spec.yaml"
        file.write_text("- a\n- list\n")
        with pytest.raises(SpecError, match="must be a YAML mapping"):
            Spec.from_file(file)


class TestSubclassScopedConstruction:
    """from_spec / from_spec_file honor the receiving class."""

    def test_from_spec_type_checks_the_receiver(self):
        spec = FakeSerializable(text="abc").to_spec()
        with pytest.raises(TypeError, match="does not reconstruct to a FakeOtherSerializable"):
            FakeOtherSerializable.from_spec(spec)

    def test_from_spec_file_reconstructs_for_the_receiver(self, tmp_path):
        file = tmp_path / "spec.yaml"
        file.write_text(f"path: {FakeSerializable().path()}\ninit: {{text: abc}}\n")
        instance = FakeSerializable.from_spec_file(file)
        assert isinstance(instance, FakeSerializable)
        assert instance.text == "abc"


class TestStrictInit:
    """Unknown init kwargs fail loudly instead of being silently dropped."""

    def test_unknown_kwarg_raises(self):
        with pytest.raises(TypeError, match="unexpected keyword argument.*nope"):
            FakeSerializable(nope=1)  # type: ignore[call-arg]  # ty: ignore[unknown-argument]


# -- build_class ---------------------------------------------------------------


@il.source(normalizer=Normalizer(snake_case_digits=True), dataset="custom_ds")
class DecoratedSource(il.Source):
    """Class-based source with decorator-provided field defaults."""

    @il.asset
    def my_asset(self) -> list:
        return []


class TestBuildClass:
    """Decorator field args must become real pydantic field defaults.

    Regression: the class builder used a plain ``setattr`` on
    already-built pydantic classes, which left ``model_fields`` (and every
    instance) on the old default — source-level normalizers silently never
    applied in production.
    """

    def test_field_defaults_reach_instances(self):
        src = DecoratedSource(id="s")
        assert src.normalizer is not None
        assert src.normalizer.snake_case_digits is True
        assert src.dataset == "custom_ds"

    def test_assets_inherit_source_normalizer(self):
        src = DecoratedSource(id="s")
        assert src.assets[0].normalizer is src.normalizer

    def test_model_fields_default_updated(self):
        assert DecoratedSource.model_fields["normalizer"].default is not None

    def test_parent_class_default_untouched(self):
        """The FieldInfo is copied — the base Source default must stay None."""
        assert Source.model_fields["normalizer"].default is None

    def test_explicit_instance_value_still_wins(self):
        override = Normalizer(snake_case_digits=False)
        src = DecoratedSource(id="s", normalizer=override)
        assert src.normalizer is override


# -- dump_spec_value -----------------------------------------------------------


class TestDumpSpecValue:
    """Serializable values must survive the spec round-trip with their class.

    Regression: normalizers (then dataclasses) were dumped as bare dicts, so
    reconstruction coerced them into the field's base annotation type, losing
    the concrete subclass (``DataFrameNormalizer`` degraded to ``Normalizer``
    across the host → child-pod boundary). Normalizer is Serializable now, so
    it serializes as a reconstructible spec like everything else.
    """

    def test_normalizer_dumps_as_reconstructible_spec(self):
        n = Normalizer(snake_case_digits=True, column_overrides={"rawName": "raw_name"})
        dumped = dump_spec_value(n)
        assert dumped["path"].endswith("Normalizer")
        assert dumped["init"]["snake_case_digits"] is True

    def test_normalizer_roundtrip_preserves_class_and_config(self):
        n = Normalizer(snake_case_digits=True, flatten_max_level=2)
        dumped = dump_spec_value(n)
        rebuilt: Any = Spec(path=dumped["path"], init=dumped["init"]).reconstruct()
        assert type(rebuilt) is Normalizer
        assert rebuilt.snake_case_digits is True
        assert rebuilt.flatten_max_level == 2

    def test_scalars_and_containers_unchanged(self):
        assert dump_spec_value([1, "a", {"k": 2}]) == [1, "a", {"k": 2}]

    def test_component_dumps_as_spec(self):
        @il.asset
        def some_asset() -> list:
            return []

        dumped = dump_spec_value(some_asset(id="a1"))
        assert dumped["id"] == "a1"
        assert "path" in dumped
