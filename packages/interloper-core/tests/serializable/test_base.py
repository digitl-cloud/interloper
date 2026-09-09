"""Tests for ``interloper.serializable.base``."""

from __future__ import annotations

import warnings
from typing import Any

import pytest

import interloper as il
from interloper.errors import SpecError
from interloper.normalizer import Normalizer
from interloper.serializable import Serializable, SerializationContext, Spec
from interloper.source.base import Source

# -- Fixtures ------------------------------------------------------------------


class FakeSerializable(Serializable):
    """Minimal class-plus-configuration fixture."""

    text: str = ""


class FakeOtherSerializable(Serializable):
    """Second class, used to exercise type-mismatch scenarios."""

    value: str = ""


with warnings.catch_warnings():
    warnings.filterwarnings("ignore", message=r'Field name "name" in "ShadowingSerializable" shadows an attribute')

    class ShadowingSerializable(Serializable):
        """Declares a ``name`` data field that shadows the parent ClassVar."""

        alpha: str | None = None
        name: str | None = None
        beta: str | None = None


class RefConn(il.Connection):
    """Connection filled by reference in reconstruction tests."""


class RefWidget(il.Source):
    """Source whose connection is filled by reference in reconstruction tests."""

    connection: RefConn


class OwningSource(il.Source):
    """Source with one asset, for the ownership rules of the serialization context."""

    @il.asset
    def rows(self) -> list[dict[str, Any]]:
        return []


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


class TestFieldOrdering:
    """A field shadowing a parent ClassVar keeps its declaration position."""

    def test_model_fields_keep_declaration_order(self):
        assert list(ShadowingSerializable.model_fields) == ["alpha", "name", "beta"]

    def test_dump_order_matches_declaration(self):
        assert list(ShadowingSerializable(name="x").model_dump()) == ["alpha", "name", "beta"]

    def test_subclass_appends_new_fields_after_inherited(self):
        class Extended(ShadowingSerializable):
            gamma: str | None = None

        assert list(Extended.model_fields) == ["alpha", "name", "beta", "gamma"]

    def test_classvars_untouched_by_shadowing_field(self):
        assert ShadowingSerializable.key == "shadowing_serializable"
        assert ShadowingSerializable(name="x").name == "x"

    def test_spec_roundtrip_with_shadowing_field(self):
        rebuilt = ShadowingSerializable.from_spec(ShadowingSerializable(name="x").to_spec())
        assert type(rebuilt) is ShadowingSerializable
        assert rebuilt.name == "x"
        assert type(rebuilt).key == "shadowing_serializable"


class TestStrictInit:
    """Unknown init kwargs fail loudly instead of being silently dropped."""

    def test_unknown_kwarg_raises(self):
        with pytest.raises(TypeError, match="unexpected keyword argument.*nope"):
            FakeSerializable(nope=1)  # type: ignore[call-arg]  # ty: ignore[unknown-argument]


class TestReferences:
    """``{"ref": id}`` values are bound after construction, from the document or from *resolve*."""

    def test_a_reference_binds_the_instance_the_document_already_built(self):
        widget = RefWidget(connection=RefConn())
        rebuilt = RefWidget.from_spec(widget.to_spec())
        assert rebuilt.connection.id == widget.connection.id

    def test_an_unresolved_reference_without_resolve_is_a_spec_error(self):
        spec = Spec(path=RefWidget.classpath(), init={"connection": {"ref": "nope"}})
        with pytest.raises(SpecError, match="unresolved reference 'nope'"):
            spec.reconstruct()

    def test_a_resolve_callable_supplies_a_missing_reference(self):
        connection = RefConn()
        spec = Spec(path=RefWidget.classpath(), init={"connection": {"ref": connection.id}})
        widget = RefWidget.from_spec(spec, resolve={connection.id: connection}.__getitem__)
        assert widget.connection is connection

    def test_a_resolve_callable_reaches_from_spec_file(self, tmp_path):
        connection = RefConn()
        file = tmp_path / "widget.yaml"
        file.write_text(f"path: {RefWidget.classpath()}\ninit:\n  connection:\n    ref: {connection.id}\n")
        widget = RefWidget.from_spec_file(file, resolve={connection.id: connection}.__getitem__)
        assert widget.connection is connection


class TestSerializationContext:
    """The reconstruction in progress: references held back, owners pinned, bound once whole."""

    def test_hold_takes_references_out_and_pins_the_owner(self):
        init = {"connection": {"ref": "c"}, "assets": {"revenue": {"dataset": "x", "orders": [{"ref": "o"}]}}}
        kept = SerializationContext().hold(init)

        assert "connection" not in kept
        assert kept["id"]
        revenue = kept["assets"]["revenue"]
        assert "orders" not in revenue
        assert revenue["dataset"] == "x"
        assert revenue["id"]

    def test_hold_keeps_a_declared_id_and_a_value_without_references(self):
        kept = SerializationContext().hold({"id": "root", "connection": {"ref": "c"}, "select": ["a"]})
        assert kept == {"id": "root", "select": ["a"]}

    def test_a_reference_mixed_with_an_inline_target_keeps_the_document_order(self):
        inline = il.MemoryDestination()
        referenced = il.MemoryDestination()
        spec = Spec(
            path=RefWidget.classpath(),
            init={
                "connection": RefConn().to_spec().model_dump(exclude_none=True),
                "destinations": [inline.to_spec().model_dump(exclude_none=True), {"ref": referenced.id}],
            },
        )
        widget = RefWidget.from_spec(spec, resolve={referenced.id: referenced}.__getitem__)
        assert [d.id for d in widget.destinations] == [inline.id, referenced.id]

    def test_a_parentless_target_is_written_once_then_referenced(self):
        destination = il.MemoryDestination()
        context = SerializationContext()

        first, second = context.emit(destination), context.emit(destination)

        assert first is not None and first["path"] == destination.path()
        assert second == Spec.reference(destination.id)

    def test_an_owned_target_is_referenced_when_carried_and_dropped_when_a_closed_context_is_not(self):
        asset = OwningSource().assets[0]

        assert SerializationContext().emit(asset) == Spec.reference(asset.id)
        assert SerializationContext([asset]).emit(asset) == Spec.reference(asset.id)
        assert SerializationContext([il.MemoryDestination()]).emit(asset) is None

    def test_a_closed_context_writes_its_own_copies_of_the_owned_components(self):
        original = OwningSource().assets[0]
        copy = original(materializable=False)

        assert SerializationContext().carried([original]) == [original]
        assert SerializationContext([copy]).carried([original]) == [copy]
        assert SerializationContext([il.MemoryDestination()]).carried([original]) == []

    def test_a_pinned_init_that_built_no_component_is_a_spec_error(self):
        spec = Spec(
            path=RefWidget.classpath(),
            init={
                "connection": RefConn().to_spec().model_dump(exclude_none=True),
                "assets": {"nope": {"orders": {"ref": "o"}}},
            },
        )
        with pytest.raises(SpecError, match="no component was built for .* under 'orders'"):
            spec.reconstruct()


class TestSpecDocuments:
    """A spec file may hold several YAML documents."""

    def test_all_from_file_loads_every_document(self, tmp_path):
        file = tmp_path / "specs.yaml"
        path = FakeSerializable().path()
        file.write_text(f"path: {path}\ninit: {{text: one}}\n---\npath: {path}\ninit: {{text: two}}\n")
        specs = Spec.all_from_file(file)
        assert [spec.init["text"] for spec in specs if spec.init] == ["one", "two"]

    def test_from_file_rejects_a_multi_document_file(self, tmp_path):
        file = tmp_path / "specs.yaml"
        path = FakeSerializable().path()
        file.write_text(f"path: {path}\n---\npath: {path}\n")
        with pytest.raises(SpecError, match="exactly one document"):
            Spec.from_file(file)


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


# -- Spec.dump_value -----------------------------------------------------------


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
        dumped = Spec.dump_value(n)
        assert dumped["path"].endswith("Normalizer")
        assert dumped["init"]["snake_case_digits"] is True

    def test_normalizer_roundtrip_preserves_class_and_config(self):
        n = Normalizer(snake_case_digits=True, flatten_max_level=2)
        dumped = Spec.dump_value(n)
        rebuilt: Any = Spec(path=dumped["path"], init=dumped["init"]).reconstruct()
        assert type(rebuilt) is Normalizer
        assert rebuilt.snake_case_digits is True
        assert rebuilt.flatten_max_level == 2

    def test_scalars_and_containers_unchanged(self):
        assert Spec.dump_value([1, "a", {"k": 2}]) == [1, "a", {"k": 2}]

    def test_component_dumps_as_spec(self):
        @il.asset
        def some_asset() -> list:
            return []

        dumped = Spec.dump_value(some_asset(id="a1"))
        assert dumped["id"] == "a1"
        assert "path" in dumped
