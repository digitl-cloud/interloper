"""Tests for ``interloper.component.build``."""

from typing import Any

import interloper as il
from interloper.component.base import ComponentSpec, dump_spec_value
from interloper.normalizer import Normalizer
from interloper.source.base import Source


@il.source(normalizer=Normalizer(snake_case_digits=True), dataset="custom_ds")
class DecoratedSource(il.Source):
    """Class-based source with decorator-provided field defaults."""

    @il.asset
    def my_asset(self) -> list:
        return []


class TestClassBasedDecoratorFields:
    """Decorator field args must become real pydantic field defaults.

    Regression: ``build_component_class`` used a plain ``setattr`` on
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


class TestDumpSpecValue:
    """Configured component values must survive the spec round-trip with their class.

    Regression: normalizers (then dataclasses) were dumped as bare dicts, so
    reconstruction coerced them into the field's base annotation type, losing
    the concrete subclass (``DataFrameNormalizer`` degraded to ``Normalizer``
    across the host → child-pod boundary). Normalizer is a Component now, so
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
        rebuilt: Any = ComponentSpec(path=dumped["path"], init=dumped["init"]).reconstruct()
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
