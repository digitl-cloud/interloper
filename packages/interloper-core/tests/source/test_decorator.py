"""Tests for ``interloper.source.decorator``."""

import inspect
from typing import Any

import pytest

import interloper as il
from interloper.normalizer import MaterializationStrategy
from interloper.source.decorator import _SOURCE_PARAMS

# -- Fixtures ------------------------------------------------------------------

# A usable value per decorator parameter, so the whole advertised surface can be
# exercised generically.
PARAM_VALUES: dict[str, Any] = {
    "resources": {},
    "destinations": [],
    "tags": ["Tag"],
    "key": "custom_key",
    "name": "Custom Name",
    "icon": "icon:custom",
    "dataset": "custom_dataset",
    "default_destination_key": "custom_destination",
    "normalizer": None,
    "materialization_strategy": MaterializationStrategy.RECONCILE,
}


def advertised_params() -> list[str]:
    """Keyword parameters the ``@source`` decorator accepts.

    Returns:
        Every keyword-only parameter name of the decorator's implementation.
    """
    signature = inspect.signature(il.source)
    return [
        name for name, parameter in signature.parameters.items() if parameter.kind is inspect.Parameter.KEYWORD_ONLY
    ]


# -- Tests ---------------------------------------------------------------------


class TestParameterSurface:
    def test_every_advertised_parameter_has_a_test_value(self):
        assert set(advertised_params()) == set(PARAM_VALUES)

    def test_every_advertised_parameter_is_routed(self):
        assert set(advertised_params()) == set(_SOURCE_PARAMS)

    @pytest.mark.parametrize("param", advertised_params())
    def test_every_advertised_parameter_builds_a_function_source(self, param):
        """Each parameter must actually work — a routed name with no matching field crashes at build."""
        value = PARAM_VALUES.get(param, "__missing__")
        assert value != "__missing__", f"add a sample value for the new {param!r} decorator parameter"
        if value is None:
            pytest.skip(f"{param} has no non-None sample value")

        @il.source(**{param: value})
        def probe():
            return []

        assert issubclass(probe, il.Source)

    @pytest.mark.parametrize("param", advertised_params())
    def test_every_advertised_parameter_builds_a_class_source(self, param):
        value = PARAM_VALUES.get(param, "__missing__")
        assert value != "__missing__", f"add a sample value for the new {param!r} decorator parameter"
        if value is None:
            pytest.skip(f"{param} has no non-None sample value")

        @il.source(**{param: value})
        class Probe:
            pass

        assert issubclass(Probe, il.Source)


class TestMaterializable:
    """``materializable`` is an asset-level runtime flag, not a source declaration."""

    def test_not_accepted_by_the_decorator(self):
        with pytest.raises(TypeError, match="materializable"):
            # The type checker rejects this too, which is half the point of the fix.
            @il.source(materializable=False)  # ty: ignore[no-matching-overload]
            def probe():
                return []

    def test_still_available_as_an_instance_override(self):
        @il.asset
        def probe_asset() -> list[dict[str, Any]]:
            return []

        @il.source
        def probe_source():
            return [probe_asset]

        assert all(not a.materializable for a in probe_source()(materializable=False).assets)
