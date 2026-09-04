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


class TestFunctionForm:
    """``@il.source`` on a function that returns its assets."""

    def test_a_list_of_asset_classes_becomes_the_source(self):
        @il.asset
        def one() -> list[dict[str, Any]]:
            return [{"a": 1}]

        @il.asset
        def two() -> list[dict[str, Any]]:
            return [{"a": 2}]

        @il.source
        def pair() -> list[type[il.Asset]]:
            return [one, two]

        assert issubclass(pair, il.Source)
        assert {asset.key for asset in pair().assets} == {"one", "two"}

    def test_a_single_asset_class_is_accepted(self):
        @il.asset
        def solo() -> list[dict[str, Any]]:
            return [{"a": 1}]

        @il.source
        def wrapper() -> type[il.Asset]:
            return solo

        assert [asset.key for asset in wrapper().assets] == ["solo"]

    def test_a_function_returning_nothing_usable_yields_no_assets(self):
        @il.source
        def empty() -> None:
            pass

        assert empty().assets == []

    def test_annotated_parameters_become_config_fields(self):
        @il.asset
        def rows() -> list[dict[str, Any]]:
            return [{"a": 1}]

        @il.source
        def configured(
            account_id: str = il.InputField(default="acc-1"),
            region: str = il.InputField(default="eu"),
        ) -> list[type[il.Asset]]:
            return [rows]

        assert set(configured.model_fields) >= {"account_id", "region"}
        instance = configured(account_id="acc-2")  # ty: ignore[unknown-argument]
        assert instance.account_id == "acc-2"
        assert instance.region == "eu"

    def test_the_docstring_is_carried_over(self):
        # Source docstrings ship as the component's description in the app.
        @il.source
        def documented() -> None:
            """Everything the vendor exposes."""

        assert documented.__doc__ == "Everything the vendor exposes."

    def test_the_key_and_module_come_from_the_function(self):
        @il.source
        def google_ads() -> None:
            pass

        assert google_ads.key == "google_ads"
        assert google_ads.__module__ == __name__
