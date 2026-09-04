"""Tests for ``interloper.config.decorator``."""

from __future__ import annotations

import pytest

from interloper.config import Config, config


class TestBareForm:
    """``@config`` on a plain class."""

    def test_a_plain_class_becomes_a_config_subclass(self) -> None:
        @config
        class Credentials:
            """Plain class carrying only annotations."""

            api_key: str
            base_url: str = "https://api.example.com"

        assert issubclass(Credentials, Config)
        instance = Credentials(api_key="secret")  # ty: ignore[unknown-argument]
        assert instance.api_key == "secret"  # ty: ignore[unresolved-attribute]
        assert instance.base_url == "https://api.example.com"  # ty: ignore[unresolved-attribute]

    def test_the_key_is_derived_from_the_class_name(self) -> None:
        @config
        class WarehouseSettings:
            """Plain class with a multi-word name."""

            size: int = 1

        assert WarehouseSettings.key == "warehouse_settings"

    def test_a_config_subclass_keeps_its_own_type(self) -> None:
        class BigQueryConfig(Config):
            """Already a Config, so the decorator must not re-parent it."""

            project: str = "demo"

        decorated = config(BigQueryConfig)

        assert issubclass(decorated, BigQueryConfig)

    def test_values_still_load_from_the_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Config extends BaseSettings, which the decorator must preserve.
        monkeypatch.setenv("API_KEY", "from-env")

        @config
        class Credentials:
            """Plain class whose field is env-backed."""

            api_key: str

        assert Credentials().api_key == "from-env"  # ty: ignore[unresolved-attribute]


class TestParameterizedForm:
    """``@config(...)`` with catalog metadata."""

    def test_every_classvar_is_applied(self) -> None:
        @config(key="custom", name="Custom Config", icon="carbon:settings", tags=["Testing"])  # ty: ignore[invalid-argument-type]
        class Other:
            """Plain class decorated with full metadata."""

            timeout: int = 30

        assert Other.key == "custom"  # ty: ignore[unresolved-attribute]
        assert Other.name == "Custom Config"  # ty: ignore[unresolved-attribute]
        assert Other.icon == "carbon:settings"  # ty: ignore[unresolved-attribute]
        assert Other.tags == ["Testing"]  # ty: ignore[unresolved-attribute]
        assert Other(timeout=5).timeout == 5  # ty: ignore[unknown-argument]

    def test_omitted_metadata_falls_back_to_the_defaults(self) -> None:
        @config(name="Just A Name")  # ty: ignore[invalid-argument-type]
        class Other:
            """Plain class decorated with a name only."""

            timeout: int = 30

        assert Other.key == "other"  # ty: ignore[unresolved-attribute]
        assert Other.name == "Just A Name"  # ty: ignore[unresolved-attribute]

    def test_a_config_subclass_keeps_its_type_through_the_parameterized_form(self) -> None:
        @config(key="scoped")
        class Scoped(Config):
            """Already a Config, decorated with a key override."""

            timeout: int = 30

        assert issubclass(Scoped, Config)
        assert Scoped.key == "scoped"
