"""Tests for the Resource base class."""

import pytest
from pydantic_settings import SettingsConfigDict

from interloper.resource import Resource


class TestResourceEnvLoading:
    """Resource extends BaseSettings, so every resource env-loads its fields."""

    def test_field_loaded_from_env(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("RES_ENV_TEST_TOKEN", "from-env")

        class MyService(Resource):
            model_config = SettingsConfigDict(env_prefix="res_env_test_")

            token: str

        assert MyService().token == "from-env"  # ty: ignore[missing-argument]

    def test_explicit_value_overrides_env(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("RES_ENV_TEST2_TOKEN", "from-env")

        class MyService(Resource):
            model_config = SettingsConfigDict(env_prefix="res_env_test2_")

            token: str

        assert MyService(token="explicit").token == "explicit"

    def test_unknown_kwargs_still_rejected(self):
        class MyService(Resource):
            token: str = "t"

        with pytest.raises(TypeError, match="unexpected keyword"):
            MyService(nope="x")  # ty: ignore[unknown-argument]
