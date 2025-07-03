"""This module contains tests for the environment variable asset parameter classes."""
import os
from unittest.mock import patch

import pytest

from interloper import errors
from interloper.params.env import Env


class TestEnv:
    """Test the Env class."""

    def test_env_resolve_with_existing_variable(self):
        """Test Env.resolve with existing environment variable."""
        with patch.dict(os.environ, {"TEST_KEY": "test_value"}):
            env_param = Env("TEST_KEY")
            result = env_param.resolve()
            assert result == "test_value"

    def test_env_resolve_with_default(self):
        """Test Env.resolve with default value when variable doesn't exist."""
        with patch.dict(os.environ, {}, clear=True):
            env_param = Env("MISSING_KEY", default="default_value")
            result = env_param.resolve()
            assert result == "default_value"

    def test_env_resolve_without_default_raises_error(self):
        """Test Env.resolve raises error when variable doesn't exist and no default."""
        with patch.dict(os.environ, {}, clear=True):
            env_param = Env("MISSING_KEY")
            with pytest.raises(errors.AssetParamResolutionError, match="Environment variable MISSING_KEY is not set"):
                env_param.resolve()

    def test_env_initialization(self):
        """Test Env initialization."""
        env_param = Env("TEST_KEY", default="default_value")
        assert env_param.key == "TEST_KEY"
        assert env_param.default == "default_value" 