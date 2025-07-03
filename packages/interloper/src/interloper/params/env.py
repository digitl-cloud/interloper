"""This module contains the environment variable asset parameter classes."""

import os
from typing import Any

from interloper import errors
from interloper.params.base import AssetParam


class Env(AssetParam[str]):
    """An asset parameter that resolves to an environment variable."""

    # Forces an Env's instance to be of type str
    def __new__(cls, value: str, default: str | None = None) -> str:
        """Create a new instance of the environment variable parameter."""
        return super().__new__(cls)  # type: ignore

    def __init__(self, key: str, default: str | None = None) -> None:
        """Initialize the environment variable parameter.

        Args:
            key: The name of the environment variable.
            default: The default value to use if the environment variable is not set.
        """
        self.key = key
        self.default = default

    def resolve(self) -> Any:
        """Resolve the value of the parameter.

        Returns:
            The value of the environment variable.

        Raises:
            AssetParamResolutionError: If the environment variable is not set and no default is provided.
        """
        value = os.environ.get(self.key, self.default)
        if value is None:
            raise errors.AssetParamResolutionError(f"Environment variable {self.key} is not set")
        return value
