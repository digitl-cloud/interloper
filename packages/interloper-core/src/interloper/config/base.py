"""Config: a resource for holding configuration values with env-loading support."""

from __future__ import annotations

from typing import ClassVar

from pydantic_settings import BaseSettings

from interloper.resource import Resource


class Config(BaseSettings, Resource):
    """A resource for arbitrary configuration values.

    Extends both ``Resource`` and ``BaseSettings``, so config values
    can be loaded from environment variables, .env files, or passed
    directly::

        class MyConfig(Config):
            api_key: str
            base_url: str = "https://api.example.com"

        # Loads API_KEY from environment if not passed explicitly
        config = MyConfig()
    """

    kind: ClassVar[str] = "config"
