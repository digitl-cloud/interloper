"""Decorator for creating Config subclasses from plain classes."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar, overload

from interloper.component.build import build_component_class
from interloper.config.base import Config

# Bounded TypeVar so that classes already extending Config preserve their
# specific type through the decorator (e.g. BigQueryConfig stays BigQueryConfig,
# not Config).  Plain classes that don't extend Config fall through to the
# fallback overload and get type[Config].
ConfigT = TypeVar("ConfigT", bound=Config)


@overload
def config(cls: type[ConfigT], /) -> type[ConfigT]: ...
@overload
def config(cls: type, /) -> type[Config]: ...
@overload
def config(
    *,
    key: str = ...,
    name: str = ...,
    icon: str = ...,
    tags: list[str] = ...,
) -> Callable[[type[ConfigT]], type[ConfigT]]: ...
def config(
    cls: type | None = None,
    /,
    *,
    key: str | None = None,
    name: str | None = None,
    icon: str | None = None,
    tags: list[str] | None = None,
) -> type[Config] | Callable[[type], type[Config]]:
    """Create a Config subclass from a decorated class.

    Can be used bare or with arguments::

        @config
        class MyConfig:
            api_key: str
            base_url: str = "https://api.example.com"

        @config(key="custom", name="Custom Config")
        class OtherConfig(Config):
            timeout: int = 30

    The decorated class's annotations and attributes become the Config
    subclass body.  Since Config extends ``BaseSettings``, fields can
    still be loaded from environment variables.

    Returns:
        A Config subclass.
    """
    classvars: dict[str, Any] = {}
    if key is not None:
        classvars["key"] = key
    if name is not None:
        classvars["name"] = name
    if icon is not None:
        classvars["icon"] = icon
    if tags is not None:
        classvars["tags"] = tags

    if cls is not None:
        return build_component_class(cls, base=Config, classvars=classvars)

    def wrapper(cls: type) -> type[Config]:
        return build_component_class(cls, base=Config, classvars=classvars)

    return wrapper
