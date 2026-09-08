"""Decorator for creating Config subclasses from plain classes."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar, overload

from interloper.component.decorator import declare, decorate
from interloper.component.relation import Relation
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
    relations: dict[str, Any] = ...,
    **overrides: Any,
) -> Callable[[type[ConfigT]], type[ConfigT]]: ...
def config(
    cls: type | None = None,
    /,
    *,
    relations: dict[str, Any] | None = None,
    **overrides: Any,
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

    Args:
        cls: The decorated class when used bare; ``None`` when called with
            arguments, which returns the decorator instead.
        relations: Relation name to a
            :class:`~interloper.component.relation.Relation`, a component class
            (the shorthand for a relation on it), or a list of component
            classes narrowing the relation the decorated class declares under
            that name. Explicit declarations win over the class annotations.
        **overrides: Definition metadata and behaviour: the public ClassVars
            and field defaults the decorated class declares, or
            :class:`~interloper.config.base.Config` itself for a plain class
            (``key``, ``name``, ``icon``, ``tags``); see the class. An unknown
            name is a ``TypeError`` at decoration.

    Returns:
        A Config subclass.
    """
    if cls is not None:
        return decorate(Config, cls, build=_build_config, relations=relations, **overrides)

    def wrapper(cls: type) -> type[Config]:
        return decorate(Config, cls, build=_build_config, relations=relations, **overrides)

    return wrapper


# -- Internals -----------------------------------------------------------------


def _build_config(
    cls: type,
    *,
    classvars: dict[str, Any],
    fields: dict[str, Any],
    relations: dict[str, Relation],
) -> type[Config]:
    """Build the Config subclass and declare the decorator's relations on it.

    Args:
        cls: The decorated class.
        classvars: Class-level attributes to stamp on the built class.
        fields: Field default overrides for the built class.
        relations: Relations the decorator declares, name to relation.

    Returns:
        A Config subclass.
    """
    return declare(Config.build_class(cls, classvars=classvars, fields=fields), relations)
