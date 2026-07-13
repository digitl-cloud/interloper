"""Resource: base class for injectable dependencies (configs, connections, services)."""

from __future__ import annotations

from typing import ClassVar

from pydantic_settings import BaseSettings

from interloper.component import Component, ComponentDefinition
from interloper.utils.text import to_label


class ResourceDefinition(ComponentDefinition):
    """Definition of a resource with its config schema inlined.

    The config schema is the JSON Schema of the resource's own fields —
    this is same-entity data so it's always inlined, never a key reference.
    """

    provider: str | None = None
    checkable: bool = False


class Resource(BaseSettings, Component):
    """Base for injectable dependencies resolved and injected into asset functions.

    Extends ``BaseSettings``, so resource values can be loaded from
    environment variables, .env files, or passed directly. Subclass to
    define a custom resource::

        class MyCache(Resource):
            model_config = SettingsConfigDict(env_prefix="my_cache_")

            host: str = "localhost"
            port: int = 6379

            def get(self, key: str) -> Any: ...
            def set(self, key: str, value: Any) -> None: ...

    Resources are declared on assets via decorator kwargs or type annotations
    and resolved through a cascade: asset → source → auto-instantiate.

    Built-in resource types:
    - ``Config`` — env-loaded settings
    - ``Connection`` — credentials and client factories
    """

    kind: ClassVar[str] = "resource"
    sensitive: ClassVar[bool] = True

    @classmethod
    def definition(cls) -> ResourceDefinition:
        """Produce a structured definition of this resource class.

        The config schema is inlined as it belongs to the resource itself.

        Returns:
            A ResourceDefinition with metadata and JSON Schema.
        """
        from interloper.utils.imports import get_object_path

        return ResourceDefinition(
            kind=cls.kind,
            key=cls.key,
            path=get_object_path(cls),
            name=cls.name or to_label(cls.__name__),
            icon=cls.icon,
            description=cls.__doc__ or "",
            tags=list(getattr(cls, "tags", [])),
            config_schema=cls.config_schema(),
            relations=cls.relation_definitions(),
        )
