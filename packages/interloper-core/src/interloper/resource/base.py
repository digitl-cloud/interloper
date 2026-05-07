"""Resource: base class for injectable dependencies (configs, connections, services)."""

from __future__ import annotations

from typing import Any, ClassVar

from pydantic import Field

from interloper.component import Component, ComponentDefinition
from interloper.utils.text import to_label


class ResourceDefinition(ComponentDefinition):
    """Definition of a resource with its config schema inlined.

    The config schema is the JSON Schema of the resource's own fields —
    this is same-entity data so it's always inlined, never a key reference.
    """

    tags: list[str] = Field(default_factory=list)
    config_schema: dict[str, Any] = Field(default_factory=dict)
    provider: str | None = None


class Resource(Component):
    """Base for injectable dependencies resolved and injected into asset functions.

    Subclass to define a custom resource::

        class MyCache(Resource):
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

    @classmethod
    def definition(cls) -> ResourceDefinition:
        """Produce a structured definition of this resource class.

        The config schema is inlined as it belongs to the resource itself.
        If the class declares an ``oauth`` ClassVar, its metadata is
        injected as an ``x-oauth`` extension at the schema root.

        Returns:
            A ResourceDefinition with metadata and JSON Schema.
        """
        from interloper.resource.fields import OAuthConfig, strip_internal_fields
        from interloper.utils.imports import get_object_path

        raw = cls.model_json_schema() if hasattr(cls, "model_json_schema") else {}
        schema = strip_internal_fields(raw)

        # Inject x-oauth from ClassVar if present.
        oauth_cfg: OAuthConfig | None = getattr(cls, "oauth", None)
        provider: str | None = None
        if isinstance(oauth_cfg, OAuthConfig):
            schema["x-oauth"] = oauth_cfg.to_schema_ext()
            provider = oauth_cfg.provider

        return ResourceDefinition(
            kind=cls.kind,
            key=cls.key,
            path=get_object_path(cls),
            name=cls.name or to_label(cls.__name__),
            icon=cls.icon,
            description=cls.__doc__ or "",
            tags=list(getattr(cls, 'tags', [])),
            config_schema=schema,
            provider=provider,
        )
