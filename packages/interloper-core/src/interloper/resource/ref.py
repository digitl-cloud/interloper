"""ResourceRef: descriptor for declarative, typed resource access on components."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, TypeVar, overload

from interloper.serializable import IgnoredDescriptor

if TYPE_CHECKING:
    from interloper.component.base import Component
    from interloper.resource.base import Resource

T = TypeVar("T", bound="Resource")


class ResourceRef(IgnoredDescriptor, Generic[T]):
    """Descriptor that declares a resource dependency and provides typed access.

    When placed on a Component subclass (Source, Asset, Destination), it
    automatically registers the resource in ``resource_types`` via
    ``__set_name__`` and provides a typed property accessor via ``__get__``.

    Usage::

        class BigQueryDestination(DatabaseDestination):
            connection = ResourceRef(GoogleCloudConnection)
            config = ResourceRef(BigQueryConfig, required=True)

    This is equivalent to manually writing::

        class BigQueryDestination(DatabaseDestination):
            resource_types: ClassVar = {
                "connection": GoogleCloudConnection,
                "config": BigQueryConfig,
            }

            @property
            def connection(self) -> GoogleCloudConnection | None:
                return self.resources.get("connection")

            @property
            def config(self) -> BigQueryConfig:
                config = self.resources.get("config")
                if config is None:
                    raise ...
                return config

    Args:
        resource_type: The Resource subclass this slot accepts.
        required: If ``True``, accessing the resource when not set raises
            an error instead of returning ``None``.
    """

    def __init__(
        self,
        resource_type: type[T],
        *,
        required: bool = False,
    ) -> None:
        """Initialise a ResourceRef descriptor.

        Args:
            resource_type: The Resource subclass this slot accepts.
            required: Whether the resource must be present at access time.
        """
        self.resource_type = resource_type
        self.required = required
        self.attr_name = ""

    def __set_name__(self, owner: type[Component], name: str) -> None:
        """Register this resource slot on the owning class."""
        self.attr_name = name

        # Auto-populate resource_types on the owning class.
        # Use the class's own dict to avoid mutating a parent's ClassVar.
        if "resource_types" not in owner.__dict__:
            parent_rt = getattr(owner, "resource_types", {})
            owner.resource_types = dict(parent_rt)
        owner.resource_types[name] = self.resource_type

    @overload
    def __get__(self, obj: None, objtype: type) -> ResourceRef[T]: ...
    @overload
    def __get__(self, obj: Any, objtype: type) -> T: ...
    def __get__(self, obj: Any, objtype: type | None = None) -> T | ResourceRef[T]:
        """Resolve the resource from the component's resources dict.

        Returns:
            The resource instance, ``None`` if not set and not required,
            or the descriptor itself when accessed on the class.

        Raises:
            ValueError: If the resource is required but not provided.
        """
        if obj is None:
            return self
        value = obj.resources.get(self.attr_name)
        if value is None and self.required:
            raise ValueError(
                f"{type(obj).__name__} requires a '{self.attr_name}' resource "
                f"({self.resource_type.__name__}) but none was provided."
            )
        return value

    def __repr__(self) -> str:
        """Return a readable representation of this ResourceRef."""
        req = ", required=True" if self.required else ""
        return f"ResourceRef({self.resource_type.__name__}{req})"
