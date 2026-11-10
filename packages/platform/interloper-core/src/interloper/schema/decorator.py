"""The schema decorator: a Schema subclass from a decorated class."""

from __future__ import annotations

from interloper.component.decorator import Decorator
from interloper.schema.base import Schema


class SchemaDecorator(Decorator[Schema]):
    """Create a Schema subclass from a decorated class.

    Can be used bare or with arguments:

    ```py
    @schema
    class UserSchema:
        id: int
        name: str
        email: str

    @schema(key="custom", name="Custom Schema")
    class OtherSchema:
        value: float
    ```

    The decorated class's annotations and attributes become the Schema subclass
    body. ``key`` overrides the auto-derived snake_cased key and ``name`` is the
    human-readable display name.
    """


schema = SchemaDecorator(Schema)
