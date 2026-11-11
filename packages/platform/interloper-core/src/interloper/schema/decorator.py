"""The schema decorator: a Schema subclass from a decorated class."""

from __future__ import annotations

from interloper.component.decorator import Decorator
from interloper.schema.base import Schema


class SchemaDecorator(Decorator[Schema]):
    """Create a Schema subclass from a decorated class.

    ```py
    @schema
    class UserSchema:
        id: int
        name: str
        email: str
    ```

    The decorated class's annotations and attributes become the Schema subclass
    body. A schema declares no settings, so every annotation is a column.
    """


schema = SchemaDecorator(Schema)
