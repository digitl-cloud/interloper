"""The source decorator: a Source subclass from a decorated class or function."""

from __future__ import annotations

from interloper.component.decorator import Decorator
from interloper.source.base import Source


class SourceDecorator(Decorator[Source]):
    """Create a Source subclass from a decorated class or function.

    A decorated class declares its configuration fields, its assets (methods
    carrying ``@asset``) and its helpers; a decorated function returns the asset
    classes and turns its own annotated parameters into configuration fields:

    ```py
    @source(tags=["Advertising"], dataset="raw_shop", relations={"connection": ShopConnection})
    class Shop(Source): ...
    ```

    ``relations`` maps a relation name to a `Relation`, a component class (the
    shorthand for a relation on it), or a list of component classes narrowing the
    relation `Source` declares under that name; explicit declarations win over the
    ones read from the class annotations.

    Every other keyword is definition metadata or behaviour: the public ClassVars and
    field defaults `Source` declares (``key``, ``name``, ``icon``, ``tags``,
    ``dataset``, ``default_destination_key``, ``normalizer``,
    ``materialization_strategy``, ...). An unknown name is a ``TypeError`` at
    decoration. The result is a Source subclass with its assets discovered.
    """


source = SourceDecorator(Source)
