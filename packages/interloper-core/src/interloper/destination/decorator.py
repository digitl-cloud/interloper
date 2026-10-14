"""The destination decorator: a Destination subclass from a decorated class."""

from __future__ import annotations

from interloper.component.decorator import Decorator
from interloper.destination.base import Destination

destination = Decorator(Destination)
"""Create a Destination subclass from a decorated class.

Relations can be declared via annotations:

```py
@destination
class MyDest:
    connection: PostgresConnection

    def read(self, context): ...
    def write(self, context, data): ...
```

Or through the ``relations`` channel:

```py
@destination(name="My destination", relations={"connection": PostgresConnection})
class MyDest:
    def read(self, context): ...
    def write(self, context, data): ...
```

An annotation naming a component class declares a relation rather than a
Pydantic model field. ``relations`` maps a relation name to a `Relation`, a
component class, or a list of component classes narrowing the relation the
decorated class declares under that name; explicit declarations win over the
class annotations. Every other keyword is one of the public ClassVars and field
defaults the decorated class declares, or `Destination` itself for a plain class
(``key``, ``name``, ``icon``, ``tags``). An unknown name is a ``TypeError`` at
decoration.
"""
