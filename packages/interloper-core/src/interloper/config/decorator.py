"""The config decorator: a Config subclass from a decorated class."""

from __future__ import annotations

from interloper.component.decorator import Decorator
from interloper.config.base import Config

config = Decorator(Config)
"""Create a Config subclass from a decorated class.

Can be used bare or with arguments:

```py
@config
class MyConfig:
    api_key: str
    base_url: str = "https://api.example.com"

@config(key="custom", name="Custom Config")
class OtherConfig(Config):
    timeout: int = 30
```

The decorated class's annotations and attributes become the Config subclass
body. Since Config extends ``BaseSettings``, fields can still be loaded from
environment variables.

``relations`` maps a relation name to a `Relation`, a component class, or a list
of component classes narrowing the relation the decorated class declares under
that name. Every other keyword is one of the public ClassVars and field defaults
the decorated class declares, or `Config` itself for a plain class (``key``,
``name``, ``icon``, ``tags``). An unknown name is a ``TypeError`` at decoration.
"""
