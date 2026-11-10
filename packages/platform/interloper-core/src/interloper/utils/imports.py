"""Dynamic import helpers."""

import importlib
from typing import Any


def import_from_path(path: str) -> Any:
    """Import an object from a path.

    Two forms are supported:

    - **Dotted path** — ``"module.submodule.ClassName"``: imports the parent
      module, then does a single ``getattr`` for the final name.
    - **Composite path** — ``"module.submodule:ClassName.nested"``: the colon
      marks the module / attribute boundary explicitly.  Everything before
      ``:`` is imported as a module, everything after is a chain of
      ``getattr`` calls on the imported object.  This is the form emitted by
      `Asset.classpath` for source-owned assets so that the asset
      class can be reached via a class-level descriptor without
      instantiating its parent source.

    Args:
        path: Dotted or composite import path.

    Returns:
        The imported object.
    """
    if ":" in path:
        module_path, attribute_chain = path.split(":", 1)
    else:
        module_path, _, attribute_chain = path.rpartition(".")

    target: Any = importlib.import_module(module_path)
    for attribute in attribute_chain.split("."):
        target = getattr(target, attribute)
    return target


def get_object_path(target: Any) -> str:
    """Return the dotted import path for a class or function.

    Args:
        target: A class or function.

    Returns:
        Dotted path string like ``"module.submodule.ClassName"``.
    """
    return f"{target.__module__}.{target.__name__}"
