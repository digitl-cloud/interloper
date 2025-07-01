"""This module contains the loader utility functions."""
import importlib
from typing import Any


def import_from_path(path: str) -> Any:
    """Import an object from a path.

    The path can be in the format 'package.module:name' or 'package.module.name'.

    Args:
        path: The path to the object.

    Returns:
        The imported object.

    Raises:
        ValueError: If the path is invalid or the object cannot be imported.
    """
    if not path or not isinstance(path, str):
        raise ValueError(f"Path must be a non-empty string, got {type(path)}")

    if not any(c in path for c in (".", ":")):
        raise ValueError(f"Invalid path format: {path}. Must be either 'package.module:name' or 'package.module.name'")

    if ":" in path:
        module_path, attr_name = path.split(":", maxsplit=1)
        if not module_path or not attr_name:
            raise ValueError(f"Invalid path format: {path}. Module path and attribute name cannot be empty")
    else:
        *module_parts, attr_name = path.split(".")
        if not module_parts or not attr_name:
            raise ValueError(f"Invalid path format: {path}. Module parts and attribute name cannot be empty")
        module_path = ".".join(module_parts)

    try:
        module = importlib.import_module(module_path)
    except (ImportError, ModuleNotFoundError) as e:
        raise ValueError(f"Failed to import module {module_path}: {str(e)}")

    try:
        attr = getattr(module, attr_name)
    except AttributeError:
        raise ValueError(f"Attribute {attr_name} not found in module {module_path}")

    # if not issubclass(attr.__class__, target):
    #     raise ValueError(f"Imported object {attr} is not a {target.__name__}. Got type {type(attr).__name__}")

    return attr
