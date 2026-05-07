"""Dynamic import helpers and import-guard decorator."""

import importlib.util
from collections.abc import Callable
from typing import Any, TypeVar, cast, overload

T = TypeVar("T")
F = TypeVar("F", bound=Callable[..., Any])
C = TypeVar("C", bound=type[Any])


@overload
def import_from_path(path: str) -> Any: ...
@overload
def import_from_path(path: str, target_type: type[T]) -> T: ...
def import_from_path(path: str, target_type: type[T] | None = None) -> Any:
    """Import an object from a path.

    Two forms are supported:

    - **Dotted path** — ``"module.submodule.ClassName"``: imports the parent
      module, then does a single ``getattr`` for the final name.
    - **Composite path** — ``"module.submodule:ClassName.nested"``: the colon
      marks the module / attribute boundary explicitly.  Everything before
      ``:`` is imported as a module, everything after is a chain of
      ``getattr`` calls on the imported object.  This is the form emitted by
      :meth:`Asset.classpath` for source-owned assets so that the asset
      class can be reached via a class-level descriptor without
      instantiating its parent source.

    Args:
        path: Dotted or composite import path.
        target_type: If provided, validates the imported object is an
            instance of this type.

    Returns:
        The imported object.

    Raises:
        ValueError: If *target_type* is given and the object does not match.
    """
    if ":" in path:
        module_path, attr_chain = path.split(":", 1)
    else:
        module_path, _, attr_chain = path.rpartition(".")

    obj: Any = importlib.import_module(module_path)
    for attr in attr_chain.split("."):
        obj = getattr(obj, attr)

    if target_type is not None and not isinstance(obj, target_type):
        msg = f"Object at '{path}' is not a {target_type.__name__}"
        raise ValueError(msg)
    return obj


def get_object_path(obj: Any) -> str:
    """Return the dotted import path for a class or function.

    Args:
        obj: A class or function.

    Returns:
        Dotted path string like ``"module.submodule.ClassName"``.
    """
    return f"{obj.__module__}.{obj.__name__}"


def require_import(import_name: str, error_message: str) -> Callable[[F | C], F | C]:
    """Decorator that defers an ``ImportError`` until the decorated object is used.

    Works with both classes and functions. For classes the check runs at
    instantiation; for functions it runs at call time.

    Args:
        import_name: Top-level package name to look for (e.g. ``"pandas"``).
        error_message: Message for the ``ImportError`` raised when the package
            is missing.

    Returns:
        A decorator that wraps the target class or function.
    """

    def decorator(obj: F | C) -> F | C:
        if isinstance(obj, type):
            original_new = obj.__new__

            def checked_new(cls: type[Any], *args: Any, **kwargs: Any) -> Any:
                if importlib.util.find_spec(import_name) is None:
                    raise ImportError(error_message)
                if original_new is object.__new__:
                    return object.__new__(cls)
                return original_new(cls, *args, **kwargs)

            obj.__new__ = staticmethod(checked_new)
            return cast(C, obj)
        else:

            def wrapper(*args: Any, **kwargs: Any) -> Any:
                if importlib.util.find_spec(import_name) is None:
                    raise ImportError(error_message)
                return obj(*args, **kwargs)

            return cast(F, wrapper)

    return decorator
