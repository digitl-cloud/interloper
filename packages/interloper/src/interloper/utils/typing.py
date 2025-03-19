from itertools import islice
from typing import Any, get_args, get_origin


def safe_isinstance(obj: Any, expected_type: Any, max_depth: int = 5, sample_size: int = 100) -> bool:
    """
    Recursively checks if obj matches expected_type, with optimizations for large datasets.

    Optimizations:
    - `max_depth`: Prevents deep recursion.
    - `sample_size`: Limits the number of elements checked in lists, sets, dicts, tuples.
        - If `sample_size=None`, checks **all** elements.
    """

    if max_depth == 0:  # Stop recursion to prevent excessive depth
        return True

    # Special case: Any always matches
    if expected_type is Any:
        return True

    origin = get_origin(expected_type)

    # Base case: If not a generic type, use normal isinstance
    if origin is None:
        return isinstance(obj, expected_type)

    args = get_args(expected_type)

    def sample(iterable: Any) -> Any:
        """Helper function to apply sampling if sample_size is set."""
        return iterable if sample_size is None else islice(iterable, sample_size)

    if origin is list:
        return isinstance(obj, list) and all(
            safe_isinstance(item, args[0], max_depth - 1, sample_size) for item in sample(obj)
        )

    if origin is tuple:
        if len(args) == 2 and args[1] is ...:  # Variadic tuple: tuple[T, ...]
            return isinstance(obj, tuple) and all(
                safe_isinstance(item, args[0], max_depth - 1, sample_size) for item in sample(obj)
            )
        return (
            isinstance(obj, tuple)
            and len(obj) == len(args)
            and all(safe_isinstance(obj[i], args[i], max_depth - 1, sample_size) for i in range(len(args)))
        )

    if origin is set:
        return isinstance(obj, set) and all(
            safe_isinstance(item, args[0], max_depth - 1, sample_size) for item in sample(obj)
        )

    if origin is dict:
        return isinstance(obj, dict) and all(
            safe_isinstance(k, args[0], max_depth - 1, sample_size)
            and safe_isinstance(v, args[1], max_depth - 1, sample_size)
            for k, v in sample(obj.items())
        )

    return False  # Unsupported type
