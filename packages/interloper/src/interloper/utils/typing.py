"""This module contains typing utility functions."""
from itertools import islice
from typing import Any, get_args, get_origin


def safe_isinstance(obj: Any, expected_type: Any, max_depth: int = 5, sample_size: int = 100) -> bool:
    """Recursively check if obj matches expected_type, with optimizations for large datasets.

    Args:
        obj: The object to check.
        expected_type: The expected type.
        max_depth: The maximum recursion depth.
        sample_size: The number of elements to check in lists, sets, dicts, tuples.

    Returns:
        True if the object matches the expected type, False otherwise.
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
        """Apply sampling if sample_size is set.

        Args:
            iterable: The iterable to sample.

        Returns:
            The sampled iterable.
        """
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


def match_type(t1: type, t2: type) -> bool:
    """Recursively check if two types match.

    Args:
        t1: The first type.
        t2: The second type.

    Returns:
        True if the types match, False otherwise.
    """
    if t1 == t2:
        return True

    origin1 = get_origin(t1) or t1
    origin2 = get_origin(t2) or t2

    if origin1 != origin2:
        return False

    args1 = get_args(t1)
    args2 = get_args(t2)

    if not args1 or not args2:
        return True  # accept if generic params are not specified

    if len(args1) != len(args2):
        return False

    return all(match_type(a1, a2) for a1, a2 in zip(args1, args2))
