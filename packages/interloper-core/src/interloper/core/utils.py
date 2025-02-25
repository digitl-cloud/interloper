import datetime as dt
import logging
import re
from collections.abc import Generator
from itertools import islice
from typing import Any, get_args, get_origin


def basic_logging(level: int = logging.DEBUG) -> None:
    logger = logging.getLogger("interloper")
    logger.setLevel(level)
    # formatter = logging.Formatter(logging.BASIC_FORMAT)
    formatter = logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)-16s | %(message)s")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def date_range(start_date: dt.date, end_date: dt.date, reversed: bool = False) -> Generator[dt.date]:
    if reversed:
        while end_date >= start_date:
            yield end_date
            end_date -= dt.timedelta(days=1)
    else:
        while start_date <= end_date:
            yield start_date
            start_date += dt.timedelta(days=1)


def to_snake_case(value: str) -> str:
    value = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", value)
    value = re.sub("__([A-Z])", r"_\1", value)
    value = re.sub("([a-z0-9])([A-Z])", r"\1_\2", value)
    value = re.sub("([a-zA-Z])([0-9])", r"\1_\2", value)
    value = re.sub("([0-9]+)([a-zA-Z])", r"\1\2", value)
    value = re.sub("_([a-zA-Z])_([0-9]+)$", r"_\1\2", value)
    return value.lower()


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
