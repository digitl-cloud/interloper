"""This module contains dictionary utility functions."""
import re
from typing import Any


def replace_in_keys(d: dict, old: str, new: str) -> dict:
    """Recursively replace a string in the keys of a dictionary.

    Args:
        d: The dictionary to replace the keys in.
        old: The string to replace.
        new: The string to replace with.

    Returns:
        A new dictionary with the keys replaced.
    """
    new_dict = {}

    for old_key, value in d.items():
        new_key = str(old_key).replace(old, new)
        if isinstance(value, dict):
            new_dict[new_key] = replace_in_keys(value, old, new)
        else:
            new_dict[new_key] = value

    return new_dict


def regex_replace_in_keys(d: dict, pattern: str, repl: str) -> dict:
    """Recursively replace a pattern in the keys of a dictionary.

    Args:
        d: The dictionary to replace the keys in.
        pattern: The pattern to replace.
        repl: The string to replace with.

    Returns:
        A new dictionary with the keys replaced.
    """
    new_dict = {}

    for old_key, value in d.items():
        new_key = re.sub(pattern, repl, old_key)

        if isinstance(value, dict):
            new_dict[new_key] = regex_replace_in_keys(value, pattern, repl)
        else:
            new_dict[new_key] = value

    return new_dict


def replace_in_values(d: dict, old: Any, new: Any) -> dict:
    """Recursively replace a value in the values of a dictionary.

    Args:
        d: The dictionary to replace the values in.
        old: The value to replace.
        new: The value to replace with.

    Returns:
        A new dictionary with the values replaced.
    """
    new_dict = {}

    for key, value in d.items():
        if isinstance(value, dict) and bool(value):
            new_dict[key] = replace_in_values(value, old, new)
        elif isinstance(value, list):
            new_dict[key] = list(map(lambda x: replace_in_values(x, old, new) if isinstance(x, dict) else x, value))
        elif value == old:
            new_dict[key] = new
        else:
            new_dict[key] = value

    return new_dict


def remove_empty_values(d: dict, recursive: bool = False) -> dict:
    """Recursively remove empty values from a dictionary.

    Args:
        d: The dictionary to remove empty values from.
        recursive: Whether to remove empty values recursively.

    Returns:
        A new dictionary with empty values removed.
    """
    new_dict = {}

    for key, value in d.items():
        if isinstance(value, dict):
            new_dict[key] = remove_empty_values(value, recursive) if recursive else value
        elif value:
            new_dict[key] = value

    return new_dict
