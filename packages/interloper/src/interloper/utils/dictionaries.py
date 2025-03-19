import re
from typing import Any


def replace_in_keys(d: dict, old: str, new: str) -> dict:
    new_dict = {}

    for old_key, value in d.items():
        new_key = str(old_key).replace(old, new)
        if isinstance(value, dict):
            new_dict[new_key] = replace_in_keys(value, old, new)
        else:
            new_dict[new_key] = value

    return new_dict


def regex_replace_in_keys(d: dict, pattern: str, repl: str) -> dict:
    new_dict = {}

    for old_key, value in d.items():
        new_key = re.sub(pattern, repl, old_key)

        if isinstance(value, dict):
            new_dict[new_key] = regex_replace_in_keys(value, pattern, repl)
        else:
            new_dict[new_key] = value

    return new_dict


def replace_in_values(d: dict, old: Any, new: Any) -> dict:
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
    new_dict = {}

    for key, value in d.items():
        if isinstance(value, dict):
            new_dict[key] = remove_empty_values(value, recursive) if recursive else value
        elif value:
            new_dict[key] = value

    return new_dict
