import re


def to_snake_case(value: str) -> str:
    value = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", value)
    value = re.sub("__([A-Z])", r"_\1", value)
    value = re.sub("([a-z0-9])([A-Z])", r"\1_\2", value)
    value = re.sub("([a-zA-Z])([0-9])", r"\1_\2", value)
    value = re.sub("([0-9]+)([a-zA-Z])", r"\1\2", value)
    value = re.sub("_([a-zA-Z])_([0-9]+)$", r"_\1\2", value)
    return value.lower()
