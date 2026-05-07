"""Text processing utilities for naming, casing, and labeling."""

import re

_KEY_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]*$")

_CAMEL_BOUNDARY = re.compile(r"([a-z0-9])([A-Z])")
_ACRONYM_BOUNDARY = re.compile(r"([A-Z]+)([A-Z][a-z])")

_SLUG_SEPARATORS = re.compile(r"[_\s:.]+")
_SLUG_COLLAPSE = re.compile(r"-+")

_LABEL_SEPARATORS = re.compile(r"[_\-:]+")
_LABEL_BOUNDARY = re.compile(r"([a-z])([A-Z])")

_SNAKE_NON_ALNUM = re.compile(r"[^a-zA-Z0-9]+")
_SNAKE_COLLAPSE = re.compile(r"_+")


def validate_key(key: str) -> None:
    """Validate that *key* starts with a letter and contains only letters, numbers, and underscores.

    Raises:
        ValueError: If the key is invalid.
    """
    if not _KEY_RE.match(key):
        msg = f"Key '{key}' is invalid. Must start with a letter and contain only letters, numbers, and underscores."
        raise ValueError(msg)


def to_slug_case(text: str) -> str:
    """Convert text to a URL/key-safe slug (lowercase, hyphen-separated).

    Args:
        text: The text to convert.

    Returns:
        The converted text.
    """
    if not text:
        return ""
    text = _CAMEL_BOUNDARY.sub(r"\1-\2", text).lower()
    text = _SLUG_SEPARATORS.sub("-", text)
    return _SLUG_COLLAPSE.sub("-", text).strip("-")


def to_label(text: str) -> str:
    """Convert text to a human-readable title-cased label.

    Args:
        text: The text to convert.

    Returns:
        The converted text.
    """
    if not text:
        return ""
    text = _LABEL_SEPARATORS.sub(" ", text)
    text = _LABEL_BOUNDARY.sub(r"\1 \2", text)
    return " ".join(text.split()).title()


def to_snake_case(text: str) -> str:
    """Convert text to snake_case.

    Args:
        text: The text to convert.

    Returns:
        The converted text.
    """
    if not text:
        return ""
    text = _ACRONYM_BOUNDARY.sub(r"\1_\2", text)
    text = _CAMEL_BOUNDARY.sub(r"\1_\2", text)
    text = _SNAKE_NON_ALNUM.sub("_", text)
    return _SNAKE_COLLAPSE.sub("_", text).strip("_").lower()
