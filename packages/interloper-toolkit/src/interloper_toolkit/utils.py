"""Small helpers shared by the toolkit tools."""

from __future__ import annotations


def clip(text: str | None, limit: int, *, tail: bool = False) -> str | None:
    """Cut a string to *limit* characters, marking how much was cut.

    Args:
        text: The string to clip, or ``None``.
        limit: Characters to keep.
        tail: Keep the last *limit* characters instead of the first, for text
            whose useful part is at the end (a traceback's raising frame).

    Returns:
        The string unchanged when it fits, else the kept part with an
        ``…[+N chars]`` marker on the cut side; ``None`` stays ``None``.
    """
    if text is None or len(text) <= limit:
        return text
    marker = f"…[+{len(text) - limit} chars]"
    return marker + text[-limit:] if tail else text[:limit] + marker

