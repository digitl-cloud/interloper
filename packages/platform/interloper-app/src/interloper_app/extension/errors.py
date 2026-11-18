"""What the ``interloper extension`` commands refuse."""

from __future__ import annotations


class ExtensionError(Exception):
    """A refusal, printed as one line before the command exits with status 1.

    Raised before anything is written, or, when files were written first,
    naming them.
    """
