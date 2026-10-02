"""Maturity: how far a component is from being relied on in production."""

from __future__ import annotations

from enum import Enum
from typing import Any


class Maturity(str, Enum):
    """How far a component is from being relied on in production.

    Informational: it is shown beside the component wherever it is offered
    and never changes what the component does. ``ALPHA`` is early work whose
    behaviour, configuration and output may still change and that has not
    been proven against a live service; ``BETA`` works and is being proven,
    with changes still possible; ``STABLE``, the default, is relied on.

    A composite, such as an asset inside its source, shows the
    :meth:`least` mature of its parts.
    """

    ALPHA = "alpha"
    BETA = "beta"
    STABLE = "stable"

    @classmethod
    def of(cls, value: Any) -> Maturity:
        """Coerce a declared value to a member.

        Args:
            value: A member, or its string value (``"alpha"``, ``"beta"``, ``"stable"``).

        Returns:
            The matching member.

        Raises:
            TypeError: If the value names no member.
        """
        try:
            return cls(value)
        except ValueError:
            allowed = ", ".join(repr(member.value) for member in cls)
            raise TypeError(f"Unknown maturity {value!r}; expected one of {allowed}.") from None

    @classmethod
    def least(cls, *values: Maturity) -> Maturity:
        """Return the least mature of the given values.

        Args:
            *values: The maturities to compare; at least one.

        Returns:
            The least mature one.
        """
        return next(member for member in (cls.ALPHA, cls.BETA, cls.STABLE) if member in values)
