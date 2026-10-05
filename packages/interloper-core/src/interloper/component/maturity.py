"""Maturity: how far a component is from being relied on in production."""

from __future__ import annotations

from enum import Enum


class Maturity(str, Enum):
    """How far a component is from being relied on in production.

    Informational: it is shown beside the component wherever it is offered
    and never changes what the component does. ``ALPHA`` is early work whose
    behaviour, configuration and output may still change and that has not
    been proven against a live service; ``BETA`` works and is being proven,
    with changes still possible; ``STABLE``, the default, is relied on;
    ``DEPRECATED`` still works but is being phased out, so new setups should
    avoid it.

    A composite, such as an asset inside its source, shows the
    `least` mature of its parts, ``DEPRECATED`` counting as the least:
    a deprecated source deprecates its assets, and a deprecated asset stays
    deprecated whatever its source.
    """

    DEPRECATED = "deprecated"
    ALPHA = "alpha"
    BETA = "beta"
    STABLE = "stable"

    @classmethod
    def least(cls, *values: Maturity) -> Maturity:
        """Return the least mature of the given values.

        Args:
            *values: The maturities to compare; at least one.

        Returns:
            The least mature one.
        """
        return next(member for member in (cls.DEPRECATED, cls.ALPHA, cls.BETA, cls.STABLE) if member in values)
