"""Normalizer for coercing, transforming, and validating asset output data."""

from interloper.normalizer.base import Normalizer
from interloper.normalizer.strategy import MaterializationStrategy

__all__ = ["MaterializationStrategy", "Normalizer"]
