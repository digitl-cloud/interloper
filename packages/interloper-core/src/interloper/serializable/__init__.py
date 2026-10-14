"""Serializable: class-plus-configuration objects and their wire format."""

from interloper.serializable.base import IgnoredDescriptor, Serializable, SerializationContext, Spec
from interloper.serializable.body import ClassBody

__all__ = [
    "ClassBody",
    "IgnoredDescriptor",
    "Serializable",
    "SerializationContext",
    "Spec",
]
