"""Component: the fundamental building block of the framework, and its relations."""

from interloper.component.base import (
    KINDS,
    Component,
    ComponentDefinition,
)
from interloper.component.relation import Bound, ComponentIdentity, Relation

__all__ = [
    "KINDS",
    "Bound",
    "Component",
    "ComponentDefinition",
    "ComponentIdentity",
    "Relation",
]
