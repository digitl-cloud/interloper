"""Component: the fundamental building block of the framework, and its relations."""

from interloper.component.base import (
    KINDS,
    Component,
    ComponentDefinition,
)
from interloper.component.relation import ComponentIdentity, Relation, unwrap_optional

__all__ = [
    "KINDS",
    "Component",
    "ComponentDefinition",
    "ComponentIdentity",
    "Relation",
    "unwrap_optional",
]
