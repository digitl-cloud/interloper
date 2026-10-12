"""Assets: the core data-producing component, its execution context, and its decorator."""

from interloper.asset.base import Asset, AssetDefinition
from interloper.asset.context import ExecutionContext
from interloper.asset.decorator import asset, reference
from interloper.asset.reference import Reference
from interloper.asset.upstream import Upstream

__all__ = ["Asset", "AssetDefinition", "ExecutionContext", "Reference", "Upstream", "asset", "reference"]
