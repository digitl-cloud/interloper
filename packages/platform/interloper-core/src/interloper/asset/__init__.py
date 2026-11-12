"""Assets: the core data-producing component, its execution context, and its decorator."""

from interloper.asset.base import Asset, AssetDefinition
from interloper.asset.context import ExecutionContext
from interloper.asset.output import Output
from interloper.asset.reference import Reference
from interloper.asset.upstream import Upstream
from interloper.asset.view import View

# isort: split
# Bound after the submodules they share a name with: importing a submodule rebinds its name on the package.
from interloper.asset.decorator import asset, reference, view

__all__ = [
    "Asset",
    "AssetDefinition",
    "ExecutionContext",
    "Output",
    "Reference",
    "Upstream",
    "View",
    "asset",
    "reference",
    "view",
]
