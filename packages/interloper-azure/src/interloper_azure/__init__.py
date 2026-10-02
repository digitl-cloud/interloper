"""Interloper Microsoft Azure integration: Entra connection and Microsoft Fabric Warehouse destination."""

from interloper_azure.connection import AzureConnection
from interloper_azure.fabric import FabricWarehouseDestination

__all__ = [
    "AzureConnection",
    "FabricWarehouseDestination",
]
